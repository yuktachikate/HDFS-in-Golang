package main

import (
	"bufio"
	"fmt"
	"hdfs/messages"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

var receivedChunks uint64 = 0
var receivedBytes uint64 = 0

var HostName, _ = os.Hostname()
var wg sync.WaitGroup

var controllerHostname = os.Args[1]
var controllerPort = os.Args[2]
var Port = os.Args[3]
var storageDirectory = os.Args[4]
var receivedChunksMutex sync.Mutex

func incrementReceivedChunks() {
	receivedChunksMutex.Lock()
	receivedChunks++
	receivedChunksMutex.Unlock()
}

func decrementReceivedChunks() {
	receivedChunksMutex.Lock()
	receivedChunks--
	receivedChunksMutex.Unlock()
}

/* save chunk data */
func saveChunkData(chunkMetadata *messages.ChunkMetaData, conn net.Conn) {
	chunkFilename := chunkMetadata.GetLocations().GetChunkFilename()
	filePath := filepath.Join(storageDirectory, "ChunkData")
	chunkSize := chunkMetadata.GetChunkSize()

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		os.MkdirAll(filePath, os.ModePerm)
	}

	file, err := os.Create(filepath.Join(filePath, chunkFilename))
	messages.HandleError(err)

	defer file.Close()

	// write/save buffer to disk
	n, _ := io.CopyN(file, conn, int64(chunkSize))
	// count the received bytes
	receivedBytes += uint64(n)
}

/* save chunk metadata */
func saveChunkMetadata(chunkMetadata *messages.ChunkMetaData) {
	filename := chunkMetadata.GetFileName()
	chunkFilename := chunkMetadata.GetLocations().GetChunkFilename()
	chunkSize := chunkMetadata.GetChunkSize()
	fileChecksum := chunkMetadata.GetChecksum()
	chunkFilePath := filepath.Join(storageDirectory, "ChunkData", chunkFilename)

	filePath := filepath.Join(storageDirectory, "ChunkMetaData")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		os.MkdirAll(filePath, os.ModePerm)
	}

	file, err := os.Create(filepath.Join(filePath, chunkFilename+"_metadata"))
	messages.HandleError(err)

	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%s, %s, %s, %d, %s\n", filename, chunkFilename, chunkFilePath, chunkSize, fileChecksum))
	messages.HandleError(err)
}

/* get chunk info from disk */
func getChunkInfoFromDisk(chunkMetadata *messages.ChunkMetaData) []byte {
	chunkFilename := chunkMetadata.GetLocations().GetChunkFilename()

	filePath := filepath.Join(storageDirectory, "ChunkData", chunkFilename)

	data, _ := os.ReadFile(filePath)

	chunkFilePath := filepath.Join(storageDirectory, "ChunkMetaData", chunkFilename+"_metadata")

	metadata, _ := os.ReadFile(chunkFilePath)

	content := strings.Split(string(metadata), ",")

	chunkMetadata.ChunkSize = uint64(len(data))
	chunkMetadata.Checksum = strings.TrimSpace(content[len(content)-1])

	return data
}

/* send chunk metadata to replicas */
func sendChunkMetadataToReplicas(chunkMetadata *messages.ChunkMetaData) {
	replicas := chunkMetadata.GetLocations().GetReplicas()
	chunkMetadata.Locations.Replicas = nil
	chunkMetadata.SenderAddr = fmt.Sprintf("%s:%s", HostName, Port)

	for _, replica := range replicas {
		addr := strings.Split(replica, ":")
		//establish connection
		handler, err := messages.GetConnection(addr[0], addr[1])
		if err != nil {
			return
		}

		//update sender address and receiver address
		chunkMetadata.ReceiverAddr = replica

		//extract chunk info from disk
		data := getChunkInfoFromDisk(chunkMetadata)

		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_ChunkMetadata{ChunkMetadata: chunkMetadata},
		}

		//send data
		handler.Send(wrapper)
		handler.GetConnection().Write(data)

		//close connection
		handler.Close()
	}
}

func sendDeleteRequestToReplicas(chunkMetadata *messages.ChunkMetaData) {
	replicas := chunkMetadata.GetLocations().GetReplicas()
	chunkMetadata.Locations.Replicas = nil
	chunkMetadata.SenderAddr = fmt.Sprintf("%s:%s", HostName, Port)

	for _, replica := range replicas {
		addr := strings.Split(replica, ":")
		//establish connection
		handler, err := messages.GetConnection(addr[0], addr[1])
		if err != nil {
			return
		}

		//update sender address and receiver address
		chunkMetadata.ReceiverAddr = replica

		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_ChunkMetadata{ChunkMetadata: chunkMetadata},
		}

		//send data
		handler.Send(wrapper)

		//close connection
		handler.Close()
	}
}

/* save chunk information to disk */
func saveChunkInfoToDisk(chunkMetadata *messages.ChunkMetaData, conn net.Conn) {
	//save metadata
	saveChunkMetadata(chunkMetadata)
	//save data to disk
	saveChunkData(chunkMetadata, conn)
	//send chunk metadata to replicas
	sendChunkMetadataToReplicas(chunkMetadata)
	//increment received chunks count
	incrementReceivedChunks()
}

/* delete chunk data and metadata */
func deleteChunk(chunkMetadata *messages.ChunkMetaData) {
	chunkFilename := chunkMetadata.GetLocations().GetChunkFilename()
	//delete chunk metadata
	filePath := filepath.Join(storageDirectory, "ChunkMetaData")
	err := os.Remove(filepath.Join(filePath, chunkFilename+"_metadata"))
	messages.HandleError(err)
	//delete chunk data
	filePath = filepath.Join(storageDirectory, "ChunkData")
	err = os.Remove(filepath.Join(filePath, chunkFilename))
	messages.HandleError(err)
}

/* delete chunk information from disk */
func deleteChunkInfoFromDisk(chunkMetadata *messages.ChunkMetaData) {
	//log.Println(fmt.Sprintf("deleting chunk %s from %s", chunkMetadata.Locations.ChunkFilename, chunkMetadata.ReceiverAddr))
	//remove chunk from disk
	deleteChunk(chunkMetadata)
	log.Println(fmt.Sprintf("deleted chunk %s from %s", chunkMetadata.Locations.ChunkFilename, chunkMetadata.ReceiverAddr))
	//send chunk metadata to replicas
	sendDeleteRequestToReplicas(chunkMetadata)
	//decrement received chunks count
	decrementReceivedChunks()
}

/* helper function to get available space */
func diskUsage(path string) uint64 {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return 0
	}
	return fs.Bfree * uint64(fs.Bsize)
}

/* read file */
func readFile(filePath string, chunks *[]*messages.ChunkFileInfo) {
	file, err := os.Open(filePath)
	messages.HandleError(err)

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		text := scanner.Text()
		content := strings.Split(text, ",")
		chunkInfo := messages.ChunkFileInfo{Filename: strings.TrimSpace(content[0]), ChunkFilename: strings.TrimSpace(content[1]), ChunkFilepath: strings.TrimSpace(content[2])}
		*chunks = append(*chunks, &chunkInfo)
	}

	err = scanner.Err()
	messages.HandleError(err)
}

/* get stored files info */
func getStoredFilesInfo() []*messages.ChunkFileInfo {
	var chunks = make([]*messages.ChunkFileInfo, 0)

	dir := filepath.Join(storageDirectory, "ChunkMetaData")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}

	lst, err := os.ReadDir(dir)

	messages.HandleError(err)

	for _, entry := range lst {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		filePath := filepath.Join(dir, entry.Name())
		readFile(filePath, &chunks)
	}

	return chunks
}

/* send heart beat to controller */
func sendHeartBeatToController(hostname string, port string) {
	for {
		// establish connection
		msgHandler, err := messages.GetConnection(hostname, port)
		if err != nil {
			break
		}
		// get the available space
		availableSpace := diskUsage(storageDirectory)

		files := getStoredFilesInfo()

		metadata := messages.StorageNodesMetadata{
			AvailableSpace:               availableSpace,
			TotalNumberOfStorageRequests: receivedChunks,
			Addr:                         fmt.Sprintf("%s:%s", HostName, Port),
			Files:                        files,
			RequestType:                  messages.RequestType_Write,
		}

		// send file metadata
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_StorageNodesMetadata{StorageNodesMetadata: &metadata},
		}

		// send data
		msgHandler.Send(wrapper)

		// close connection
		msgHandler.Close()

		// send request in every 5 seconds
		time.Sleep(messages.HEARTBEAT_PER_SEC * time.Second)
	}
}

/* send messages */
func sendMessages(hostname string, port string) {
	defer wg.Done()
	for {
		//send heartbeat to controller
		sendHeartBeatToController(hostname, port)
	}
}

/* request replica info from controller */
func requestReplicaInfoFromController(msg *messages.Wrapper_ChunkMetadata) {
	//establish connection
	handler, err := messages.GetConnection(controllerHostname, controllerPort)
	if err != nil {
		return
	}

	//add address of sender
	msg.ChunkMetadata.SenderAddr = fmt.Sprintf("%s:%s", HostName, Port)
	msg.ChunkMetadata.ReceiverAddr = fmt.Sprintf("%s:%s", controllerHostname, controllerPort)
	//change request type to replica
	msg.ChunkMetadata.RequestType = messages.RequestType_Replica

	wrapper := &messages.Wrapper{
		Msg: msg,
	}

	//send data
	handler.Send(wrapper)

	//close connection
	handler.Close()
}

/* send chunk data to client */
func sendChunkDataToClient(senderAddr string, msg *messages.Wrapper_ChunkMetadata, data []byte) {
	log.Println(fmt.Sprintf("sending chunk data %s to %s", msg.ChunkMetadata.GetLocations().ChunkFilename, senderAddr))

	//extract senders address
	addr := strings.Split(senderAddr, ":")
	//establish connection with sender
	msgHandler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		return
	}
	//change replication type
	msg.ChunkMetadata.RequestType = messages.RequestType_Write

	//update sender address and receiver address
	msg.ChunkMetadata.ReceiverAddr = senderAddr
	msg.ChunkMetadata.SenderAddr = fmt.Sprintf("%s:%s", HostName, Port)

	wrapper := &messages.Wrapper{
		Msg: msg,
	}

	// send data
	msgHandler.Send(wrapper)
	msgHandler.GetConnection().Write(data)

	//close connection
	msgHandler.Close()
}

/* helper function to handler corrupted chunk */
func handleCorruptedChunk(msg *messages.Wrapper_ChunkMetadata) {
	for {
		originalAddr := msg.ChunkMetadata.SenderAddr
		log.Println(fmt.Sprintf("found corrupted chunk %s. Repairing it...", msg.ChunkMetadata.GetLocations().GetChunkFilename()))
		go requestReplicaInfoFromController(msg)

		// sleep for 5 seconds until the node repairs corrupted chunk
		time.Sleep(5 * time.Second)

		// get updated chunk data once again
		data := getChunkInfoFromDisk(msg.ChunkMetadata)

		log.Println(fmt.Sprintf("validating chunk %s once again", msg.ChunkMetadata.GetLocations().GetChunkFilename()))
		isChunkValid, _ := messages.ValidateFile(msg.ChunkMetadata.GetChecksum(), filepath.Join(storageDirectory, "ChunkData", msg.ChunkMetadata.GetLocations().GetChunkFilename()))
		if isChunkValid {
			sendChunkDataToClient(originalAddr, msg, data)
			break
		}
	}
}

/* send chunk info to client */
func getChunkInfoAndSendToClient(msg *messages.Wrapper_ChunkMetadata) {
	//extract chunk info from disk
	data := getChunkInfoFromDisk(msg.ChunkMetadata)

	log.Println(fmt.Sprintf("validating chunk %s", msg.ChunkMetadata.GetLocations().GetChunkFilename()))
	isChunkValid, _ := messages.ValidateFile(msg.ChunkMetadata.GetChecksum(), filepath.Join(storageDirectory, "ChunkData", msg.ChunkMetadata.GetLocations().GetChunkFilename()))

	if !isChunkValid {
		handleCorruptedChunk(msg)
	} else {
		sendChunkDataToClient(msg.ChunkMetadata.SenderAddr, msg, data)
	}

}

/* request data from replication node */
func requestDataFromReplicationNode(msg *messages.Wrapper_ChunkMetadata) {
	//retrieve data from first replica
	replica := msg.ChunkMetadata.Locations.Replicas[0]

	//remove first replica from the list
	msg.ChunkMetadata.Locations.Replicas = msg.ChunkMetadata.Locations.Replicas[1:]

	//extract replica's address
	addr := strings.Split(replica, ":")
	//establish connection
	handler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		return
	}

	// set request type to read
	msg.ChunkMetadata.RequestType = messages.RequestType_Read

	//update sender address and receiver address
	msg.ChunkMetadata.ReceiverAddr = replica
	msg.ChunkMetadata.SenderAddr = fmt.Sprintf("%s:%s", HostName, Port)

	wrapper := &messages.Wrapper{
		Msg: msg,
	}

	//send data
	handler.Send(wrapper)

	//close connection
	handler.Close()
}

/* handle client Request */
func handleClientRequest(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	for {
		wrapper, err := msgHandler.Receive()
		messages.HandleError(err)

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_ChunkMetadata:
			requestType := msg.ChunkMetadata.GetRequestType()
			if requestType == messages.RequestType_Replica {
				log.Println(fmt.Sprintf("get chunk data %s from replica storage node %s for %s", msg.ChunkMetadata.Locations.ChunkFilename, msg.ChunkMetadata.Locations.Replicas[0], msg.ChunkMetadata.ReceiverAddr))
				requestDataFromReplicationNode(msg)
			} else if requestType == messages.RequestType_Write {
				log.Println(fmt.Sprintf("saving chunk data %s to disk received from %s", msg.ChunkMetadata.Locations.ChunkFilename, msg.ChunkMetadata.SenderAddr))
				saveChunkInfoToDisk(msg.ChunkMetadata, msgHandler.GetConnection())
			} else if requestType == messages.RequestType_Delete {
				log.Println(fmt.Sprintf("deleting chunk data %s from disk received from %s", msg.ChunkMetadata.Locations.ChunkFilename, msg.ChunkMetadata.SenderAddr))
				deleteChunkInfoFromDisk(msg.ChunkMetadata)
			} else {
				log.Println(fmt.Sprintf("received chunk data %s retrieval request from %s", msg.ChunkMetadata.Locations.ChunkFilename, msg.ChunkMetadata.SenderAddr))
				getChunkInfoAndSendToClient(msg)
			}
			return
		case nil:
			log.Println("Received an empty message, terminating client")
			return
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

/* receive messages */
func receiveMessages() {
	defer wg.Done()

	listener, err := net.Listen("tcp", ":"+Port)
	messages.HandleError(err)

	log.Println("Storage Node with port " + Port + " started! Waiting for connections...")

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			go handleClientRequest(msgHandler)
		}
	}
}

func main() {
	// send messages
	go sendMessages(controllerHostname, controllerPort)
	//receive messages
	go receiveMessages()

	// wait for go routines to complete
	wg.Add(2)

	wg.Wait()
}
