package main

import (
	"bufio"
	"fmt"
	"hdfs/filters"
	"hdfs/messages"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup
var HostName, _ = os.Hostname()
var Port = os.Args[1]
var directory = os.Args[2]

//active nodes map for example : node addr -> StorageNodeInfo
var activeNodesMap = make(map[string]*StorageNodeInfo)
var activeNodesMapMutex = &sync.RWMutex{}

//filename, chunk_filename, node addr -> chunk_file_location along with replicas
var fileChunkMap = make(map[string]map[string]map[string]string)
var fileChunkMapMutex = &sync.RWMutex{}

//stores file metadata for example: file name -> FileInfo
var fileMap = make(map[string]*FileInfo)
var fileMapMutex = &sync.RWMutex{}
var bf = filters.New(10000, 3)

type StorageNodeInfo struct {
	availableSpace              uint64
	totalNumberOfStorageRequest uint64
	hostname                    string
	port                        string
	lastHeartbeatTime           time.Time
	files                       []*chunksInfo
}

type chunksInfo struct {
	filename      string
	chunkFileName string
	chunkFilePath string
}

type FileInfo struct {
	totalNumberOfChunks uint64
	chunkSize           uint64
	fileSize            uint64
	fileChecksum        string
	folderName          string
}

/* get file metadata */
func getFileMetadata(fileMetadata *messages.Wrapper_FileMetadata) {
	filename := fileMetadata.FileMetadata.GetFileName()

	fileMetadata.FileMetadata.FileSize = fileMap[filename].fileSize
	fileMetadata.FileMetadata.ChunkSize = fileMap[filename].chunkSize
	fileMetadata.FileMetadata.TotalChunks = fileMap[filename].totalNumberOfChunks
	fileMetadata.FileMetadata.Checksum = fileMap[filename].fileChecksum
	fileMetadata.FileMetadata.FolderName = fileMap[filename].folderName
}

/* save file metadata to disk */
func saveFileMetadata(fileMetadata *messages.Wrapper_FileMetadata) {
	filename := fileMetadata.FileMetadata.GetFileName()
	totalNumberOfChunks := fileMetadata.FileMetadata.GetTotalChunks()
	chunkSize := fileMetadata.FileMetadata.GetChunkSize()
	fileSize := fileMetadata.FileMetadata.GetFileSize()
	fileChecksum := fileMetadata.FileMetadata.GetChecksum()
	folderName := fileMetadata.FileMetadata.GetFolderName()

	filePath := filepath.Join(directory, "FileMetaData")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		os.MkdirAll(filePath, os.ModePerm)
	}

	//save file Info to map
	fileInfo := FileInfo{totalNumberOfChunks: totalNumberOfChunks, chunkSize: chunkSize, fileSize: fileSize, fileChecksum: fileChecksum, folderName: folderName}
	fileMapMutex.Lock()
	fileMap[filename] = &fileInfo
	fileMapMutex.Unlock()

	file, err := os.Create(filepath.Join(filePath, filename))
	messages.HandleError(err)

	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("FileName: %s,ChunkSize: %d, FileCheckSum: %s, TotalNumberOfChunks: %d, FileSize: %d, FolderName: %s\n",
		filename, chunkSize, fileChecksum, totalNumberOfChunks, fileSize, folderName))
	messages.HandleError(err)
}

func deleteChunkFileInfoFromDisk(filename string) {
	chunkStorageNodeFilePath := filepath.Join(directory, "ChunkStorageNodeMetadata")
	err := os.Remove(filepath.Join(chunkStorageNodeFilePath, filename))
	fileChunkMapMutex.Lock()
	delete(fileChunkMap, filename)
	fileChunkMapMutex.Unlock()
	messages.HandleError(err)

	chunkFilePath := filepath.Join(directory, "FileMetaData")
	err = os.Remove(filepath.Join(chunkFilePath, filename))
	fileMapMutex.Lock()
	delete(fileMap, filename)
	fileMapMutex.Unlock()
	messages.HandleError(err)
}

func deleteFileMetadata(fileMetadata *messages.Wrapper_FileMetadata) {
	filename := fileMetadata.FileMetadata.GetFileName()
	log.Println("Filename is ", filename)
	deleteChunkFileInfoFromDisk(filename)
}

/* get random storage nodes from map */
func getRandomStorageNodes(mapI interface{}) interface{} {
	keys := reflect.ValueOf(mapI).MapKeys()
	return keys[rand.Intn(len(keys))].Interface()
}

/* helper function to get storage nodes for client */
func getStorageNodesForClient(msg *messages.Wrapper_FileMetadata) {
	totalNumberOfChunks := msg.FileMetadata.GetTotalChunks()
	filename := msg.FileMetadata.GetFileName()

	var locations = make([]*messages.ChunkStorageLocations, totalNumberOfChunks)
	i := 0
	fileChunkMapMutex.RLock()
	for chunkFilename, storageInfo := range fileChunkMap[filename] {
		var nodes = make([]string, 0)
		for addr, _ := range storageInfo {
			nodes = append(nodes, addr)
		}

		locations[i] = &messages.ChunkStorageLocations{ChunkFilename: chunkFilename, Replicas: nodes}
		i++
	}
	fileChunkMapMutex.RUnlock()
	msg.FileMetadata.Locations = locations
}

/* helper function to get storage nodes along with replicas */
func getStorageNodes(totalNumberOfChunks uint64) []*messages.ChunkStorageLocations {
	var locations = make([]*messages.ChunkStorageLocations, totalNumberOfChunks)

	nodes := make(map[string]struct{}, 0)

	rand.Seed(time.Now().UnixNano())
	activeNodesMapMutex.RLock()
	for {
		loc := getRandomStorageNodes(activeNodesMap).(string)
		nodes[loc] = struct{}{}
		if len(nodes) == messages.MAX_REPLICAS || len(activeNodesMap) < messages.MAX_REPLICAS {
			break
		}
	}
	activeNodesMapMutex.RUnlock()

	keys := make([]string, 0)
	for k := range nodes {
		keys = append(keys, k)
	}

	for i := uint64(0); i < totalNumberOfChunks; i++ {
		locations[i] = &messages.ChunkStorageLocations{Replicas: keys}
	}

	return locations
}

/* send storage locations to client */
func sendStorageNodesToClient(msg *messages.Wrapper_FileMetadata) {

	totalNumberOfChunks := msg.FileMetadata.GetTotalChunks()

	locations := getStorageNodes(totalNumberOfChunks)

	//add locations to the client and send it to client
	msg.FileMetadata.Locations = locations

	//extract senders address
	addr := strings.Split(msg.FileMetadata.SenderAddr, ":")
	msgHandler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		return
	}

	msg.FileMetadata.ReceiverAddr = msg.FileMetadata.SenderAddr
	msg.FileMetadata.SenderAddr = fmt.Sprintf("%s:%s", HostName, Port)

	wrapper := &messages.Wrapper{
		Msg: msg,
	}

	//send data
	err = msgHandler.Send(wrapper)
	messages.HandleError(err)

	//close connection
	msgHandler.Close()
}

/* remove failed nodes for each chunk and replace with a replication node */
func replaceFailedNode(filename string, chunkFilename string, failedNode string) {
	//all node locations for the chunk
	fileChunkMapMutex.Lock()
	allChunkNodes := fileChunkMap[filename][chunkFilename]
	fileChunkMapMutex.Unlock()
	//node to store all the working chunk nodes
	workingChunkNodes := make(map[string]bool)
	var newReplicationNodeLoc string

	//remove the failed node from chunk nodes map
	for node, _ := range allChunkNodes {
		if failedNode == node {
			delete(allChunkNodes, node)
			continue
		}
		workingChunkNodes[node] = true
	}

	//extract keys
	keys := make([]string, 0)
	for k := range workingChunkNodes {
		keys = append(keys, k)
	}

	//add a new replication node
	for {
		loc := getRandomStorageNodes(activeNodesMap).(string)
		if loc == failedNode {
			continue
		}
		workingChunkNodes[loc] = true
		if _, contains := allChunkNodes[loc]; !contains {
			//this is our new node
			newReplicationNodeLoc = loc
		}
		if len(workingChunkNodes) == messages.MAX_REPLICAS || len(activeNodesMap) < messages.MAX_REPLICAS {
			break
		}
	}

	log.Println(fmt.Sprintf(
		"%s node failed! new replication node %s created for chunk %s with replicas %s",
		failedNode, newReplicationNodeLoc, chunkFilename, reflect.ValueOf(workingChunkNodes).MapKeys()))

	//create chunk metadata
	chunkLocations := &messages.ChunkStorageLocations{
		ChunkFilename: chunkFilename, Replicas: keys,
	}
	chunkMetadata := messages.ChunkMetaData{
		FileName:     filename,
		Locations:    chunkLocations,
		RequestType:  messages.RequestType_Replica,
		SenderAddr:   fmt.Sprintf("%s:%s", HostName, Port),
		ReceiverAddr: newReplicationNodeLoc,
	}

	//send replication request to storage node
	sendReplicationMessage(&chunkMetadata, newReplicationNodeLoc)
}

/* send replication request to storage node */
func sendReplicationMessage(chunkMetadata *messages.ChunkMetaData, newReplicationNodeLoc string) {
	addr := strings.Split(newReplicationNodeLoc, ":")

	//connect to storage node
	msgHandler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		//msgHandler.Close()
		return
	}

	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ChunkMetadata{ChunkMetadata: chunkMetadata},
	}

	err = msgHandler.Send(wrapper)

	if err != nil {
		return
	}

	msgHandler.Close()
}

/* handle failed nodes */
func handleFailedNodes(failedNode string, info *StorageNodeInfo) {
	for _, file := range info.files {
		replaceFailedNode(file.filename, file.chunkFileName, failedNode)
	}
}

/* handle dead nodes */
func handleDeadNodes(deadNodes map[string]*StorageNodeInfo) {
	for addr, node := range deadNodes {
		handleFailedNodes(addr, node)
	}
}

/* monitor nodes */
func monitorNodes() {
	activeNodesMapMutex.Lock()
	deadNodes := make(map[string]*StorageNodeInfo)
	for addr, node := range activeNodesMap {
		timeDiff := math.Abs(time.Now().Sub(node.lastHeartbeatTime).Seconds())

		//check if the node is down
		if timeDiff > 2*messages.HEARTBEAT_PER_SEC {
			delete(activeNodesMap, addr)
			deadNodes[addr] = node
		}
	}
	activeNodesMapMutex.Unlock()

	go handleDeadNodes(deadNodes)
}

/* monitor nodes status */
func monitorNodesStatus() {
	for {
		monitorNodes()
	}
}

/* save chunk info to memory */
func getAndSaveChunkInfoToMemory(storageNodeMetadata *messages.StorageNodesMetadata) []*chunksInfo {
	addr := strings.Split(storageNodeMetadata.Addr, ":")
	hostname := addr[0]
	port := addr[1]

	//maintain file map
	files := storageNodeMetadata.GetFiles()

	chunks := make([]*chunksInfo, 0)

	fileChunkMapMutex.Lock()
	for _, chunkFileInfo := range files {
		filename := chunkFileInfo.Filename
		chunkFileName := chunkFileInfo.ChunkFilename
		chunkFilePath := chunkFileInfo.ChunkFilepath
		_, prs := fileChunkMap[filename]
		if !prs {
			fileChunkMap[filename] = map[string]map[string]string{}
		}
		_, prs = fileChunkMap[filename][chunkFileName]
		if !prs {
			fileChunkMap[filename][chunkFileName] = make(map[string]string, 0)
		}
		chunks = append(chunks, &chunksInfo{filename: filename, chunkFileName: chunkFileName, chunkFilePath: chunkFilePath})
		fileChunkMap[filename][chunkFileName][fmt.Sprintf("%s:%s", hostname, port)] = chunkFilePath
	}
	fileChunkMapMutex.Unlock()

	return chunks
}

/* save chunk and storage metadata to disk */
func saveChunkFileInfoToDisk() {
	fileChunkMapMutex.RLock()
	for filename, m := range fileChunkMap {
		filePath := filepath.Join(directory, "ChunkStorageNodeMetadata")
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			os.MkdirAll(filePath, os.ModePerm)
		}

		file, err := os.Create(filepath.Join(filePath, filename))
		messages.HandleError(err)

		for chunkFilename, storageInfo := range m {
			for addr, chunkFilePath := range storageInfo {
				arr := strings.Split(addr, ":")
				file.WriteString(fmt.Sprintf("%s, %s, %s, %s, %s\n", filename, chunkFilename, chunkFilePath, arr[0], arr[1]))
			}
		}

		err = file.Close()
		messages.HandleError(err)
	}
	fileChunkMapMutex.RUnlock()
}

/* save storage Node info to disk */
func saveStorageNodeInfo(storageNodeMetadata *messages.StorageNodesMetadata) {
	//log.Println("\n\n=====================\n")
	//log.Println("TotalNumberOfStorageRequests:", storageNodeMetadata.TotalNumberOfStorageRequests)
	//log.Println("Sender Addr:", storageNodeMetadata.SenderAddr)
	//log.Println("Request Type: ", storageNodeMetadata.RequestType)
	//log.Println("Addr:", storageNodeMetadata.Addr)
	//log.Println("Receiver Addr:", storageNodeMetadata.ReceiverAddr)
	//log.Println("Available Space:", storageNodeMetadata.AvailableSpace)
	////fmt.Println("Files:", storageNodeMetadata.Files)
	//log.Println("\n=======================\n\n")
	availableSpace := storageNodeMetadata.GetAvailableSpace()
	totalNumberOfStorageRequest := storageNodeMetadata.GetTotalNumberOfStorageRequests()
	addr := strings.Split(storageNodeMetadata.Addr, ":")
	hostname := addr[0]
	port := addr[1]

	//get chunk info and save it to disk
	chunks := getAndSaveChunkInfoToMemory(storageNodeMetadata)

	//save information to disk
	saveChunkFileInfoToDisk()

	nodeInfo := StorageNodeInfo{availableSpace: availableSpace, totalNumberOfStorageRequest: totalNumberOfStorageRequest, hostname: hostname, port: port, lastHeartbeatTime: time.Now(), files: chunks}

	//maintain active nodes map
	activeNodesMapMutex.Lock()
	activeNodesMap[fmt.Sprintf("%s:%s", hostname, port)] = &nodeInfo
	activeNodesMapMutex.Unlock()

}

/* send file metadata to client */
func sendFileMetadataToClient(metadata *messages.Wrapper_FileMetadata) {
	addr := strings.Split(metadata.FileMetadata.SenderAddr, ":")
	//establish connection
	msgHandler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		return
	}

	//update sender address and receiver address
	metadata.FileMetadata.ReceiverAddr = metadata.FileMetadata.SenderAddr
	metadata.FileMetadata.SenderAddr = fmt.Sprintf("%s:%s", HostName, Port)

	wrapper := &messages.Wrapper{
		Msg: metadata,
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

/* get replicas for storage node */
func getReplicasForStorageNode(msg *messages.Wrapper_ChunkMetadata) {
	filename := msg.ChunkMetadata.GetFileName()
	chunkFileName := msg.ChunkMetadata.GetLocations().GetChunkFilename()
	senderAddr := msg.ChunkMetadata.SenderAddr

	nodes := make([]string, 0)
	fileChunkMapMutex.RLock()
	for addr, _ := range fileChunkMap[filename][chunkFileName] {
		if addr != senderAddr {
			nodes = append(nodes, addr)
		}
	}
	fileChunkMapMutex.RUnlock()

	msg.ChunkMetadata.GetLocations().Replicas = nodes
}

/* send replicas to storage node */
func sendReplicasToStorageNode(msg *messages.Wrapper_ChunkMetadata) {
	//get replicas for given chunk name
	getReplicasForStorageNode(msg)

	addr := strings.Split(msg.ChunkMetadata.SenderAddr, ":")
	//establish connection
	msgHandler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		return
	}

	//update sender address and receiver address
	msg.ChunkMetadata.ReceiverAddr = msg.ChunkMetadata.SenderAddr
	msg.ChunkMetadata.SenderAddr = fmt.Sprintf("%s:%s", HostName, Port)

	wrapper := &messages.Wrapper{
		Msg: msg,
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

/*	send file not found exception to client */
func sendMessagesToClient(msgType messages.MsgType, senderAddr string, errorDescription string) {
	addr := strings.Split(senderAddr, ":")
	// establish connection
	msgHandler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		return
	}

	res := messages.Response{MsgType: msgType, Description: errorDescription}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_Response{Response: &res},
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

/* get active storage nodes for client */
func getActiveStorageNodeInfoForClient(metadata *messages.StorageNodesMetadata) {
	addr := strings.Split(metadata.SenderAddr, ":")

	activeNodesMapMutex.RLock()
	for _, node := range activeNodesMap {
		//establish connection
		msgHandler, err := messages.GetConnection(addr[0], addr[1])
		if err != nil {
			return
		}

		res := messages.StorageNodesMetadata{AvailableSpace: node.availableSpace, TotalNumberOfStorageRequests: node.totalNumberOfStorageRequest, Addr: fmt.Sprintf("%s:%s", node.hostname, node.port), SenderAddr: fmt.Sprintf("%s:%s", HostName, Port), ReceiverAddr: metadata.Addr}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_StorageNodesMetadata{StorageNodesMetadata: &res},
		}

		//send data
		msgHandler.Send(wrapper)

		//close connection
		msgHandler.Close()
	}
	activeNodesMapMutex.RUnlock()

}

func getFilesWithinFolder(folderName string) []string {
	files := make([]string, 0)
	fileMapMutex.RLock()
	for fileName, fileInfo := range fileMap {
		if fileInfo.folderName == folderName {
			files = append(files, fileName)
		}
	}
	fileMapMutex.RUnlock()
	return files
}

func getAndSendFilesWithinFolder(request *messages.Request) {
	files := getFilesWithinFolder(request.Description)

	addr := strings.Split(request.SenderAddr, ":")

	// establish connection
	msgHandler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		return
	}

	res := messages.Response{MsgType: messages.MsgType_SUCCESS, Description: fmt.Sprintf("%s \n %s", request.Description, strings.Join(files, " "))}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_Response{Response: &res},
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

func deleteFromStorageNodes(msg *messages.Wrapper_FileMetadata) {
	fileName := msg.FileMetadata.FileName
	fileChunkMapMutex.RLock()
	for chunkName, _ := range fileChunkMap[fileName] {
		sendStorageNodeDeleteRequest(fileName, chunkName)
	}
	fileChunkMapMutex.RUnlock()
}

func sendStorageNodeDeleteRequest(fileName string, chunkName string) {
	replicas := make([]string, 0)
	for addr, _ := range fileChunkMap[fileName][chunkName] {
		replicas = append(replicas, addr)
	}
	mainNode := replicas[0]
	replicas = replicas[1:]

	storageLocations := &messages.ChunkStorageLocations{ChunkFilename: chunkName, Replicas: replicas}

	chunkMetadata := messages.ChunkMetaData{FileName: fileName, Locations: storageLocations, RequestType: messages.RequestType_Delete, SenderAddr: fmt.Sprintf("%s:%s", HostName, Port), ReceiverAddr: mainNode}

	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ChunkMetadata{ChunkMetadata: &chunkMetadata},
	}

	addr := strings.Split(mainNode, ":")
	// establish connection
	msgHandler, err := messages.GetConnection(addr[0], addr[1])
	if err != nil {
		return
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

/* handle client request */
func handleClientRequest(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	for {
		wrapper, err := msgHandler.Receive()
		messages.HandleError(err)

		activeNodesMapMutex.RLock()
		log.Println(fmt.Sprintf("List of active Storage nodes: %s", reflect.ValueOf(activeNodesMap).MapKeys()))
		activeNodesMapMutex.RUnlock()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_FileMetadata:
			requestType := msg.FileMetadata.GetRequestType()
			if requestType == messages.RequestType_Write {
				log.Println(fmt.Sprintf("received file metadata from %s", msg.FileMetadata.SenderAddr))
				found := bf.Get([]byte(msg.FileMetadata.GetFileName()))
				fileMapMutex.RLock()
				_, prs := fileMap[msg.FileMetadata.FileName]
				fileMapMutex.RUnlock()
				if found && prs {
					log.Println(fmt.Sprintf("%s with similar name already present", msg.FileMetadata.GetFileName()))
					errorMsg := fmt.Sprintf("%s%s with similar name already present", msg.FileMetadata.GetFileName(), msg.FileMetadata.GetFileExtension())
					sendMessagesToClient(messages.MsgType_ERROR, msg.FileMetadata.SenderAddr, errorMsg)
				} else {
					log.Println(fmt.Sprintf("saving file metadata received from %s", msg.FileMetadata.SenderAddr))
					saveFileMetadata(msg)
					//put file name into bloom filter
					bf.Put([]byte(msg.FileMetadata.GetFileName()))
					log.Println(fmt.Sprintf("sending storage node locations to %s", msg.FileMetadata.SenderAddr))
					sendStorageNodesToClient(msg)
				}
			} else if requestType == messages.RequestType_Delete {
				found := bf.Get([]byte(msg.FileMetadata.GetFileName()))
				fileMapMutex.RLock()
				_, prs := fileMap[msg.FileMetadata.FileName]
				fileMapMutex.RUnlock()
				if !found || !prs {
					log.Println(fmt.Sprintf("%s not found", msg.FileMetadata.GetFileName()))
					errorMsg := fmt.Sprintf("File %s%s not found", msg.FileMetadata.GetFileName(), msg.FileMetadata.GetFileExtension())
					sendMessagesToClient(messages.MsgType_ERROR, msg.FileMetadata.SenderAddr, errorMsg)
				} else {
					log.Println(fmt.Sprintf("received file metadata from %s", msg.FileMetadata.SenderAddr))
					log.Println(fmt.Sprintf("deleting file metadata received from %s", msg.FileMetadata.SenderAddr))
					//delete from storage nodes
					deleteFromStorageNodes(msg)
					//delete file metadata
					deleteFileMetadata(msg)
					//send deletion request to client
					log.Println(fmt.Sprintf("sending deletion response to %s", msg.FileMetadata.SenderAddr))
					successMsg := fmt.Sprintf("Successfully removed %s", msg.FileMetadata.GetFileName())
					sendMessagesToClient(messages.MsgType_SUCCESS, msg.FileMetadata.SenderAddr, successMsg)
				}
			} else {
				//fetch file from bloom filter
				found := bf.Get([]byte(msg.FileMetadata.GetFileName()))
				fileMapMutex.RLock()
				_, prs := fileMap[msg.FileMetadata.FileName]
				fileMapMutex.RUnlock()
				if !found || !prs {
					log.Println(fmt.Sprintf("%s not found", msg.FileMetadata.GetFileName()))
					errorMsg := fmt.Sprintf("File %s%s not found", msg.FileMetadata.GetFileName(), msg.FileMetadata.GetFileExtension())
					sendMessagesToClient(messages.MsgType_ERROR, msg.FileMetadata.SenderAddr, errorMsg)
				} else {
					log.Println(fmt.Sprintf("received file retrieval request from %s", msg.FileMetadata.SenderAddr))
					log.Println(fmt.Sprintf("fetching file metadata for %s", msg.FileMetadata.SenderAddr))
					fileMapMutex.RLock()
					getFileMetadata(msg)
					fileMapMutex.RUnlock()
					log.Println(fmt.Sprintf("fetching storage node locations for %s", msg.FileMetadata.SenderAddr))
					getStorageNodesForClient(msg)
					log.Println(fmt.Sprintf("responding to retrieval request from %s", msg.FileMetadata.SenderAddr))
					sendFileMetadataToClient(msg)
				}
			}
			return
		case *messages.Wrapper_StorageNodesMetadata:
			if msg.StorageNodesMetadata.RequestType == messages.RequestType_Write {
				saveStorageNodeInfo(msg.StorageNodesMetadata)
			} else {
				getActiveStorageNodeInfoForClient(msg.StorageNodesMetadata)
			}
			return
		case *messages.Wrapper_ChunkMetadata:
			log.Println(fmt.Sprintf("received request for replicas from %s", msg.ChunkMetadata.SenderAddr))
			sendReplicasToStorageNode(msg)
			return
		case *messages.Wrapper_Request:
			log.Println(fmt.Sprintf("get files for user stored within folder %s", msg.Request.Description))
			getAndSendFilesWithinFolder(msg.Request)
		case nil:
			log.Println("Received an empty message, terminating controller")
			return
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

/* receive client messages */
func receiveMessages() {
	defer wg.Done()

	listener, err := net.Listen("tcp", ":"+Port)
	messages.HandleError(err)

	log.Println("Controller started! Waiting for connections...")

	for {
		if conn, err := listener.Accept(); err == nil {
			log.Println(fmt.Sprintf("%s connected", conn.RemoteAddr()))
			msgHandler := messages.NewMessageHandler(conn)
			go handleClientRequest(msgHandler)
		}
	}
}

/* parse file data */
func parseFile(content []string) (string, FileInfo) {
	tempFileName := strings.TrimSpace(content[0])
	filename := strings.TrimSpace(strings.Split(tempFileName, ":")[1])

	tempChunkSize := strings.TrimSpace(content[1])
	tempChunkSize = strings.TrimSpace(strings.Split(tempChunkSize, ":")[1])
	chunkSize, _ := strconv.ParseUint(tempChunkSize, 10, 64)

	tempFileChecksum := strings.TrimSpace(content[2])
	fileChecksum := strings.TrimSpace(strings.Split(tempFileChecksum, ":")[1])

	tempTotalNumberOfChunks := strings.TrimSpace(content[3])
	tempTotalNumberOfChunks = strings.TrimSpace(strings.Split(tempTotalNumberOfChunks, ":")[1])
	totalNumberOfChunks, _ := strconv.ParseUint(tempTotalNumberOfChunks, 10, 64)

	tempFileSize := strings.TrimSpace(content[4])
	tempFileSize = strings.TrimSpace(strings.Split(tempFileSize, ":")[1])
	fileSize, _ := strconv.ParseUint(tempFileSize, 10, 64)

	tempFolderName := strings.TrimSpace(content[5])
	folderName := strings.TrimSpace(strings.Split(tempFolderName, ":")[1])

	fileInfo := FileInfo{chunkSize: chunkSize, fileChecksum: fileChecksum, totalNumberOfChunks: totalNumberOfChunks, fileSize: fileSize, folderName: folderName}

	return filename, fileInfo
}

/* read file */
func readFile(filePath string) {
	file, err := os.Open(filePath)
	messages.HandleError(err)

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		text := scanner.Text()
		content := strings.Split(text, ",")

		filename, fileInfo := parseFile(content)
		fileMapMutex.Lock()
		fileMap[filename] = &fileInfo
		fileMapMutex.Unlock()
		bf.Put([]byte(filename))
	}

	err = scanner.Err()
	messages.HandleError(err)
}

/*
	load data from the disk before startup
	if the controller fails
*/
func loadData() {
	log.Println("starting process of restoring data from disk")

	dir := filepath.Join(directory, "FileMetaData")
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
		readFile(filePath)
	}
	log.Println("restoring data from disk completed")
}

func main() {
	loadData()

	go receiveMessages()
	go monitorNodesStatus()
	// wait for go routines to complete
	wg.Add(2)

	wg.Wait()

}
