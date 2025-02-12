package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hdfs/messages"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var wg sync.WaitGroup
var file_path string
var chunk_size string
var folder_name = "root"
var controllerHostname = os.Args[1]
var controllerPort = os.Args[2]
var HostName, _ = os.Hostname()
var Port = os.Args[3]
var directory = os.Args[4]

var totalNumberOfChunks uint64
var fileCheckSum string
var chunksReceived uint64 = 0
var chunksSent uint64 = 0
var fileSize uint64 = 0
var receivedBytes float64 = 0
var sentBytes float64 = 0

/* get file size */
func getFileSize(filepath string) int64 {
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		log.Fatalln(err.Error())
		os.Exit(1)
	}
	// get the size
	size := fileInfo.Size()
	return size
}

/* get file check sum */
func getCheckSum(file *os.File) string {
	h := sha256.New()
	_, err := io.Copy(h, file)
	messages.HandleError(err)
	checksum := hex.EncodeToString(h.Sum(nil))
	//revert file pointer to
	file.Seek(0, 0)
	return checksum
}

/* get number of chunks */
func getNumberOfChunks(fileSize int64, chunkSize int64) uint64 {
	return uint64(math.Ceil(float64(fileSize) / float64(chunkSize)))
}

/* open a file */
func openFile(path string) *os.File {
	// open the file
	file, err := os.Open(path)
	messages.HandleError(err)

	return file
}

/* close a file */
func closeFile(file *os.File) {
	file.Close()
}

/* extract file metadata */
func getFileMetadata(file *os.File, receiverHostName string, receiverPort string, folderName string) *messages.Wrapper {
	// convert to int from string
	chunkSize, err := strconv.ParseInt(chunk_size, 0, 64)
	messages.HandleError(err)

	// get the size
	fileSize := getFileSize(file.Name())
	//get the checksum
	checksum := getCheckSum(file)
	//get number of chunks
	numberOfChunks := getNumberOfChunks(fileSize, chunkSize)

	// create file metadata proto
	filename := filepath.Base(file.Name())
	filenameWithoutExtension := strings.TrimSuffix(filename, filepath.Ext(filename))

	metadata := messages.FileMetaData{FileName: filenameWithoutExtension, FileExtension: filepath.Ext(filename), TotalChunks: numberOfChunks, ChunkSize: uint64(chunkSize), FileSize: uint64(fileSize), Checksum: checksum, RequestType: messages.RequestType_Write, SenderAddr: fmt.Sprintf("%s:%s", HostName, Port), ReceiverAddr: fmt.Sprintf("%s:%s", receiverHostName, receiverPort), FolderName: folderName}

	// send file metadata
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_FileMetadata{FileMetadata: &metadata},
	}

	return wrapper
}

/* send chunk data to storage nodes */
func sendFileDataToStorageNodes(filepath string, fileMetadata *messages.FileMetaData) {
	//open file
	file := openFile(filepath)

	sentBytes = 0.0
	chunksSent = 0
	numberOfChunks := fileMetadata.GetTotalChunks()
	chunkSize := fileMetadata.GetChunkSize()
	filenameWithoutExtension := fileMetadata.GetFileName()
	fileExtension := fileMetadata.GetFileExtension()
	locations := fileMetadata.GetLocations()

	for i := uint64(0); i < numberOfChunks; i++ {
		mainNode, replicas := locations[i].Replicas[0], locations[i].Replicas[1:]
		location := strings.Split(mainNode, ":")
		hostname := location[0]
		port := location[1]

		//establish connection with storage Node
		msgHandler, err := messages.GetConnection(hostname, port)
		if err != nil {
			return
		}

		// create buffer of chunk size
		buffer := bytes.NewBuffer(make([]byte, 0, chunkSize))

		// copy contents to buffer
		n, err := io.CopyN(buffer, file, int64(chunkSize))

		// get check sum of the chunk
		sha256HashInBytes := sha256.Sum256(buffer.Bytes())
		sha256HashInString := hex.EncodeToString(sha256HashInBytes[:])

		storageLocations := &messages.ChunkStorageLocations{ChunkFilename: fmt.Sprintf("%s_chunk_%d", filenameWithoutExtension, i), Replicas: replicas}

		chunkMetadata := messages.ChunkMetaData{FileName: filenameWithoutExtension, FileExtension: fileExtension, Locations: storageLocations, ChunkSize: uint64(n), Checksum: sha256HashInString, RequestType: messages.RequestType_Write, SenderAddr: fmt.Sprintf("%s:%s", HostName, Port), ReceiverAddr: fmt.Sprintf("%s:%s", hostname, port)}

		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_ChunkMetadata{ChunkMetadata: &chunkMetadata},
		}

		//send file data
		msgHandler.Send(wrapper)

		msgHandler.GetConnection().Write(buffer.Bytes())

		sentBytes += float64(n)
		chunksSent++

		//close connection
		msgHandler.Close()

		if err == io.EOF {
			break
		}
	}

	closeFile(file)
}

///* delete chunk data from storage nodes */
//func deleteChunkDataFromStorageNodes(fileMetadata *messages.FileMetaData) {
//	locations := fileMetadata.GetLocations()
//	filenameWithoutExtension := fileMetadata.GetFileName()
//	fileExtension := fileMetadata.GetFileExtension()
//	totalNumberOfChunks = fileMetadata.GetTotalChunks()
//	fileCheckSum = fileMetadata.GetChecksum()
//	fileSize = fileMetadata.GetFileSize()
//
//	for _, location := range locations {
//		node, _ := location.Replicas[0], location.Replicas[1:]
//		addr := strings.Split(node, ":")
//		//establish a connection
//		handler := messages.GetConnection(addr[0], addr[1])
//
//		storageLocations := &messages.ChunkStorageLocations{ChunkFilename: location.ChunkFilename}
//
//		chunkMetadata := messages.ChunkMetaData{
//			FileName:      filenameWithoutExtension,
//			FileExtension: fileExtension,
//			Locations:     storageLocations,
//			RequestType:   messages.RequestType_Delete,
//			SenderAddr:    fmt.Sprintf("%s:%s", HostName, Port),
//			ReceiverAddr:  node,
//		}
//
//		wrapper := &messages.Wrapper{
//			Msg: &messages.Wrapper_ChunkMetadata{ChunkMetadata: &chunkMetadata},
//		}
//		// send data
//		handler.Send(wrapper)
//
//		//close connection
//		handler.Close()
//	}
//}

/* retrieve chunk data from storage nodes */
func retrieveChunkDataFromStorageNodes(fileMetadata *messages.FileMetaData) {
	locations := fileMetadata.GetLocations()
	filenameWithoutExtension := fileMetadata.GetFileName()
	fileExtension := fileMetadata.GetFileExtension()
	totalNumberOfChunks = fileMetadata.GetTotalChunks()
	fileCheckSum = fileMetadata.GetChecksum()
	fileSize = fileMetadata.GetFileSize()
	folder_name = fileMetadata.GetFolderName()

	receivedBytes = 0.0
	chunksReceived = 0

	for _, location := range locations {
		node, _ := location.Replicas[0], location.Replicas[1:]
		addr := strings.Split(node, ":")
		//establish a connection
		handler, err := messages.GetConnection(addr[0], addr[1])
		if err != nil {
			return
		}

		storageLocations := &messages.ChunkStorageLocations{ChunkFilename: location.ChunkFilename}

		chunkMetadata := messages.ChunkMetaData{FileName: filenameWithoutExtension, FileExtension: fileExtension, Locations: storageLocations, RequestType: messages.RequestType_Read, SenderAddr: fmt.Sprintf("%s:%s", HostName, Port), ReceiverAddr: node}

		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_ChunkMetadata{ChunkMetadata: &chunkMetadata},
		}
		// send data
		handler.Send(wrapper)

		//close connection
		handler.Close()
	}
}

/* save chunk data to disk */
func saveChunkInfoToDisk(chunkMetaData *messages.ChunkMetaData, conn net.Conn) {
	chunkFileName := chunkMetaData.GetLocations().ChunkFilename
	filePath := filepath.Join(directory, folder_name)
	chunkSize := chunkMetaData.GetChunkSize()

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		os.MkdirAll(filePath, os.ModePerm)
	}

	file, err := os.Create(filepath.Join(filePath, chunkFileName))
	messages.HandleError(err)
	defer file.Close()

	n, err := io.CopyN(file, conn, int64(chunkSize))
	messages.HandleError(err)

	receivedBytes += float64(n)
	chunksReceived++
}

/* print storage nodes */
func printStorageNodeInfo(msg *messages.StorageNodesMetadata) {
	fmt.Println(fmt.Sprintf("Storage Node: %s, Available Space: %d, Total Number of Storage Request Processed: %d", msg.Addr, msg.AvailableSpace, msg.TotalNumberOfStorageRequests))
}

/* handle client request */
func handleClientRequest(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	for {
		wrapper, err := msgHandler.Receive()
		messages.HandleError(err)

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_FileMetadata:
			requestType := msg.FileMetadata.GetRequestType()
			log.Println("received storage locations from controller")
			if requestType == messages.RequestType_Write {
				log.Println("sending request to storage nodes for saving chunk data")
				sendFileDataToStorageNodes(file_path, msg.FileMetadata)
				log.Println(fmt.Sprintf("For Filename: %s Sent Bytes: %f and ChunksSent: %d", msg.FileMetadata.FileName, sentBytes, chunksSent))
			} else {
				log.Println("sending request to storage nodes for retrieving chunk data")
				retrieveChunkDataFromStorageNodes(msg.FileMetadata)
			}
			return
		case *messages.Wrapper_ChunkMetadata:
			log.Println(fmt.Sprintf("received chunk data %s from storage node %s", msg.ChunkMetadata.GetLocations().ChunkFilename, msg.ChunkMetadata.SenderAddr))
			log.Println(fmt.Sprintf("saving chunk data %s to disk received from %s", msg.ChunkMetadata.GetLocations().ChunkFilename, msg.ChunkMetadata.SenderAddr))
			log.Println(fmt.Sprintf("For Filename: %s Received Bytes: %f and ChunksReceived: %d", msg.ChunkMetadata.FileName, receivedBytes, chunksReceived))
			saveChunkInfoToDisk(msg.ChunkMetadata, msgHandler.GetConnection())
			if chunksReceived == totalNumberOfChunks && fileSize == uint64(receivedBytes) {
				log.Println(fmt.Sprintf("For Filename: %s Received Bytes: %f and ChunksReceived: %d", msg.ChunkMetadata.FileName, receivedBytes, chunksReceived))
				log.Println("Now merging chunks...")
				mergeChunks()
			}
			return
		case *messages.Wrapper_StorageNodesMetadata:
			printStorageNodeInfo(msg.StorageNodesMetadata)
			return
		case *messages.Wrapper_Response:
			fmt.Println(fmt.Sprintf("%s", msg.Response.Description))
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

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			//log.Println(fmt.Sprintf("%s connected", conn.RemoteAddr().String()))
			go handleClientRequest(msgHandler)
		}
	}
}

/* send file metadata */
func sendFileMetadata(hostname string, port string, folder string) {
	//establish connection with controller
	msgHandler, err := messages.GetConnection(hostname, port)
	if err != nil {
		return
	}

	file := openFile(file_path)

	// get file metadata
	wrapper := getFileMetadata(file, hostname, port, folder)

	//start sending file metadata
	msgHandler.Send(wrapper)

	//close file
	closeFile(file)

	//close connection
	msgHandler.Close()
}

/* retrieve file metadata */
func retrieveFileMetadata(hostname string, port string) {
	//establish connection with controller
	msgHandler, err := messages.GetConnection(hostname, port)
	if err != nil {
		return
	}

	filename := filepath.Base(file_path)
	filenameWithoutExtension := strings.TrimSuffix(filename, filepath.Ext(filename))

	metadata := messages.FileMetaData{FileName: filenameWithoutExtension, FileExtension: filepath.Ext(filename), RequestType: messages.RequestType_Read, SenderAddr: fmt.Sprintf("%s:%s", HostName, Port), ReceiverAddr: fmt.Sprintf("%s:%s", hostname, port)}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_FileMetadata{FileMetadata: &metadata},
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

/* delete file */
func deleteFile(hostname string, port string) {
	//establish connection with controller
	msgHandler, err := messages.GetConnection(hostname, port)
	if err != nil {
		return
	}

	filename := filepath.Base(file_path)
	filenameWithoutExtension := strings.TrimSuffix(filename, filepath.Ext(filename))

	metadata := messages.FileMetaData{
		FileName:      filenameWithoutExtension,
		FileExtension: filepath.Ext(filename),
		RequestType:   messages.RequestType_Delete,
		SenderAddr:    fmt.Sprintf("%s:%s", HostName, Port),
		ReceiverAddr:  fmt.Sprintf("%s:%s", hostname, port),
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_FileMetadata{FileMetadata: &metadata},
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

/* merge chunks */
func mergeChunks() {
	filename := filepath.Base(file_path)
	filenameWithoutExtension := strings.TrimSuffix(filename, filepath.Ext(filename))

	filePath := filepath.Join(directory, folder_name)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		os.MkdirAll(filePath, os.ModePerm)
	}

	//open file in append mode
	newfile, err := os.Create(filepath.Join(filePath, filenameWithoutExtension))
	messages.HandleError(err)

	var writePosition int64 = 0

	// iterate through all the chunk files
	for j := uint64(0); j < totalNumberOfChunks; j++ {

		currentChunkFileName := fmt.Sprintf("%s_chunk_%d", filenameWithoutExtension, j)
		currentChunkFilePath := filepath.Join(directory, folder_name, currentChunkFileName)

		// open a chunk
		currentChunkFile, err := os.Open(currentChunkFilePath)
		messages.HandleError(err)

		defer currentChunkFile.Close()

		// calculate the bytes size of each chunk
		chunkInfo, err := currentChunkFile.Stat()
		messages.HandleError(err)

		var chunkSize = chunkInfo.Size()

		chunkBufferBytes := make([]byte, chunkSize)

		writePosition += chunkSize

		// read into chunkBufferBytes
		reader := bufio.NewReader(currentChunkFile)
		_, err = reader.Read(chunkBufferBytes)
		messages.HandleError(err)

		_, err = newfile.Write(chunkBufferBytes)
		messages.HandleError(err)

		//flush to disk
		newfile.Sync()

		// reset or empty our buffer
		chunkBufferBytes = nil

		// remove file once done merging
		os.Remove(currentChunkFilePath)

	}

	// now, we close the newFileName
	newfile.Close()

	log.Println("Done constructing new file: ", filename)

	log.Println("start file validation")
	isValidFile, _ := messages.ValidateFile(fileCheckSum, filepath.Join(filePath, filenameWithoutExtension))
	if isValidFile {
		log.Println(fmt.Sprintf("File %s successfully validated", filenameWithoutExtension))
	} else {
		log.Println(fmt.Sprintf("File %s is corrupted", filenameWithoutExtension))
		os.Remove(filepath.Join(filePath, filenameWithoutExtension))
	}
}

/* retrieve active storage node info from controller*/
func retrieveActiveStorageNodesInfoFromController(hostname string, port string) {
	//establish connection with controller
	msgHandler, err := messages.GetConnection(hostname, port)
	if err != nil {
		return
	}

	req := messages.StorageNodesMetadata{RequestType: messages.RequestType_Read, SenderAddr: fmt.Sprintf("%s:%s", HostName, Port), ReceiverAddr: fmt.Sprintf("%s:%s", hostname, port)}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_StorageNodesMetadata{StorageNodesMetadata: &req},
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

/* print directories, subdirectories and files */
//func visit(p string, info os.FileInfo, err error) error {
//	if err != nil {
//		return err
//	}
//	if !info.IsDir() {
//		fmt.Println(fmt.Sprintf("  %s", p))
//	} else {
//		fmt.Println(fmt.Sprintf(" %s", p))
//	}
//	return nil
//}

/* listing directories, subdirectories and files within given directory */
//func listFilesWithinDirectory(filePath string) {
//	err := filepath.Walk(filePath, visit)
//	messages.HandleError(err)
//}

func retrieveFilesWithinGivenFolder(hostname string, port string, folderName string) {
	//establish connection
	msgHandler, err := messages.GetConnection(hostname, port)
	if err != nil {
		return
	}

	req := messages.Request{RequestType: messages.RequestType_FOLDER_INFO, Description: folderName, SenderAddr: fmt.Sprintf("%s:%s", HostName, Port), ReceiverAddr: fmt.Sprintf("%s:%s", hostname, port)}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_Request{Request: &req},
	}

	//send data
	msgHandler.Send(wrapper)

	//close connection
	msgHandler.Close()
}

/* get user input */
func getUserInput() {
	defer wg.Done()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		result := scanner.Scan() // Reads up to a \n newline character
		if result == false {
			break
		}

		message := scanner.Text()
		if len(message) != 0 {
			tokens := strings.Split(message, " ")
			if tokens[0] == "/write" {
				file_path = tokens[1]
				chunk_size = tokens[2]
				if len(tokens) > 3 {
					folder_name = tokens[3]
				}
				log.Println("sending file metadata to controller")
				sendFileMetadata(controllerHostname, controllerPort, folder_name)
			} else if tokens[0] == "/list" {
				retrieveActiveStorageNodesInfoFromController(controllerHostname, controllerPort)
			} else if tokens[0] == "/delete" {
				file_path = tokens[1]
				log.Println(fmt.Sprintf("delete file %s from storage nodes", file_path))
				deleteFile(controllerHostname, controllerPort)
			} else if tokens[0] == "/ls" {
				folder_name = tokens[1]
				//listFilesWithinDirectory(file_path)
				retrieveFilesWithinGivenFolder(controllerHostname, controllerPort, folder_name)
			} else if strings.ToLower(tokens[0]) == "/quit" || strings.ToLower(tokens[0]) == "/q" {
				log.Println("exiting client. good bye!")
				os.Exit(0)
			} else {
				file_path = tokens[1]
				log.Println("retrieving file metadata from controller")
				retrieveFileMetadata(controllerHostname, controllerPort)
			}
		}
	}
}

func main() {
	//get user input
	go getUserInput()

	//receive messages
	go receiveMessages()

	// wait for go routines to complete
	wg.Add(2)

	wg.Wait()
}
