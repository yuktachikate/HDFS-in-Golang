# Distributed File System (DFS) Implementation in Go

## Overview
This is a distributed file system (DFS) implemented in Go. It supports functionalities like file storage, retrieval, and deletion across multiple storage nodes. It employs techniques like replication, fault tolerance, and concurrency to ensure the durability and availability of data. The system is designed using the client-server architecture, where the controller manages metadata and storage nodes store the actual data chunks. This project also utilizes Go’s multithreading and Protocol Buffers (ProtoBuf) for efficient data handling and communication.

## Key Features:
- **Replication:** Ensures data durability by replicating file chunks across multiple storage nodes.
- **Fault Tolerance:** Detects node failures and maintains replication by redistributing data.
- **Concurrency:** Utilizes Go’s multithreading capabilities to handle multiple operations concurrently and efficiently.
- **Protocol Buffers (ProtoBuf):** Used for efficient serialization of data exchanged between the controller, storage nodes, and clients.

## Endpoints:

### 1. Start Controller
    $ cd hdfs
    $ cd controller
    Sample Request: go run controller.go {port} {directory}
    Usage:          go run controller.go 8000 /home/BigData/Controller

### 2. Start Storage Node
    $ cd hdfs
    $ cd nodes
    Sample Request: go run storagenode.go {controller hostname} {controller port} {port} {directory}
    Usage:          go run storagenode.go 127.0.0.1 8000 8001 /home/BigData/StorageNode1

### 3. Start Client
    $ cd hdfs
    $ cd client
    Sample Request: go run client.go {controller hostname} {controller port} {port} {directory}
    Usage:          go run client.go 127.0.0.1 8000 8800 /home/BigData/Client

#### a. Save File
    Sample Request: /write {file to be saved} {size of each chunk} {folder name}
    Usage:          /write /home/test.pdf 1000 test

#### b. Retrieve File
    Sample Request: /read {file to be retrieved}
    Usage:          /read test.pdf

#### c. List Active Storage Nodes
    Sample Request: /list
    Usage:          /list

#### d. List Directories
    Sample Request: /ls {directory name}
    Usage:          /ls /home/BigData

#### e. Exit
    Sample Request: /quit or /q
    Usage:          /quit or /q

#### f. Delete a File
    Sample Request: /delete {file to be deleted}
    Usage:          /delete test.pdf

#### To run client:
    cd client/
    go run client.go gamma01 11100 11998 /bigdata/$(whoami)/Client

## Architecture

![HDFS.png](HDFS.png)

## Storage Workflow:
1. Break a file into chunks.
2. Send file metadata (filename, filesize, number of chunks, file checksum, etc.) to Controller and request storage locations for the chunks.
3. Controller checks if the file already exists in the bloom filter:
    - If it does, the request is rejected.
    - Otherwise, the controller inserts the file into the bloom filter, saves metadata to disk and memory, and uses a random generator to determine storage locations for each chunk.
4. Client sends chunk data to the assigned storage locations, including metadata (checksum, chunk size, associated filename, etc.), and replicates the chunk across multiple nodes.
5. Storage nodes save the chunks, replicate them to other nodes, and send heartbeats to the controller to confirm active status.
6. Controller updates storage node status and saves chunk information in disk and memory.

## Retrieval Workflow:
1. Client requests a file from the controller.
2. Controller checks for the file in the bloom filter:
    - If it does not exist, an error response is sent.
    - If it exists, a list of storage nodes and file metadata is returned.
3. Client retrieves the chunks from the respective storage nodes.
4. Storage nodes verify the integrity of the requested chunks, using the checksum. If a chunk is corrupted, replicas are retrieved from other storage nodes.
5. The client merges the received chunks, validates them with the checksum, and reconstructs the original file.

## Deletion Workflow:
1. Client requests the deletion of a file from the controller.
2. Controller checks the bloom filter for the file:
    - If the file does not exist, an error response is sent.
    - If it exists, the controller sends deletion requests to all storage nodes holding the file and its replicas.
3. Storage nodes delete the chunks and notify the controller of successful deletion.
4. The controller removes the file metadata from its index and storage, then sends a deletion success message to the client.

## How to Use:
1. Clone the repository.
2. Run the controller and storage nodes.
3. Use the client to store and retrieve files.

## Requirements:
- Go 1.x+
- Any operating system that supports Go
