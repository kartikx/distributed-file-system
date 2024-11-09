package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

var fileInfoMap = map[string]*FileInfo{}
var fileBlockMap = map[string][]*FileBlock{}

var tempFileInfoMap = map[string]*FileInfo{}
var tempFileBlockMap = map[string][]*FileBlock{}

type FileInfo struct {
	Name      string
	NumBlocks int
	// Shouldn't be exported, because different nodes will have different isPrimary values for the same file.
	isPrimary  bool
	mostRecent bool
}

type FileBlock struct {
	Name    string
	Content []byte
	// Shouldn't be exported, because different nodes will have different block IDs for the same file.
	blockId int
}

type GetMessage struct {
	Name      string
	Requester string
}

// Checks whether this node has become the primary replica for any file.
// Membership Info should have been updated before invoking this function.
// Returns primary replicas.
func UpdatePrimaryReplicas() []*FileInfo {
	fmt.Println("Updating primary replica status for all files on this node")
	var filenames []*FileInfo

	for _, f := range fileInfoMap {
		if !f.isPrimary && GetPrimaryReplicaForFile(f.Name) == NODE_ID {
			// f is a pointer so this is ok
			f.isPrimary = true
		}

		if f.isPrimary {
			filenames = append(filenames, f)
		}
	}

	return filenames
}

func PrintStoredFiles() {
	fmt.Println("===STORED FILES===")
	for name, file := range fileInfoMap {
		var ch string
		if file.isPrimary {
			ch = "*"
		}
		fmt.Printf("%s %s (%d blocks)\n", name, ch, file.NumBlocks)
	}
	fmt.Println("===")
}

func CreateHDFSFile(localfilename string, hdfsfilename string) error {
	nodeId := GetPrimaryReplicaForFile(hdfsfilename)

	fmt.Println("File hash: ", GetRingPosition(hdfsfilename))

	content, err := os.ReadFile(localfilename)
	if err != nil {
		return err
	}

	fileInfo := FileInfo{hdfsfilename, 0, false, true}
	encodedFileInfo, err := json.Marshal(fileInfo)

	if err != nil {
		return err
	}

	createMessage := Message{Kind: CREATE, Data: string(encodedFileInfo)}

	fileBlock := FileBlock{hdfsfilename, content, 0}
	encodedFileBlock, err := json.Marshal(fileBlock)

	if err != nil {
		return err
	}

	appendMessage := Message{Kind: APPEND, Data: string(encodedFileBlock)}

	if nodeId == NODE_ID {
		create_err := ProcessCreateMessage(createMessage, false)
		fmt.Printf("Created Local file\n")
		if create_err == nil {
			append_err := ProcessAppendMessage(appendMessage, false)
			fmt.Printf("Appended Local file\n")
			return append_err
		}
		return create_err

	} else {
		create_err := SendMessage(nodeId, createMessage)
		if create_err == nil {
			append_err := SendMessage(nodeId, appendMessage)
			return append_err
		}
		return create_err
	}
}

// Load the localfile and append it to an existing file on HyDFS
func AppendToHDFSFile(localfilename string, hdfsfilename string) error {
	nodeId := GetPrimaryReplicaForFile(hdfsfilename)

	fmt.Println("File hash: ", GetRingPosition(hdfsfilename))

	// Check if the file even exists on HyDFS
	checkMessage := Message{Kind: CHECK, Data: hdfsfilename}

	// Send a CHECK message and get the response CHECK message
	responseMessage, err := SendMessageGetReply(nodeId, checkMessage)
	if err == nil {
		var responseFileInfo FileInfo
		err := json.Unmarshal([]byte(responseMessage.Data), &responseFileInfo)
		// If the file does not exist, response FileInfo has an empty filename
		if err == nil && responseFileInfo.Name == "" {
			fmt.Printf("File %s does not exist on HyDFS, not appending %s", hdfsfilename, localfilename)
			return fmt.Errorf("tried to append localfile to a file that does not exist on HyDFS")
		}
	}

	// Read the contents of the localfile
	content, err := os.ReadFile(localfilename)
	if err != nil {
		return err
	}

	// Make a new fileblock with the content of the localfile
	fileBlock := FileBlock{hdfsfilename, content, 0}
	encodedFileBlock, err := json.Marshal(fileBlock)

	if err != nil {
		return err
	}

	// This fileblock will be appended to the HyDFS file
	appendMessage := Message{Kind: APPEND, Data: string(encodedFileBlock)}

	if nodeId == NODE_ID {
		append_err := ProcessAppendMessage(appendMessage, false)
		fmt.Printf("Appended Local file\n")
		return append_err

	} else {
		append_err := SendMessage(nodeId, appendMessage)
		fmt.Printf("Sent a message to %s append a localfile\n", nodeId)
		return append_err
	}
}

// Creates file on local disk and triggers replication.
func CreateLocalFile(filename string, isTemp bool) error {
	fmt.Printf("Creating file with name: %s \n", filename)

	isPrimaryReplica := false
	if GetPrimaryReplicaForFile(filename) == NODE_ID {
		isPrimaryReplica = true
	}

	fileInfo := &FileInfo{filename, 0, isPrimaryReplica, true}

	var fileInfoMapToUse map[string]*FileInfo
	if isTemp {
		fileInfoMapToUse = tempFileInfoMap
	} else {
		fileInfoMapToUse = fileInfoMap
	}

	_, ok := fileInfoMapToUse[fileInfo.Name]

	if !ok {
		fileInfoMapToUse[fileInfo.Name] = fileInfo
	} else {
		// TODO @kartikr2 Throw error if file already exists. Should be propagated across the network.
		return fmt.Errorf("file already exists on the HDFS")
	}

	dirName := STORAGE_LOCATION + filename
	if isTemp {
		dirName += "_temp"
	}
	err := os.MkdirAll(dirName, 0777)
	if err != nil {
		return err
	}

	// If you are the primary replica for this file, make the successors create the file too
	if GetPrimaryReplicaForFile(filename) == NODE_ID {
		replicateFileInfo := FileInfo{filename, 0, isPrimaryReplica, true}
		encodedFileInfo, err := json.Marshal(replicateFileInfo)
		if err != nil {
			return err
		}
		createMessage := Message{Kind: CREATE, Data: string(encodedFileInfo)}
		// Wait for both replicas to create the file
		// Otherwise this might result in a case where a replica gets an APPEND before a CREATE
		err = PerformReplication(createMessage, true)
		if err != nil {
			return err
		}
	}

	return err
}

func RequestFile(hdfsfilename string) error {

	// Do a CHECK to find if the primary replica has the file
	fileNodeId := GetPrimaryReplicaForFile(hdfsfilename)

	checkMessage := Message{Kind: CHECK, Data: hdfsfilename}

	// Send a CHECK message and get the response CHECK message
	responseMessage, err := SendMessageGetReply(fileNodeId, checkMessage)
	if err != nil {
		return err
	}

	var responseFileInfo FileInfo
	err = json.Unmarshal([]byte(responseMessage.Data), &responseFileInfo)

	// If the file does not exist, response FileInfo has an empty filename
	if err != nil {
		return err
	}

	if responseFileInfo.Name == "" {
		return fmt.Errorf("primary replica does not have the HDFS file %s", hdfsfilename)
	} else {
		// Make a new struct to have info about who is requesting what
		getMessageStruct := GetMessage{Name: hdfsfilename, Requester: NODE_ID}
		encodedGetMessageStruct, err := json.Marshal(getMessageStruct)
		if err != nil {
			return err
		}
		// Construct a message to ask for a file
		getFileMessage := Message{Kind: GETFILE, Data: string(encodedGetMessageStruct)}
		// Send a GETFILE message
		err = SendMessage(fileNodeId, getFileMessage)
		if err != nil {
			return fmt.Errorf("unable to requeset file %s from %s", hdfsfilename, fileNodeId)
		}

		// Wait till you get the file info
		// TODO @kartikr2 This could throw a race condition.
		var newFileInfo *FileInfo
		for {
			_, ok := tempFileInfoMap[hdfsfilename]
			if ok {
				newFileInfo = tempFileInfoMap[hdfsfilename]
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		// Wait till you get all the blocks (maximum of 10 seconds)
		waitStart := time.Now()
		for {
			if (responseFileInfo.NumBlocks <= newFileInfo.NumBlocks) || (time.Since(waitStart).Seconds() > 10) {
				break
			}

			time.Sleep(1 * time.Second)
		}
	}

	return nil
}

func MergeHDFSFile(hdfsfilename string) error {

	if GetPrimaryReplicaForFile(hdfsfilename) == NODE_ID {
		if fileInfoMap[hdfsfilename].mostRecent {
			// The file has been merged recently with no new appends to it
			fmt.Printf("File %s has had no recent updates. Merge is a no-op\n", hdfsfilename)
			return nil
		}

		successors := GetRingSuccessors(RING_POSITION)

		for _, succ := range successors {
			// Delete this file on the successors
			deleteMessage := Message{Kind: DELETE, Data: hdfsfilename}
			err := SendMessage(succ, deleteMessage)
			if err != nil {
				return err
			}
			// TODO do we need this? This is just to make sure that the DELETE message is processed on the replicas
			time.Sleep(1 * time.Second)
		}

		// (Re)Create the file on the successors
		// Make a new FileInfo with 0 blocks to send to the replicas
		replicateFileInfo := FileInfo{hdfsfilename, 0, false, true}
		encodedFileInfo, err := json.Marshal(replicateFileInfo)
		if err != nil {
			return err
		}
		createMessage := Message{Kind: CREATE, Data: string(encodedFileInfo)}
		// Wait for both replicas to create the file
		err = PerformReplication(createMessage, true)
		if err != nil {
			return err
		}

		// Send all the appends to the successors
		for _, eachhdfsfileblock := range fileBlockMap[hdfsfilename] {
			encodedFileBlock, err := json.Marshal(*eachhdfsfileblock)
			if err != nil {
				return err
			}
			appendMessage := Message{Kind: APPEND, Data: string(encodedFileBlock)}
			// Wait for both replicas to do the append
			err = PerformReplication(appendMessage, true)
			if err != nil {
				return err
			}

		}

		// Subsequent immediate merges will be a no-op
		fileInfoMap[hdfsfilename].mostRecent = true

	} else {
		nodeId := GetPrimaryReplicaForFile(hdfsfilename)
		mergeMessage := Message{Kind: MERGE, Data: hdfsfilename}
		err := SendMessage(nodeId, mergeMessage)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetHDFSToLocal(hdfsfilename string, localfilename string) error {
	fmt.Printf("Getting HDFS File %s to local file %s", hdfsfilename, localfilename)

	var fileBlockMapToUse map[string][]*FileBlock

	if GetPrimaryReplicaForFile(hdfsfilename) != NODE_ID {
		// Get the file from the primary replica, since it would be the most recent
		err := RequestFile(hdfsfilename)
		if err != nil {
			// This node was unable to get the file for some reason
			return err
		}
		fileBlockMapToUse = tempFileBlockMap
	} else {
		fileBlockMapToUse = fileBlockMap
	}

	// Create the local file
	f, err := os.Create(localfilename)
	if err != nil {
		return fmt.Errorf("unable to create the localfilename")
	}
	defer f.Close()

	// For each fileblock, write it to the localfile
	for _, eachhdfsfileblock := range fileBlockMapToUse[hdfsfilename] {
		_, err = f.Write(eachhdfsfileblock.Content)
		if err != nil {
			return err
		}
	}

	// Done with this file. Delete it so that the next GET would request for it again
	delete(tempFileInfoMap, hdfsfilename)
	delete(tempFileBlockMap, hdfsfilename)

	return nil
}

func AppendToLocalFile(filename string, content []byte, isTemp bool) error {
	fmt.Println("Appending to file: ", filename)

	var fileInfoMapToUse map[string]*FileInfo
	var fileBlockMapToUse map[string][]*FileBlock
	if isTemp {
		fileInfoMapToUse = tempFileInfoMap
		fileBlockMapToUse = tempFileBlockMap
	} else {
		fileInfoMapToUse = fileInfoMap
		fileBlockMapToUse = fileBlockMap
	}

	_, ok := fileInfoMapToUse[filename]

	if !ok {
		return fmt.Errorf("trying to append to a file that does not exist")
	}

	// Used for both caching and in merge
	fileInfoMapToUse[filename].mostRecent = false

	fileBlock := &FileBlock{filename, content, fileInfoMapToUse[filename].NumBlocks}
	fileBlockMapToUse[filename] = append(fileBlockMapToUse[filename], fileBlock)

	appendFileName := ""
	if isTemp {
		appendFileName = STORAGE_LOCATION + filename + "_temp" + "/" + filename + "_block" + strconv.Itoa(fileInfoMapToUse[filename].NumBlocks) + "_temp"

	} else {
		appendFileName = STORAGE_LOCATION + filename + "/" + filename + "_block" + strconv.Itoa(fileInfoMapToUse[filename].NumBlocks)
	}

	f, err := os.Create(appendFileName)
	if err != nil {
		return fmt.Errorf("unable to create the block of file")
	}
	defer f.Close()

	_, err = f.Write(content)
	if err != nil {
		return err
	}

	fileInfoMapToUse[filename].NumBlocks += 1

	// If you are the primary replica for this file, make the successors process the appends too
	if GetPrimaryReplicaForFile(filename) == NODE_ID {
		replicateFileBlock := FileBlock{filename, content, fileInfoMapToUse[filename].NumBlocks}
		encodedFileBlock, err := json.Marshal(replicateFileBlock)
		if err != nil {
			return err
		}
		appendMessage := Message{Kind: APPEND, Data: string(encodedFileBlock)}
		err = PerformReplication(appendMessage, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// Gets files for which this node is the primary replica.
func GetPrimaryFiles() []*FileInfo {
	var primaryFiles []*FileInfo
	for _, file := range fileInfoMap {
		if file.isPrimary {
			primaryFiles = append(primaryFiles, file)
		}
	}
	return primaryFiles
}

// This is used for replicating - file creation and first block append
func PerformReplication(message Message, waitBoth bool) error {
	successors := GetRingSuccessors(RING_POSITION)

	// One of the two successors could additionally be down. This might happen when the second failure
	// hasn't been reflected in the membership list yet.
	// If this happens, we just let replicate fail on one of the nodes.
	// Eventually, the membership list will be updated, and the updated successor will get the replicas.

	ch := make(chan error, 2)
	for _, succ := range successors {
		if message.Kind == CREATE {
			var fileInfo FileInfo

			err := json.Unmarshal([]byte(message.Data), &fileInfo)
			if err != nil {
				return err
			}

			checkMessage := Message{Kind: CHECK, Data: fileInfo.Name}

			// Send a CHECK message and get the response CHECK message
			responseMessage, err := SendMessageGetReply(succ, checkMessage)
			if err == nil {
				var responseFileInfo FileInfo
				err := json.Unmarshal([]byte(responseMessage.Data), &responseFileInfo)
				// If the file does not exist, response FileInfo has an empty filename
				if err == nil && responseFileInfo.Name == "" {
					go SendAnyReplicationMessage(succ, message, ch)
				} else if responseFileInfo.Name != "" {
					ch <- fmt.Errorf("trying to create a file that already exists on HyDFS")
				}
			}
		} else {
			go SendAnyReplicationMessage(succ, message, ch)
		}
	}

	// Ensure one more replication.
	err := <-ch
	if err != nil {
		return err
	}

	// In some cases, you would want to wait for both replicas to respond
	if waitBoth {
		err := <-ch
		if err != nil {
			return err
		}
	}

	return nil
}

// This is used for both, replicate-on-create and replicate-on-fail
func ReplicateFiles(files []*FileInfo) error {
	fmt.Println("Replicating files, Count: ", len(files))
	LogMessage(fmt.Sprintf("Replicating files, Count: %d", len(files)))

	if len(files) == 0 {
		return nil
	}

	successors := GetRingSuccessors(RING_POSITION)
	LogMessage(fmt.Sprintf("Successors are: [%s]", successors))

	filenames := make([]string, 0, len(files))
	for _, file := range files {
		filenames = append(filenames, file.Name)
	}

	for _, succ := range successors {
		// Get all files from succ
		succ_files, err := GetFileNamesFromNode(succ)
		if err != nil {
			return err
		}

		fmt.Printf("Succ (%s, %d) returned files: [%s]\n", succ, GetRingPosition(succ), succ_files)
		LogMessage(fmt.Sprintf("Succ (%s, %d) returned files: [%s]\n", succ, GetRingPosition(succ), succ_files))

		filesToReplicate := RemoveCommonElements(filenames, succ_files)

		fmt.Printf("Files to replicate %d [%s]\n", len(filesToReplicate), filesToReplicate)
		LogMessage(fmt.Sprintf("Files to replicate %d [%s]\n", len(filesToReplicate), filesToReplicate))

		// One of the two successors could additionally be down. This might happen when the second failure
		// hasn't been reflected in the membership list yet.
		// If this happens, we just let replicate fail on one of the nodes.
		// Eventually, the membership list will be updated, and the updated successor will get the replicas.

		for _, fileToReplicate := range filesToReplicate {
			fmt.Println("CREATE replicate for ", fileToReplicate)
			LogMessage(fmt.Sprintf("CREATE replicate for %s at [%s, %d]", fileToReplicate, succ, GetRingPosition(succ)))

			encodedFileInfo, err := json.Marshal(FileInfo{fileToReplicate, 0, false, false})
			if err != nil {
				return err
			}
			createMessage := Message{Kind: CREATE, Data: string(encodedFileInfo)}

			ch := make(chan error)
			// TODO Are we even using the channel functionality anymore.
			// I could keep ch common across all loop iterations.
			// and just check that I received fileToReplicate responses (+ one of them could be an error)
			go SendAnyReplicationMessage(succ, createMessage, ch)

			err = <-ch
			if err != nil {
				return err
			}

			for _, fileBlockToReplicate := range fileBlockMap[fileToReplicate] {
				fmt.Println("APPEND replicate for ", fileToReplicate, fileBlockToReplicate.blockId)
				LogMessage(fmt.Sprintf("APPEND replicate for [%s,%d] at [%s, %d]", fileToReplicate,
					fileBlockToReplicate.blockId, succ, GetRingPosition(succ)))

				replicateFileBlock := FileBlock{fileToReplicate, fileBlockToReplicate.Content, 0}
				encodedFileBlock, err := json.Marshal(replicateFileBlock)
				if err != nil {
					return err
				}

				appendMessage := Message{Kind: APPEND, Data: string(encodedFileBlock)}

				// TODO What if this node fails right after sending this? The next node should become the leader.
				go SendAnyReplicationMessage(succ, appendMessage, ch)

				// Send every append one-by-one, so that ordering of writes is maintained.
				<-ch
			}
		}
	}

	// Ensure two replications.
	return nil
}

func GetFileNamesFromNode(node string) ([]string, error) {
	filesMessage := Message{Kind: FILES, Data: ""}

	// Send a CHECK message and get the response CHECK message
	responseMessage, err := SendMessageGetReply(node, filesMessage)

	if err == nil {
		var responseFiles []string
		err := json.Unmarshal([]byte(responseMessage.Data), &responseFiles)
		if err != nil {
			return []string{}, err
		}
		return responseFiles, err
	}

	return []string{}, nil
}

func GetFilesNamesOnNode() []string {
	filenames := make([]string, 0, len(fileInfoMap))

	for filename, _ := range fileInfoMap {
		filenames = append(filenames, filename)
	}

	fmt.Println("Files on Node: ", len(filenames))
	return filenames
}

func ReplicateFilesWrapper(files []*FileInfo) {
	err := ReplicateFiles(files)

	if err != nil {
		fmt.Printf("ReplicateFilesWrapper: [%s]\n", err.Error())
	}
}
