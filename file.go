package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

var fileInfoMap = map[string]*FileInfo{}
var fileBlockMap = map[string][]*FileBlock{}

type FileInfo struct {
	Name      string
	NumBlocks int
	// Shouldn't be exported, because different nodes will have different isPrimary values for the same file.
	isPrimary bool
}

type FileBlock struct {
	Name    string
	Content []byte
	// Shouldn't be exported, because different nodes will have different block IDs for the same file.
	blockId int
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

	fileInfo := FileInfo{hdfsfilename, 0, false}
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
		create_err := ProcessCreateMessage(createMessage)
		fmt.Printf("Created Local file\n")
		if create_err == nil {
			append_err := ProcessAppendMessage(appendMessage)
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

// Creates file on local disk and triggers replication.
func CreateLocalFile(filename string) error {
	fmt.Printf("Creating file with name: %s \n", filename)

	isPrimaryReplica := false
	if GetPrimaryReplicaForFile(filename) == NODE_ID {
		isPrimaryReplica = true
	}

	fileInfo := &FileInfo{filename, 0, isPrimaryReplica}

	_, ok := fileInfoMap[fileInfo.Name]

	if !ok {
		fileInfoMap[fileInfo.Name] = fileInfo
	} else {
		// TODO @kartikr2 Throw error if file already exists. Should be propagated across the network.
		return fmt.Errorf("file already exists on the HDFS")
	}

	dirName := STORAGE_LOCATION + filename
	err := os.MkdirAll(dirName, 0777)
	if err != nil {
		return err
	}

	// If you are the primary replica for this file, make the successors create the file too
	if GetPrimaryReplicaForFile(filename) == NODE_ID {
		replicateFileInfo := FileInfo{filename, 0, isPrimaryReplica}
		encodedFileInfo, err := json.Marshal(replicateFileInfo)
		if err != nil {
			return err
		}
		createMessage := Message{Kind: CREATE, Data: string(encodedFileInfo)}
		err = PerformReplication(createMessage)
		if err != nil {
			return err
		}
	}

	return err
}

func AppendToLocalFile(filename string, content []byte) error {
	fmt.Println("Appending to file: ", filename)

	_, ok := fileInfoMap[filename]

	if !ok {
		return fmt.Errorf("trying to append to a file that does not exist")
	}

	fileBlock := &FileBlock{filename, content, fileInfoMap[filename].NumBlocks}
	fileBlockMap[filename] = append(fileBlockMap[filename], fileBlock)

	appendFileName := STORAGE_LOCATION + filename + "/" + filename + "_block" + strconv.Itoa(fileInfoMap[filename].NumBlocks)

	f, err := os.Create(appendFileName)
	if err != nil {
		return fmt.Errorf("unable to create the block of file")
	}
	defer f.Close()

	_, err = f.Write(content)
	if err != nil {
		return err
	}

	fileInfoMap[filename].NumBlocks += 1

	// If you are the primary replica for this file, make the successors process the appends too
	if GetPrimaryReplicaForFile(filename) == NODE_ID {
		replicateFileBlock := FileBlock{filename, content, fileInfoMap[filename].NumBlocks}
		encodedFileBlock, err := json.Marshal(replicateFileBlock)
		if err != nil {
			return err
		}
		appendMessage := Message{Kind: APPEND, Data: string(encodedFileBlock)}
		err = PerformReplication(appendMessage)
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
func PerformReplication(message Message) error {
	successors := GetRingSuccessors(RING_POSITION)

	// One of the two successors could additionally be down. This might happen when the second failure
	// hasn't been reflected in the membership list yet.
	// If this happens, we just let replicate fail on one of the nodes.
	// Eventually, the membership list will be updated, and the updated successor will get the replicas.

	ch := make(chan error, 2)
	for _, succ := range successors {
		go SendAnyReplicationMessage(succ, message, ch)
	}

	// Ensure one more replication.
	err := <-ch
	if err != nil {
		return err
	}

	return nil
}

// This is used for both, replicate-on-create and replicate-on-fail
func ReplicateFiles(files []*FileInfo) error {
	fmt.Println("Replicating files, Count: ", len(files))

	successors := GetRingSuccessors(RING_POSITION)

	// TODO Ask successor for what files and blocks it has.

	// TODO Diff files

	// One of the two successors could additionally be down. This might happen when the second failure
	// hasn't been reflected in the membership list yet.
	// If this happens, we just let replicate fail on one of the nodes.
	// Eventually, the membership list will be updated, and the updated successor will get the replicas.

	for _, fileToReplicate := range files {
		fmt.Println("CREATE replicate for ", fileToReplicate.Name)
		encodedFileInfo, err := json.Marshal(fileToReplicate)
		if err != nil {
			return err
		}
		createMessage := Message{Kind: CREATE, Data: string(encodedFileInfo)}

		ch := make(chan error, 2)
		for _, succ := range successors {
			go SendAnyReplicationMessage(succ, createMessage, ch)
		}

		// TODO @kartikr2 Retest with concurrent failures.
		// Print the return value in the caller of this function.
		err = <-ch
		if err != nil {
			return err
		}

		err = <-ch
		if err != nil {
			return err
		}

		// TODO @kartikr2 Can UDP mess up packet ordering?

		for _, fileBlockToReplicate := range fileBlockMap[fileToReplicate.Name] {
			fmt.Println("APPEND replicate for ", fileToReplicate.Name, fileBlockToReplicate.blockId)

			// TODO @sdevata2 Is passing 0 correct?
			replicateFileBlock := FileBlock{fileToReplicate.Name, fileBlockToReplicate.Content, 0}
			encodedFileBlock, err := json.Marshal(replicateFileBlock)
			if err != nil {
				return err
			}

			appendMessage := Message{Kind: APPEND, Data: string(encodedFileBlock)}

			for _, succ := range successors {
				go SendAnyReplicationMessage(succ, appendMessage, ch)
			}

			_, _ = <-ch, <-ch
		}
	}

	// Ensure two replications.

	return nil
}
