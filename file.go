package main

import "fmt"

var fileInfoMap = map[string]*FileInfo{}

type FileInfo struct {
	Name    string
	Content []byte
	// Shouldn't be exported, because different nodes will have different isPrimary values for the same file.
	isPrimary bool
}

// Checks whether this node has become the primary replica for any file.
// Membership Info should have been updated before invoking this function.
// Returns primary replicas.
func UpdatePrimaryReplicas() []*FileInfo {
	fmt.Println("Updating primary replicas")
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
		fmt.Printf("%s %s\n", name, ch)
	}
	fmt.Println("===")
}

func CreateHDFSFile(filename string, content []byte) error {
	nodeId := GetPrimaryReplicaForFile(filename)

	fmt.Println("File hash: ", GetRingPosition(filename))

	if nodeId == NODE_ID {
		return CreateLocalFile(filename, content)
	} else {
		return SendFileCreationMessage(nodeId, filename, content)
	}
}

// Creates file on local disk and triggers replication.
func CreateLocalFile(filename string, content []byte) error {
	fmt.Printf("Creating file with name: %s and content: [%s] \n", filename, string(content))

	fileInfo := &FileInfo{filename, content, true}

	// TODO @kartikr2 Throw error if file already exists. Should be propagated across the network.
	StoreFileLocally(fileInfo)

	// ? @kartikr2 Should we wait on both successors? We want to ensure there are two replicas.
	err := ReplicateFiles([]*FileInfo{fileInfo})

	return err
}

func StoreFileLocally(fileInfo *FileInfo) error {
	fmt.Printf("Replicating file with name: %s and content: [%s] \n", fileInfo.Name, string(fileInfo.Content))

	_, ok := fileInfoMap[fileInfo.Name]

	if !ok {
		fileInfoMap[fileInfo.Name] = fileInfo

		// TODO Store file and contents on disk.
	}

	// TODO could throw an error if file already exists.
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

// This is used for both, replicate-on-create and replicate-on-fail
func ReplicateFiles(files []*FileInfo) error {
	successors := GetRingSuccessors(RING_POSITION)

	// One of the two successors could additionally be down. This might happen when the second failure
	// hasn't been reflected in the membership list yet.
	// If this happens, we just let replicate fail on one of the nodes.
	// Eventually, the membership list will be updated, and the updated successor will get the replicas.

	ch := make(chan error, 2)
	for _, succ := range successors {
		go SendReplicationMessages(succ, files, ch)
	}

	// Ensure two replications.
	err := <-ch
	if err != nil {
		return err
	}

	err = <-ch
	if err != nil {
		return err
	}

	return nil
}
