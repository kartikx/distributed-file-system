package main

import "fmt"

var files []FileInfo

type FileInfo struct {
	Name      string
	Content   []byte
	IsPrimary bool
}

func PrintStoredFiles() {
	for i, f := range files {
		var ch string
		if f.IsPrimary {
			ch = "*"
		}
		fmt.Printf("%d %s %s\n", i+1, f.Name, ch)
	}
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

// Creates file on local disk, sends 2 replication requests.
func CreateLocalFile(filename string, content []byte) error {
	fmt.Printf("Creating file with name: %s and content: [%s] \n", filename, string(content))
	fileInfo := FileInfo{filename, content, true}
	files = append(files, fileInfo)

	// Replicate on 2 successors, we wait on response from one.
	ch := make(chan error, 2)
	successors := GetRingSuccessors(RING_POSITION)
	for _, succ := range successors {
		fmt.Println("Send replication request to: ", succ)

		go SendReplicationMessage(succ, fileInfo, ch)
	}

	err := <-ch
	if err == nil {
		return nil
	}

	// If there is an error from the previous connection, try again.
	err = <-ch
	return err
}

func ReplicateFile(filename string, content []byte) {
	fmt.Printf("Replicating file with name: %s and content: [%s] \n", filename, string(content))

	// TODO Add to fileinfos however way you want.
	files = append(files, FileInfo{filename, content, false})
}
