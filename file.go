package main

import "fmt"

var files []FileInfo

type FileInfo struct {
	Name         string
	RingPosition int
	// Could potentially store file content in-memory
}

func PrintStoredFiles() {
	for i, f := range files {
		fmt.Printf("%d %s\n", i+1, f.Name)
	}
}

// Returns all files that should be stored on Node at NodeRingPosition.
func FilterFilesForRingPosition(NodeRingPosition int) []string {
	var filteredFiles []string

	for _, f := range files {
		if f.RingPosition <= NodeRingPosition {
			filteredFiles = append(filteredFiles, f.Name)
		}
	}

	return filteredFiles
}

func CreateLocalFile(filename string, ringPosition int, content []byte) {
	fmt.Printf("Creating file with name: %s and content: [%s] \n", filename, string(content))
	fileInfo := FileInfo{filename, ringPosition}
	files = append(files, fileInfo)

	// Replicate.
	successors := GetRingSuccessors(RING_POSITION)
	for _, succ := range successors {
		fmt.Println("Send replication request to: ", succ)

		// Make request (in a goroutine)
		err := SendReplicationMessage(succ, fileInfo)

		if err != nil {
			fmt.Println("Received error: ", err.Error())
			continue
		}

		fmt.Println("Received response from ", succ)
		// Wait for 1 response.

		// Return.
	}
}

func ReplicateFile(filename string, content []byte) {
	fmt.Printf("Replicating file with name: %s and content: [%s] \n", filename, string(content))

	// TODO Add to fileinfos however way you want.
}
