package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

var NODE_ID = ""
var LOCAL_IP = ""
var RING_POSITION = -1

var isLeader = false

func main() {
	// Synchronizes start of client and server.
	clientServerChan := make(chan int, 5)

	// Listener is started even before introduction so that the
	// introducer can make a connection to this node.
	go startServer(clientServerChan)

	LOCAL_IP, err := GetLocalIP()
	if err != nil {
		log.Fatalf("Unable to get local IP")
	}

	if LOCAL_IP == LEADER_SERVER_HOST {
		isLeader = true
	}

	JoinRing()

	clientServerChan <- 1

	// Dial connection.
	go startClient(clientServerChan)

	for {
		var demoInstruction string
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			demoInstruction = scanner.Text()
		}

		demoArgs := strings.Split(demoInstruction, " ")

		switch {
		case demoArgs[0] == "RainStorm":
			if len(demoArgs) < 6 {
				log.Println("Expected usage: \"Rainstorm <op1 _exe> <op2 _exe> " +
					"<hydfs_src_file> <hydfs_dest_filename> <num_tasks>\"")
				continue
			}

		case demoArgs[0] == "list_mem":
			PrintMembershipInfo()
		case demoArgs[0] == "list_self":
			fmt.Printf("ID: %s POINT: %d \n", NODE_ID, RING_POSITION)
		case demoArgs[0] == "piggybacks":
			PrintPiggybackMessages()
		case demoArgs[0] == "leave":
			ExitGroup()
		case demoArgs[0] == "meta_info":
			fmt.Printf("ID: %s\n", NODE_ID)
		case demoArgs[0] == "create":
			err = CreateHDFSFile(demoArgs[1], demoArgs[2])
			if err == nil {
				fmt.Println("Creation completed")
			} else {
				fmt.Println("Error while creating file: ", err.Error())
			}
		case demoArgs[0] == "print_succ":
			ringSuccessors := GetRingSuccessors(RING_POSITION)
			fmt.Print(ringSuccessors)
		case demoArgs[0] == "list_ring":
			PrintRing()
		case demoArgs[0] == "store":
			PrintStoredFiles()
		case demoArgs[0] == "append":
			AppendToHDFSFile(demoArgs[1], demoArgs[2])
		case demoArgs[0] == "get":
			GetHDFSToLocal(demoArgs[1], demoArgs[2], "")
		case demoArgs[0] == "get_from_replica":
			GetHDFSToLocal(demoArgs[2], demoArgs[3], demoArgs[1])
		case demoArgs[0] == "list_files":
			filenames := GetFilesNamesOnNode()
			fmt.Print(filenames)
		case demoArgs[0] == "merge":
			MergeHDFSFile(demoArgs[1])
		case demoArgs[0] == "ls":
			PrintMachinesWithFile(demoArgs[1])
		}
	}
}
