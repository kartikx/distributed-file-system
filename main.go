package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
)

var NODE_ID = ""
var LOCAL_IP = ""
var RING_POSITION = -1

var isIntroducer = false

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

	if LOCAL_IP == INTRODUCER_SERVER_HOST {
		isIntroducer = true
	}

	// TODO @kartikramesh Move this into a separate function
	// TODO Add functionality to clean-up files from prior runs before you enter the system.
	// TODO Add functionality to fetch files to replicate.
	if !isIntroducer {
		members, introducer_conn, err := IntroduceYourself()
		if err != nil {
			log.Fatalf("Unable to join the group: %s", err.Error())
		}

		NODE_ID = InitializeMembershipInfoAndList(members, introducer_conn, LOCAL_IP)

		helloMessage := Message{
			Kind: HELLO,
			Data: NODE_ID,
		}

		AddPiggybackMessage(helloMessage)
	} else {
		NODE_ID = ConstructNodeID(INTRODUCER_SERVER_HOST)
		RING_POSITION = GetRingPosition(NODE_ID)
	}

	clientServerChan <- 1

	// Dial connection.
	go startClient(clientServerChan)

	fmt.Println("Joined the group as: ", NODE_ID)

	os_signals := make(chan os.Signal, 1)
	signal.Notify(os_signals, os.Interrupt)
	go func() {
		for sig := range os_signals {
			// sig is a ^C, handle it
			fmt.Println("Application got an OS interrupt:", sig, "at", time.Now().Format(time.RFC3339))
			os.Exit(0)
		}
	}()

	// TODO make this more elaborate and in-line with demo expectations.
	// Would be nice to move this into a separate file.
	for {
		var demoInstruction string
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			demoInstruction = scanner.Text()
		}

		demoArgs := strings.Split(demoInstruction, " ")

		switch {
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
		// TODO @kartikr2 Create an APPEND function.
		case demoArgs[0] == "print_succ":
			ringSuccessors := GetRingSuccessors(RING_POSITION)
			fmt.Print(ringSuccessors)
		case demoArgs[0] == "print_ring":
			PrintRing()
		case demoArgs[0] == "store":
			PrintStoredFiles()
		case demoArgs[0] == "append":
			AppendToHDFSFile(demoArgs[1], demoArgs[2])
		}
	}
}
