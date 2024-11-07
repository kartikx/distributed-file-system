// Stores functionality for responding to messages.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
)

func startServer(clientServerChan chan int) {
	addr := &net.UDPAddr{
		IP:   net.ParseIP(SERVER_HOST),
		Port: SERVER_PORT,
		Zone: "",
	}

	server, err := net.ListenUDP("udp", addr)

	if err != nil {
		log.Fatalf("Couldn't start server: %s", err.Error())
	}

	clientServerChan <- 1

	for {
		buf := make([]byte, 8192)
		mlen, address, err := server.ReadFromUDP(buf)

		if err != nil {
			log.Fatalf("Error accepting: %s", err.Error())
		}

		var message Message
		json.Unmarshal(buf[:mlen], &message)

		var messagesToPiggyback Messages

		switch message.Kind {
		case PING:
			messagesToPiggyback = GetUnexpiredPiggybackMessages()
			PrintMessage("Incoming", message, address.IP.String())
			var messages Messages
			err = json.Unmarshal([]byte(message.Data), &messages)

			if err != nil {
				LogError("Failed to unmarshal PING messages, skipping")
				continue
			}

			// Each PING contains multiple messages within it.
			for _, subMessage := range messages {
				switch subMessage.Kind {
				case HELLO:
					ProcessHelloMessage(subMessage)
				case LEAVE:
					ProcessFailOrLeaveMessage(subMessage)
				case FAIL:
					ProcessFailOrLeaveMessage(subMessage)
				default:
					log.Fatalf("Unexpected submessage kind in PING")
				}
			}
		case JOIN:
			PrintMessage("Incoming", message, "")
			responseMessage, err := ProcessJoinMessage(message, address)
			if err != nil {
				log.Fatalln("Failed to process join message", message)
			}
			// Piggyback the JOIN message
			messagesToPiggyback = Messages{responseMessage}
		case LEAVE:
			messagesToPiggyback = GetUnexpiredPiggybackMessages()
			ProcessFailOrLeaveMessage(message)
		case REPLICATE:
			// Replicate does not piggyback anything.

			// TODO If there is an error we can indicate this to the client appropriately.
			err = ProcessReplicateMessage(message)
		case CREATE:
			err = ProcessCreateMessage(message)
		case APPEND:
			err = ProcessAppendMessage(message)
		case CHECK:
			err = ProcessCheckMessage(message, server, address)
			continue
		default:
			log.Fatalln("Unexpected message kind: ", message)
		}

		ackResponse, err := EncodeAckMessage(messagesToPiggyback)
		if err != nil {
			LogError("Failed to generate response.")
			continue
		}

		var ackMessage Message
		err = json.Unmarshal(ackResponse, &ackMessage)
		if err != nil {
			LogError("Unable to decode outgoing ACK message")
			continue
		}
		PrintMessage("outgoing", ackMessage, "")

		server.WriteToUDP(ackResponse, address)
	}
}

// request contains the encoded Data of the JOIN message.
// addr is the address of the host that sent this PING.
func ProcessJoinMessage(message Message, addr *net.UDPAddr) (Message, error) {
	if isIntroducer {
		joinResponse, err := IntroduceNodeToGroup(message.Data, addr)
		return joinResponse, err
	} else {
		return Message{}, fmt.Errorf("Unexpected JOIN message received for non Introducer node")
	}
}

func ProcessHelloMessage(message Message) error {
	PrintMessage("incoming", message, "")

	nodeId := message.Data

	_, ok := GetMemberInfo(nodeId)

	if ok {
		LogMessage(fmt.Sprintf("Node %s already exists in membership info, Skipping HELLO \n", nodeId))
		return nil
	}

	if nodeId == NODE_ID {
		LogMessage(fmt.Sprintf("Received self hello message for ID: %s Skip \n", nodeId))
		return nil
	}

	err := AddNewMemberToMembershipInfo(nodeId)
	if err != nil {
		return err
	}

	AddPiggybackMessage(message)

	return nil
}

func ProcessFailOrLeaveMessage(message Message) error {
	PrintMessage("incoming", message, "")

	// For the fail message, Data is expected to be the node Id.
	nodeId := message.Data

	// If it's you, be very confused.
	if nodeId == NODE_ID {
		fmt.Println("Received self failure message.")
		os.Exit(0)
	}

	// TODO @kartikr2 Remove.
	fmt.Printf("RECEIVED NODE %s as FAILED\n", nodeId)

	_, ok := GetMemberInfo(nodeId)

	if ok { // node exists in membership info, remove and disseminate
		DeleteMemberAndReReplicate(nodeId)

		// disseminating info that the node left
		AddPiggybackMessage(message)

		return nil
	}

	return nil
}

func ProcessReplicateMessage(message Message) error {
	PrintMessage("incoming", message, "")

	encodedFiles := message.Data

	var files []FileInfo
	err := json.Unmarshal([]byte(encodedFiles), &files)

	if err != nil {
		return err
	}

	for _, file := range files {
		// This file was received over the network, ensure isPrimary is false for safety.
		file.isPrimary = false

		// StoreFileLocally(&file)
	}

	return nil
}

func ProcessCreateMessage(message Message) error {
	PrintMessage("incoming", message, "")

	encodedFileInfo := message.Data

	var fileInfo FileInfo

	err := json.Unmarshal([]byte(encodedFileInfo), &fileInfo)
	if err != nil {
		return err
	}

	CreateLocalFile(fileInfo.Name)
	return nil
}

func ProcessAppendMessage(message Message) error {
	PrintMessage("incoming", message, "")

	encodedFileBlock := message.Data

	var fileBlock FileBlock

	err := json.Unmarshal([]byte(encodedFileBlock), &fileBlock)
	if err != nil {
		return err
	}

	AppendToLocalFile(fileBlock.Name, fileBlock.Content)
	return nil
}

func ProcessCheckMessage(message Message, server *net.UDPConn, address *net.UDPAddr) error {
	PrintMessage("incoming", message, "")

	filenameToCheck := message.Data

	var checkFileInfo FileInfo

	_, ok := fileInfoMap[filenameToCheck]
	if ok {
		checkFileInfo = *fileInfoMap[filenameToCheck]
	}

	encodedFileInfo, err := json.Marshal(checkFileInfo)
	if err != nil {
		return err
	}
	checkResponse := Message{Kind: CHECK, Data: string(encodedFileInfo)}
	encodedcheckResponse, err := json.Marshal(checkResponse)
	if err != nil {
		return err
	}

	server.WriteToUDP(encodedcheckResponse, address)

	return nil
}
