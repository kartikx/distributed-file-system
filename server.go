// Stores functionality for responding to messages.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func startServer(clientServerChan chan int) {

	server, err := net.Listen("tcp", fmt.Sprintf(":%d", SERVER_PORT))

	if err != nil {
		log.Fatalf("Couldn't start server: %s", err.Error())
	}

	clientServerChan <- 1

	for {
		conn, err := server.Accept()

		if err != nil {
			log.Fatalf("Error accepting: %s", err.Error())
		}
		// TODO @kartikr2 Should I process each packet in a GoRoutine? Could it cause any issues with sync?
		// Can't defer here because inf loop.
		// defer conn.Close()

		// TODO @kartikr2 Larger buffers to handle larger files.
		buf := make([]byte, 8192)
		mlen, err := conn.Read(buf)

		var message Message
		json.Unmarshal(buf[:mlen], &message)

		var messagesToPiggyback Messages

		remoteAddress := strings.Split(conn.RemoteAddr().String(), ":")[0]

		switch message.Kind {
		case PING:
			messagesToPiggyback = GetUnexpiredPiggybackMessages()
			PrintMessage("Incoming", message, remoteAddress)
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
			responseMessage, err := ProcessJoinMessage(message, remoteAddress)
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
			err = ProcessCheckMessage(message, conn)
			if err != nil {
				fmt.Println(err.Error())
			}
			conn.Close()
			continue
		case FILES:
			err = ProcessFilesMessage(message, conn)
			if err != nil {
				fmt.Println(err.Error())
			}
			conn.Close()
			continue
		default:
			log.Fatalln("Unexpected message kind: ", message)
		}

		if err != nil {
			fmt.Println(err.Error())
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

		conn.Write(ackResponse)
		conn.Close()
	}
}

// request contains the encoded Data of the JOIN message.
// addr is the address of the host that sent this PING.
func ProcessJoinMessage(message Message, addr string) (Message, error) {
	if isIntroducer {
		joinResponse, err := IntroduceNodeToGroup(message.Data, addr)
		return joinResponse, err
	} else {
		return Message{}, fmt.Errorf("unexpected JOIN message received for non Introducer node")
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

	return CreateLocalFile(fileInfo.Name)
}

func ProcessAppendMessage(message Message) error {
	PrintMessage("incoming", message, "")

	encodedFileBlock := message.Data

	var fileBlock FileBlock

	err := json.Unmarshal([]byte(encodedFileBlock), &fileBlock)
	if err != nil {
		return err
	}

	return AppendToLocalFile(fileBlock.Name, fileBlock.Content)
}

func ProcessCheckMessage(message Message, conn net.Conn) error {
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
	encodedCheckResponse, err := json.Marshal(checkResponse)
	if err != nil {
		return err
	}

	conn.Write(encodedCheckResponse)
	conn.Close()

	return nil
}

func ProcessFilesMessage(message Message, conn net.Conn) error {
	PrintMessage("incoming", message, "")

	filenames := GetFilesNamesOnNode()

	encodedFilenames, err := json.Marshal(filenames)
	if err != nil {
		return err
	}

	filesResponse := Message{Kind: FILES, Data: string(encodedFilenames)}
	encodedFilesResponse, err := json.Marshal(filesResponse)
	if err != nil {
		return err
	}

	conn.Write(encodedFilesResponse)
	conn.Close()

	return nil
}
