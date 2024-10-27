// Stores functionality for initiating messages.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func startClient(clientServerChan chan int) {
	// Ensures that sending starts after listener has started and introduction is complete.
	_, _ = <-clientServerChan, <-clientServerChan

	for {
		members := GetMembers()
		var member_ids []string
		for k := range members {
			member_ids = append(member_ids, k)
		}
		Shuffle(member_ids)

		for _, nodeId := range member_ids {
			go handleEachMember(nodeId)
			time.Sleep(PING_INTERVAL_MILLISECONDS * time.Millisecond)
		}
	}
}

func handleEachMember(nodeId string) {

	connection := GetNodeConnection(nodeId)

	member, membererr := GetMemberInfo(nodeId)
	if member.failed || !membererr {
		return
	}

	if connection == nil {
		LogError(fmt.Sprintf("Node %s connection is nil, it might have been removed from the group\n",
			nodeId))
		DeleteMember(nodeId)
		return
	}

	var messagesToPiggyback Messages = GetUnexpiredPiggybackMessages()

	pingMessageEnc, err := EncodePingMessage(messagesToPiggyback)
	if err != nil {
		LogError("Unable to encode ping message")
		return
	}

	var pingMessage Message
	err = json.Unmarshal(pingMessageEnc, &pingMessage)
	if err != nil {
		LogError("Unable to decode outgoing PING message")
		return
	}
	PrintMessage("outgoing", pingMessage, nodeId)

	connection.Write(pingMessageEnc)

	buffer := make([]byte, 8192)

	randomFloat := rand.Float64()

	// TODO would this work would even if I were to re-use the connection?
	connection.SetReadDeadline(time.Now().Add(TIMEOUT_DETECTION_MILLISECONDS * time.Millisecond))
	mLen, err := connection.Read(buffer)

	if err != nil || randomFloat < dropRate {
		// In suspicion, you would want to suspect it first.
		if inSuspectMode {
			LogMessage(fmt.Sprintf("SUSPECT NODE %s", nodeId))
			// Create a SUSPECT message to process and disseminate
			member, _ := GetMemberInfo(nodeId)
			suspectMessage := Message{Kind: SUSPECT, Data: strconv.Itoa(member.incarnation) + "@" + nodeId}
			// PrintMessage("outgoing", suspectMessage, suspectMessage.Data)
			go ProcessSuspectMessage(suspectMessage)

			return
		} else { // Otherwise, just mark the node as failed
			LogError(fmt.Sprintf("Error in reading from connection for nodeId [%s] Err: [%s]\n", nodeId, err))

			LogMessage(fmt.Sprintf("DETECTED NODE %s as FAILED", nodeId))
			DeleteMember(nodeId)

			// Start propagating FAIL message.
			failedMessage := Message{
				Kind: FAIL,
				Data: nodeId,
			}

			AddPiggybackMessage(failedMessage)

			return
		}
	}

	messages, err := DecodeAckMessage(buffer[:mLen])
	if err != nil {
		LogError(fmt.Sprintf("Unable to decode ACK message from node: %s", nodeId))
		return
	}

	var ackMessage Message
	err = json.Unmarshal(buffer[:mLen], &ackMessage)
	if err != nil {
		return
	}
	PrintMessage("incoming", ackMessage, nodeId)

	if inSuspectMode {
		// if you are suspecting the node, mark it as alive since you got an ACK
		member, _ := GetMemberInfo(nodeId)
		if member.suspected {
			aliveMessage := Message{Kind: ALIVE, Data: strconv.Itoa(member.incarnation) + "@" + nodeId}
			ProcessAliveMessage(aliveMessage)
			AddPiggybackMessage(aliveMessage)
		}
	}

	for _, subMessage := range messages {
		switch subMessage.Kind {
		case HELLO:
			ProcessHelloMessage(subMessage)
		case LEAVE:
			ProcessFailOrLeaveMessage(subMessage)
		case FAIL:
			ProcessFailOrLeaveMessage(subMessage)
		case SUSPECT:
			go ProcessSuspectMessage(subMessage)
		case ALIVE:
			ProcessAliveMessage(subMessage)
		case SUSPECT_MODE:
			ProcessSuspectModeMessage(subMessage)
		case DROPOUT:
			ProcessDropoutMessage(subMessage)
		default:
			log.Fatalf("Unexpected submessage kind in ACK")
		}
	}

}

func ExitGroup() {
	fmt.Printf("Exiting gracefully %s\n", NODE_ID)

	// Leave message just contains the NODE_ID
	leaveMessageEnc, err := GetEncodedLeaveMessage(NODE_ID)
	if err != nil {
		LogError("Unable to encode leave message")
		return
	}

	members := GetMembers()
	for nodeId := range members {
		connection := GetNodeConnection(nodeId)
		if connection != nil {
			var leaveMessage Message
			err = json.Unmarshal(leaveMessageEnc, &leaveMessage)
			if err != nil {
				continue
			}
			PrintMessage("outgoing", leaveMessage, nodeId)

			connection.Write(leaveMessageEnc)
			connection.Close()
		}
	}

	// TODO close the log file

	os.Exit(0)

}

func StartSuspecting() {
	fmt.Println("Enabling Suspicion")
	suspectMessage := Message{Kind: SUSPECT_MODE, Data: "true"}
	ProcessSuspectModeMessage(suspectMessage)
	AddPiggybackMessage(suspectMessage)
}

func StopSuspecting() {
	fmt.Println("Disabling Suspicion")
	suspectMessage := Message{Kind: SUSPECT_MODE, Data: "false"}
	ProcessSuspectModeMessage(suspectMessage)
	AddPiggybackMessage(suspectMessage)
}

func SetDropout(dropoutCommand string) {
	dropoutMessage := Message{Kind: DROPOUT, Data: dropoutCommand}
	ProcessDropoutMessage(dropoutMessage)
	AddPiggybackMessage(dropoutMessage)
}
