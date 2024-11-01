// Stores functionality for initiating messages.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
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
			go PingMember(nodeId)
			time.Sleep(PING_INTERVAL_MILLISECONDS * time.Millisecond)
		}
	}
}

func PingMember(nodeId string) {
	connection, err := net.Dial("udp", GetServerEndpoint(membershipInfo[nodeId].Host))
	if err != nil {
		LogError(fmt.Sprintf("Node %s connection is nil, it might have been removed from the group\n",
			nodeId))
		DeleteMember(nodeId)
	}
	defer connection.Close()

	member, membererr := GetMemberInfo(nodeId)
	if member.failed || !membererr {
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

	// TODO would this work would even if I were to re-use the connection?
	connection.SetReadDeadline(time.Now().Add(TIMEOUT_DETECTION_MILLISECONDS * time.Millisecond))
	mLen, err := connection.Read(buffer)

	if err != nil {
		// Just mark the node as failed
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

	for _, subMessage := range messages {
		switch subMessage.Kind {
		case HELLO:
			ProcessHelloMessage(subMessage)
		case LEAVE:
			ProcessFailOrLeaveMessage(subMessage)
		case FAIL:
			ProcessFailOrLeaveMessage(subMessage)
		default:
			log.Fatalf("Unexpected submessage kind in ACK")
		}
	}

}

func SendReplicationMessage(nodeId string, fileInfo FileInfo) error {
	message := Message{Kind: REPLICATE, Data: fileInfo.Name}

	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return err
	}

	connection, err := net.Dial("udp", GetServerEndpoint(membershipInfo[nodeId].Host))
	if err != nil {
		return err
	}
	defer connection.Close()

	member, membererr := GetMemberInfo(nodeId)
	if member.failed || !membererr {
		return errors.New("unable to get member info")
	}

	if connection == nil {
		return errors.New("node connection is nil")
	}

	connection.Write(encodedMessage)
	buffer := make([]byte, 8192)

	_, err = connection.Read(buffer)
	if err != nil {
		return err
	}

	return nil
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
		connection, _ := net.Dial("udp", GetServerEndpoint(membershipInfo[nodeId].Host))

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

	os.Exit(0)
}
