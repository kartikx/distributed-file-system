// Stores functionality for initiating messages.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

// This starts the Client to repeatedly send PING messages to all members in the group.
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

// Sends a PING to a given node ID. Handles piggybacking of messages.
func PingMember(nodeId string) {
	connection, err := net.Dial("tcp", GetServerEndpoint(membershipInfo[nodeId].Host))
	if err != nil {
		LogError(fmt.Sprintf("Node %s connection is nil, it might have been removed from the group\n",
			nodeId))
		DeleteMemberAndReReplicate(nodeId)
		return
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

	connection.SetReadDeadline(time.Now().Add(TIMEOUT_DETECTION_MILLISECONDS * time.Millisecond))
	mLen, err := connection.Read(buffer)

	if err != nil {
		// Just mark the node as failed
		LogError(fmt.Sprintf("Error in reading from connection for nodeId [%s] Err: [%s]\n", nodeId, err))

		LogMessage(fmt.Sprintf("DETECTED NODE %s as FAILED", nodeId))

		fmt.Printf("DETECTED NODE %s as FAILED\n", nodeId)

		DeleteMemberAndReReplicate(nodeId)

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

// Sends replication requests for all files to the given node.
func SendAnyReplicationMessage(nodeId string, message Message, ch chan error) {
	PrintMessage("outgoing", message, nodeId)
	encodedMessage, err := json.Marshal(message)
	if err != nil {
		ch <- err
		return
	}

	connection, err := net.Dial("tcp", GetServerEndpoint(membershipInfo[nodeId].Host))
	if err != nil {
		ch <- err
		return
	}
	defer connection.Close()

	connection.Write(encodedMessage)
	buffer := make([]byte, 8192)

	_, err = connection.Read(buffer)
	if err != nil {
		ch <- err
		return
	}

	ch <- nil
}

// Sends replication requests for all files to the given node.
// This is used for both, replicate-on-create and replicate-on-fail
func SendReplicationMessages(nodeId string, files []*FileInfo, ch chan error) {
	fmt.Printf("Sending %d Replication messages to %s\n", len(files), nodeId)

	encodedFiles, err := json.Marshal(files)
	if err != nil {
		ch <- err
		return
	}

	message := Message{Kind: REPLICATE, Data: string(encodedFiles)}

	encodedMessage, err := json.Marshal(message)
	if err != nil {
		ch <- err
		return
	}

	connection, err := net.Dial("tcp", GetServerEndpoint(membershipInfo[nodeId].Host))
	if err != nil {
		ch <- err
		return
	}
	defer connection.Close()

	connection.Write(encodedMessage)
	buffer := make([]byte, 8192)

	_, err = connection.Read(buffer)
	if err != nil {
		ch <- err
		return
	}

	ch <- nil
}

// Sends a message to the given Node.
func SendMessage(nodeId string, message Message) error {
	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return err
	}

	connection, err := net.Dial("tcp", GetServerEndpoint(membershipInfo[nodeId].Host))
	if err != nil {
		return err
	}
	defer connection.Close()

	connection.Write(encodedMessage)
	buffer := make([]byte, 8192)

	_, err = connection.Read(buffer)
	if err != nil {
		return err
	}

	// response := string(buffer[:mLen])
	// fmt.Println("Replicated response: ", response)

	return nil
}

// Sends a message to the given Node, waits for a reply, and returns the response.
func SendMessageGetReply(nodeId string, message Message) (Message, error) {

	// Make an object to store the reply message
	var responseMessage Message

	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return responseMessage, err
	}

	connection, err := net.Dial("tcp", GetServerEndpoint(membershipInfo[nodeId].Host))
	if err != nil {
		return responseMessage, err
	}
	defer connection.Close()

	// Send the query message
	connection.Write(encodedMessage)
	buffer := make([]byte, 8192)

	// Read the buffer to see if you have a response
	mLen, err := connection.Read(buffer)
	if err != nil {
		return responseMessage, err
	}

	// Unmarshal the response message and return that
	err = json.Unmarshal(buffer[:mLen], &responseMessage)

	return responseMessage, err
}

// Leave the group.
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
		connection, _ := net.Dial("tcp", GetServerEndpoint(membershipInfo[nodeId].Host))

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
