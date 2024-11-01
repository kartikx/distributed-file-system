package main

import (
	"encoding/json"
	"net"
)

const (
	// Top level message types.
	PING      MessageType = 0
	ACK       MessageType = 1
	JOIN      MessageType = 2
	CREATE    MessageType = 3
	REPLICATE MessageType = 4

	// Piggybacked message types.
	LEAVE MessageType = 16
	FAIL  MessageType = 17
	HELLO MessageType = 18
)

type MemberInfo struct {
	connection *net.Conn
	failed     bool

	// Exported variables
	Host         string
	RingPosition int
}

type MessageType int32

type Message struct {
	Kind MessageType
	// This might be a JSON encoded string, and should be decoded based on Kind.
	Data string
}

type Messages []Message

type PiggbackMessage struct {
	message Message
	ttl     int
}

type PiggybackMessages []PiggbackMessage

func GetEncodedJoinMessage() ([]byte, error) {
	joinMessage := Message{Kind: JOIN, Data: ""}

	joinMessageEnc, err := json.Marshal(joinMessage)

	return joinMessageEnc, err
}

func GetEncodedLeaveMessage(nodeId string) ([]byte, error) {
	leaveMessage := Message{Kind: LEAVE, Data: nodeId}
	leaveMessageEnc, err := json.Marshal(leaveMessage)
	return leaveMessageEnc, err
}
