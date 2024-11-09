package main

import (
	"encoding/json"
)

const (
	// Top level message types.
	// TODO @kartikr2 Get rid of REPLICATE.
	PING        MessageType = 0
	ACK         MessageType = 1
	JOIN        MessageType = 2
	CREATE      MessageType = 3
	APPEND      MessageType = 4
	CHECK       MessageType = 5
	REPLICATE   MessageType = 6
	FILES       MessageType = 7
	GETFILE     MessageType = 8
	TEMP_CREATE MessageType = 9
	TEMP_APPEND MessageType = 10
	DELETE      MessageType = 11
	MERGE       MessageType = 12

	// Piggybacked message types.
	LEAVE MessageType = 16
	FAIL  MessageType = 17
	HELLO MessageType = 18
)

type MemberInfo struct {
	// TODO @kartikr2 Remove if unused.
	failed bool

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

type PiggybackMessage struct {
	message Message
	ttl     int
}

type PiggybackMessages []PiggybackMessage

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
