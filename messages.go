package main

import (
	"encoding/json"
	"net"
)

const (
	// Top level message types.
	PING MessageType = 0
	ACK  MessageType = 1
	JOIN MessageType = 2
	// Piggybacked message types.
	LEAVE        MessageType = 3
	FAIL         MessageType = 4
	HELLO        MessageType = 5
	SUSPECT      MessageType = 6
	ALIVE        MessageType = 7
	SUSPECT_MODE MessageType = 8
	DROPOUT      MessageType = 9
)

type MemberInfo struct {
	connection *net.Conn
	host       string
	// TODO are we ever using this failed bool?
	failed      bool
	suspected   bool
	incarnation int
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
