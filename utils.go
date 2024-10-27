package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"
)

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid local IP address found")
}

func GetIPFromID(id string) string {
	parts := strings.Split(id, "@")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func ConstructNodeID(ip string) string {
	// return fmt.Sprintf("%s@%s", ip, time.Now().Format(time.RFC3339))
	return fmt.Sprintf("%s@%s", ip, time.Now().Format(time.TimeOnly))

}

func GetServerEndpoint(host string) string {
	return fmt.Sprintf("%s:%d", host, SERVER_PORT)
}

// Take a list of messages and encapsulates them into a PING message.
// Returns the encoded message.
func EncodePingMessage(messages Messages) ([]byte, error) {
	messagesEnc, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}

	pingMessage := Message{Kind: PING, Data: string(messagesEnc)}

	pingMessageEnc, err := json.Marshal(pingMessage)
	if err != nil {
		return nil, err
	}

	return pingMessageEnc, nil
}

// Take a list of messages and encapsulates them into an ACK message.
// Returns the encoded message.
func EncodeAckMessage(messages Messages) ([]byte, error) {
	messagesEnc, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}

	ackMessage := Message{Kind: ACK, Data: string(messagesEnc)}

	ackMessageEnc, err := json.Marshal(ackMessage)
	if err != nil {
		return nil, err
	}

	return ackMessageEnc, nil
}

// For a given message, returns the sub-messages present inside it.
func DecodeAckMessage(messageEnc []byte) (Messages, error) {
	var message Message

	err := json.Unmarshal(messageEnc, &message)
	if err != nil {
		return nil, err
	}

	if message.Kind != ACK {
		return nil, fmt.Errorf("Message kind is not ACK")
	}

	var messages Messages

	err = json.Unmarshal([]byte(message.Data), &messages)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func Shuffle(slice []string) {
	for i := range slice {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}
