// Implements logging functionality.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

var log_file, _ = os.OpenFile("./machine.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
var log_file_writer = bufio.NewWriter(log_file)

func PrintMessage(direction string, message Message, nodeId string) {
	// fmt.Fprintf(log_file_writer, "---------\nPrinting %s message\n%s\n", direction, time.Now())
	currentTime := time.Now().Format(time.TimeOnly)

	switch message.Kind {
	case PING:
		fmt.Fprintf(log_file_writer, "[%s] [%s] PING message (%s):\n", currentTime, direction, nodeId)

		var messages Messages
		err := json.Unmarshal([]byte(message.Data), &messages)
		if err != nil {
			fmt.Fprintf(log_file_writer, "Failed to unmarshal PING submessages")
			return
		}

		fmt.Fprintf(log_file_writer, "Submessages vvvvv\n")
		for _, subMessage := range messages {
			PrintMessage(direction, subMessage, nodeId)
		}
		fmt.Fprintf(log_file_writer, "Submessages ^^^^^\n")
	case ACK:
		fmt.Fprintf(log_file_writer, "[%s] [%s] ACK message (%s):\n", currentTime, direction, nodeId)

		var messages Messages
		err := json.Unmarshal([]byte(message.Data), &messages)
		if err != nil {
			fmt.Fprintf(log_file_writer, "Failed to unmarshal PING submessages")
			return
		}

		fmt.Fprintf(log_file_writer, "Submessages vvvvv\n")
		for _, subMessage := range messages {
			PrintMessage(direction, subMessage, nodeId)
		}
		fmt.Fprintf(log_file_writer, "Submessages ^^^^^\n")

	case JOIN:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] JOIN message with %s\n", currentTime, direction, nodeId, message.Data)

	case LEAVE:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] LEAVE message with %s\n", currentTime, direction, nodeId, message.Data)

	case FAIL:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] FAIL message with %s\n", currentTime, direction, nodeId, message.Data)

	case HELLO:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] HELLO message with %s\n", currentTime, direction, nodeId, message.Data)

	case SUSPECT:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] SUSPECT message with %s\n", currentTime, direction, nodeId, message.Data)

	case ALIVE:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] ALIVE message with %s\n", currentTime, direction, nodeId, message.Data)

	case SUSPECT_MODE:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] SUSPECT_MODE message with %s\n", currentTime, direction, nodeId, message.Data)

	case DROPOUT:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] DROPOUT message with %s\n", currentTime, direction, nodeId, message.Data)

	default:
		fmt.Fprintf(log_file_writer, "[%s] [%s] ********Trying to print unknown message type**********", currentTime, direction)
	}
	fmt.Fprintf(log_file_writer, "---------\n")
}

func LogMessage(message string) {
	fmt.Fprintf(log_file_writer, "[%s] %s\n", time.Now().Format(time.TimeOnly), message)
}

func LogError(message string) {
	fmt.Fprintf(log_file_writer, "[%s] ERROR: %s\n", time.Now().Format(time.TimeOnly), message)
}
