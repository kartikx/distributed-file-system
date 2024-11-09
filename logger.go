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

	case CREATE:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] CREATE message with %s\n", currentTime, direction, nodeId, message.Data)

	case APPEND:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] APPEND message with %s\n", currentTime, direction, nodeId, message.Data)

	case CHECK:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] CHECK message with %s\n", currentTime, direction, nodeId, message.Data)

	case FILES:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] FILES message with %s\n", currentTime, direction, nodeId, message.Data)

	case GETFILE:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] GETFILE message with %s\n", currentTime, direction, nodeId, message.Data)

	case MERGE:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] MERGE message with %s\n", currentTime, direction, nodeId, message.Data)

	case DELETE:
		fmt.Fprintf(log_file_writer, "[%s] [%s] [%s] DELETE message with %s\n", currentTime, direction, nodeId, message.Data)

	default:
		fmt.Fprintf(log_file_writer, "[%s] [%s] ********Trying to print unknown message type: %d**********", currentTime, direction, message.Kind)
	}
	fmt.Fprintf(log_file_writer, "---------\n")
	log_file_writer.Flush()
}

func LogMessage(message string) {
	fmt.Fprintf(log_file_writer, "[%s] %s\n", time.Now().Format(time.TimeOnly), message)
	log_file_writer.Flush()
}

func LogError(message string) {
	fmt.Fprintf(log_file_writer, "[%s] ERROR: %s\n", time.Now().Format(time.TimeOnly), message)
	log_file_writer.Flush()
}
