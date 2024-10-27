package main

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func IntroduceYourself() (map[string]MemberInfo, *net.Conn, error) {
	conn, err := net.Dial("udp", GetServerEndpoint(INTRODUCER_SERVER_HOST))
	if err != nil {
		return nil, nil, err
	}

	joinMessageEnc, err := GetEncodedJoinMessage()
	if err != nil {
		return nil, nil, err
	}

	var joinMessage Message
	_ = json.Unmarshal(joinMessageEnc, &joinMessage)
	localIP, _ := GetLocalIP()
	PrintMessage("outgoing", joinMessage, localIP)

	conn.Write(joinMessageEnc)

	buffer := make([]byte, 1024)
	mLen, err := conn.Read(buffer)
	if err != nil {
		return nil, nil, err
	}

	members, err := parseMembersFromJoinResponse(buffer[:mLen])
	if err != nil {
		return nil, nil, err
	}

	LogMessage(fmt.Sprintf("Received members: ", members))

	return members, &conn, nil
}

func parseMembersFromJoinResponse(buffer []byte) (map[string]MemberInfo, error) {
	messages, err := DecodeAckMessage(buffer)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, err
	}

	membersEnc := []byte(messages[0].Data)                 // First message is a "JOIN" with membership info
	inSuspectMode, _ = strconv.ParseBool(messages[1].Data) // Second piggyback message is a "SUSPECT_MODE"

	dropoutRateValue := strings.Split(messages[2].Data, " ")[1] // Third piggyback message is a "DROPOUT"
	dropoutRate, _ := strconv.ParseFloat(strings.TrimSpace(dropoutRateValue), 64)
	if err != nil {
		dropRate = 0.0
	} else {
		dropRate = dropoutRate
	}

	if err != nil {
		LogError("Unable to decode the initial suspect state")
	}

	var members map[string]MemberInfo
	err = json.Unmarshal(membersEnc, &members)
	if err != nil {
		LogError("Unable to decode returned members list")
		return nil, nil
	}

	return members, err
}

// Initalizes the Membership Information map for the newly joined node.
// Returns the NODE_ID for this node.
func InitializeMembershipInfoAndList(members map[string]MemberInfo, introducer_conn *net.Conn, localIP string) string {
	nodeId := ""

	for id, memberInfo := range members {
		ip := GetIPFromID(id)

		if ip == INTRODUCER_SERVER_HOST {
			// TODO kartikr2 using pointers, ensure that it works fine.
			AddToMembershipInfo(id, &MemberInfo{
				connection:  introducer_conn,
				host:        ip,
				failed:      memberInfo.failed,
				suspected:   memberInfo.suspected,
				incarnation: memberInfo.incarnation,
			})
		} else if ip == localIP {
			nodeId = id
		} else {
			conn, err := net.Dial("udp", GetServerEndpoint(ip))

			if err != nil {
				LogError(fmt.Sprintf("Failed to estabilish connection with: ", id))
				// TODO what to do here? If it actually failed it should be detected by some other node.
			}

			AddToMembershipInfo(id, &MemberInfo{
				connection:  &conn,
				host:        ip,
				failed:      memberInfo.failed,
				suspected:   memberInfo.suspected,
				incarnation: memberInfo.incarnation,
			})
		}
	}

	return nodeId
}

// Add nodes to membership list and returns a message containing all members.
func IntroduceNodeToGroup(request string, addr *net.UDPAddr) (Message, error) {
	// TODO Add corner case checking, what if the introducer gets a looped around message from
	// the past? It should check that the node doesn't already exist.

	ipAddr := addr.IP.String()
	nodeId := ConstructNodeID(ipAddr)

	// fmt.Printf("IP: %s NodeID: %s", ipAddr, nodeId)

	AddNewMemberToMembershipInfo(nodeId)

	membershipListResponse := GetMembers()

	// For the response, add yourself to the list as well.
	membershipListResponse[NODE_ID] = MemberInfo{
		connection:  nil,
		host:        NODE_ID,
		failed:      false,
		suspected:   false,
		incarnation: INCARNATION,
	}

	membershipListEnc, err := json.Marshal(membershipListResponse)
	if err != nil {
		return Message{}, err
	}

	// TODO is it okay for the kind of this message to be "JOIN"?
	response := Message{
		Kind: JOIN,
		Data: string(membershipListEnc),
	}

	// // Introducer should also disseminate the message.
	// helloMessage := Message{
	// 	Kind: HELLO,
	// 	Data: nodeId,
	// }

	// AddPiggybackMessage(helloMessage)

	return response, err
}
