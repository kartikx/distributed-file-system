package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
)

func JoinRing() {
	if !isLeader {
		members, err := IntroduceYourself()
		if err != nil {
			log.Fatalf("Unable to join the group: %s", err.Error())
		}

		NODE_ID = InitializeMembershipInfoAndList(members, LOCAL_IP)

		helloMessage := Message{
			Kind: HELLO,
			Data: NODE_ID,
		}

		AddPiggybackMessage(helloMessage)
	} else {
		NODE_ID = ConstructNodeID(LEADER_SERVER_HOST)
		RING_POSITION = GetRingPosition(NODE_ID)
	}

	fmt.Println("Joined the group as: ", NODE_ID)
}

func IntroduceYourself() (map[string]MemberInfo, error) {
	conn, err := net.Dial("tcp", GetServerEndpoint(LEADER_SERVER_HOST))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	joinMessageEnc, err := GetEncodedJoinMessage()
	if err != nil {
		return nil, err
	}

	var joinMessage Message
	_ = json.Unmarshal(joinMessageEnc, &joinMessage)
	localIP, _ := GetLocalIP()
	PrintMessage("outgoing", joinMessage, localIP)

	conn.Write(joinMessageEnc)

	buffer := make([]byte, 1024)
	mLen, err := conn.Read(buffer)
	if err != nil {
		return nil, err
	}

	members, err := parseMembersFromJoinResponse(buffer[:mLen])
	if err != nil {
		return nil, err
	}

	return members, nil
}

func parseMembersFromJoinResponse(buffer []byte) (map[string]MemberInfo, error) {
	messages, err := DecodeAckMessage(buffer)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return nil, err
	}

	membersEnc := []byte(messages[0].Data) // First message is a "JOIN" with membership info

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
func InitializeMembershipInfoAndList(members map[string]MemberInfo, localIP string) string {
	nodeId := ""

	for id, memberInfo := range members {
		ip := GetIPFromID(id)
		fmt.Printf("ID: %s Position: %d\n", id, memberInfo.RingPosition)

		if ip == LEADER_SERVER_HOST {
			AddToMembershipInfo(id, &MemberInfo{
				Host:         ip,
				failed:       memberInfo.failed,
				RingPosition: memberInfo.RingPosition,
			})
		} else if ip == localIP {
			nodeId = id
			RING_POSITION = memberInfo.RingPosition
		} else {
			AddToMembershipInfo(id, &MemberInfo{
				Host:         ip,
				failed:       memberInfo.failed,
				RingPosition: memberInfo.RingPosition,
			})
		}
	}

	return nodeId
}

// Add nodes to membership list and returns a message containing all members.
func IntroduceNodeToGroup(request string, ipAddr string) (Message, error) {
	nodeId := ConstructNodeID(ipAddr)

	// fmt.Printf("IP: %s NodeID: %s", ipAddr, nodeId)

	AddNewMemberToMembershipInfo(nodeId)

	membershipListResponse := GetMembers()

	// For the response, add yourself to the list as well.
	// ! @kartikr2 This isn't using the lock.
	membershipListResponse[NODE_ID] = MemberInfo{
		Host:         NODE_ID,
		failed:       false,
		RingPosition: RING_POSITION,
	}

	membershipListEnc, err := json.Marshal(membershipListResponse)
	if err != nil {
		return Message{}, err
	}

	response := Message{
		Kind: JOIN,
		Data: string(membershipListEnc),
	}

	// Note, adding introducer dissemination here could have implications if new node isn't ready to receive messages just yet.

	return response, err
}
