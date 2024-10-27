// Stores functions for the map that stores membership information.
package main

import (
	"fmt"
	"net"
	"sync"
)

// TODO Rename to use capital M after merge.
var membershipInfo map[string]MemberInfo = make(map[string]MemberInfo)

var membershipInfoMutex = sync.RWMutex{}

func AddNewMemberToMembershipInfo(nodeId string) error {
	ipAddr := GetIPFromID(nodeId)

	if nodeId == NODE_ID || ipAddr == LOCAL_IP {
		LogError(fmt.Sprintf("Unexpected, attempt to add self. Don't do anything. %s %s %s %s\n",
			nodeId, NODE_ID, ipAddr, LOCAL_IP))
		return nil
	}

	LogMessage(fmt.Sprintf("Adding new member to info %s %s %s %s\n",
		nodeId, NODE_ID, ipAddr, LOCAL_IP))

	conn, err := net.Dial("udp", GetServerEndpoint(ipAddr))
	if err != nil {
		return err
	}

	membershipInfoMutex.Lock()
	defer membershipInfoMutex.Unlock()

	membershipInfo[nodeId] = MemberInfo{
		connection:   &conn,
		host:         ipAddr,
		failed:       false,
		ringPosition: CalculatePointOnRing(nodeId),
	}

	LogMessage(fmt.Sprintf("JOIN NODE: %s", nodeId))

	return nil
}

// Returns the members in the group. Doesn't return failed members.
func GetMembers() map[string]MemberInfo {
	members := make(map[string]MemberInfo)

	membershipInfoMutex.RLock()
	defer membershipInfoMutex.RUnlock()

	for k, v := range membershipInfo {
		members[k] = v
	}
	return members
}

func PrintMembershipInfo() {
	membershipInfoMutex.RLock()
	defer membershipInfoMutex.RUnlock()

	for k := range membershipInfo {
		fmt.Printf("NODE ID: %s\n", k)
	}
}

func GetNodeConnection(nodeId string) net.Conn {
	membershipInfoMutex.RLock()
	defer membershipInfoMutex.RUnlock()

	conn := membershipInfo[nodeId].connection

	if conn == nil {
		return nil
	}

	return *conn
}

func AddToMembershipInfo(nodeId string, member *MemberInfo) {
	membershipInfoMutex.Lock()
	defer membershipInfoMutex.Unlock()

	membershipInfo[nodeId] = *member

	LogMessage(fmt.Sprintf("JOIN NODE: %s", nodeId))
}

func GetMemberInfo(nodeId string) (MemberInfo, bool) {
	membershipInfoMutex.RLock()
	defer membershipInfoMutex.RUnlock()

	member, ok := membershipInfo[nodeId]

	return member, ok
}

func DeleteMember(nodeId string) {
	membershipInfoMutex.Lock()
	defer membershipInfoMutex.Unlock()

	member := membershipInfo[nodeId]
	member.failed = true
	membershipInfo[nodeId] = member

	// Deleting a non-existent entry is a no-op, so this operation is safe.
	delete(membershipInfo, nodeId)

	LogMessage(fmt.Sprintf("DELETE NODE: %s", nodeId))
}
