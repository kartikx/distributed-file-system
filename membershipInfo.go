// Stores functions for the map that stores membership information.
package main

import (
	"fmt"
	"sync"
)

var membershipInfo map[string]MemberInfo = make(map[string]MemberInfo)

var membershipInfoMutex = sync.RWMutex{}

type RingMemberInfo struct {
	Id           string
	RingPosition int
}

func AddNewMemberToMembershipInfo(nodeId string) error {
	ipAddr := GetIPFromID(nodeId)

	if nodeId == NODE_ID || ipAddr == LOCAL_IP {
		LogError(fmt.Sprintf("Unexpected, attempt to add self. Don't do anything. %s %s %s %s\n",
			nodeId, NODE_ID, ipAddr, LOCAL_IP))
		return nil
	}

	LogMessage(fmt.Sprintf("Adding new member to info %s %s %s %s\n",
		nodeId, NODE_ID, ipAddr, LOCAL_IP))

	membershipInfoMutex.Lock()
	defer membershipInfoMutex.Unlock()

	membershipInfo[nodeId] = MemberInfo{
		Host:         ipAddr,
		failed:       false,
		RingPosition: GetRingPosition(nodeId),
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

func GetMemberAndRingPositions() []RingMemberInfo {
	membershipInfoMutex.RLock()
	defer membershipInfoMutex.RUnlock()

	members := make([]RingMemberInfo, 0, len(membershipInfo))

	for k, v := range membershipInfo {
		members = append(members, RingMemberInfo{k, v.RingPosition})
	}

	return members
}

func PrintMembershipInfo() {
	membershipInfoMutex.RLock()
	defer membershipInfoMutex.RUnlock()

	for k, v := range membershipInfo {
		fmt.Printf("NODE ID: %s RING POSITION: %d\n", k, v.RingPosition)
	}
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
