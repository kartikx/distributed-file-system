package main

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// TODO @kartikr2 Do we need failed now?

func GetRingPosition(nodeId string) int {
	hashObject := fnv.New64a()
	hashObject.Write([]byte(nodeId))

	ringPoint := int(hashObject.Sum64() % RING_NUM_POINTS)
	// fmt.Println("Point on Ring for ", inputString, "is: ", ringPoint)
	return ringPoint
}

func GetSortedRingMembers() []RingMemberInfo {
	var members []RingMemberInfo = GetMemberAndRingPositions()

	members = append(members, RingMemberInfo{NODE_ID, RING_POSITION})

	sort.Slice(members, func(i, j int) bool {
		return members[i].RingPosition < members[j].RingPosition
	})

	return members
}

func GetRingSuccessors(ringPosition int) []string {
	var successorNodeIds []string

	var members []RingMemberInfo = GetSortedRingMembers()

	// TODO What to do if numNodes <= 2?

	index := 0
	for index < len(members) && members[index].RingPosition != RING_POSITION {
		index++
	}

	// ! If you spot failed nodes in replicas, add check here.
	for i := 0; i < NUM_REPLICAS-1; i++ {
		successorNodeIds = append(successorNodeIds, members[(index+i+1)%len(members)].Id)
	}

	return successorNodeIds
}

func GetRingPredecessor(ringPosition int) string {
	members := GetSortedRingMembers()

	index := 0
	for index < len(members) && members[index].RingPosition != RING_POSITION {
		index++
	}

	return members[(index+len(members)-1)%len(members)].Id
}

func GetPrimaryReplicaForFile(filename string) string {
	fileHash := GetRingPosition(filename)

	members := GetSortedRingMembers()

	index := 0
	for index < len(members) && members[index].RingPosition < fileHash {
		index++
	}

	return members[index%len(members)].Id
}

func PrintRing() {
	members := GetSortedRingMembers()

	for i, m := range members {
		fmt.Printf("%d NODE: %s RING POSITION: %d\n", i, m.Id, m.RingPosition)
	}
}
