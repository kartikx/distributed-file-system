package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
)

func CalculatePointOnRing(inputString string) int {
	hashObject := fnv.New64a()
	hashObject.Write([]byte(inputString))

	ringPoint := int(hashObject.Sum64() % RING_NUM_POINTS)
	fmt.Println("Point on Ring for ", inputString, "is: ", ringPoint)
	return ringPoint
}

func GetNodesOnRingForPoint(pointOnRing int) ([]string, error) {
	var returnNodeIds []string

	// Get all the members in the membership list
	members := GetMembers()
	var member_ids []string
	for k := range members {
		member_ids = append(member_ids, k)
	}

	var effectiveMemberRingPositions map[string]int = make(map[string]int)
	for eachMemberId := range members {
		// For the points between 0 and pointOnRing, add RING_NUM_POINTS
		// so that they come in the order
		if members[eachMemberId].ringPosition < pointOnRing {
			effectiveMemberRingPositions[eachMemberId] = members[eachMemberId].ringPosition + RING_NUM_POINTS
		}
	}

	// Sort the membership list according to the position on the ring
	sort.SliceStable(member_ids, func(i, j int) bool {
		return effectiveMemberRingPositions[member_ids[i]] < effectiveMemberRingPositions[member_ids[j]]
	})

	// Get the first NUM_REPLICAS node IDs to return
	for _, eachMemberId := range member_ids {

		if (!members[eachMemberId].failed) &&
			(len(returnNodeIds) < NUM_REPLICAS) &&
			(effectiveMemberRingPositions[eachMemberId] > pointOnRing) {
			returnNodeIds = append(returnNodeIds, eachMemberId)
		}
	}

	var err error
	if len(returnNodeIds) == 0 {
		err = errors.New("No nodes found on the ring for the given point")
	}

	return returnNodeIds, err
}
