package linken

import (
	"sort"
	"time"
)

type nodeStatus int

const (
	nodeStatusAlive  nodeStatus = 0
	nodeStatusDead   nodeStatus = 1
	nodeStatusZombie nodeStatus = 2
)

type nodeInfo struct {
	status     nodeStatus
	lastActive time.Time
}

type groupVersion uint64

type groupState struct {
	version    groupVersion
	nodes      map[string]nodeInfo
	partitions []partitionInfo
}

type partitionInfo struct {
	status     PartitionStatus
	owner      string
	nextOwner  string
	modVersion groupVersion // modify version
}

func newGroupState(count int) *groupState {
	return &groupState{
		version:    0,
		nodes:      map[string]nodeInfo{},
		partitions: make([]partitionInfo, count),
	}
}

func (s *groupState) nodeJoin(name string) bool {
	s.nodes[name] = nodeInfo{}

	nodes := make([]string, 0, len(s.nodes))
	for name := range s.nodes {
		nodes = append(nodes, name)
	}
	sort.Strings(nodes)

	current := map[string][]PartitionID{}
	for i, p := range s.partitions {
		id := PartitionID(i)

		currentName := p.owner
		current[currentName] = append(current[currentName], id)
	}

	expected := reallocatePartitions(len(s.partitions), nodes, current)

	for expectedName, expectedIDs := range expected {
		for _, id := range expectedIDs {
			prev := s.partitions[id]
			if prev.status == PartitionStatusStarting && prev.owner != expectedName {
				s.partitions[id] = partitionInfo{
					status:     PartitionStatusStopping,
					owner:      prev.owner,
					nextOwner:  expectedName,
					modVersion: s.version + 1,
				}
				continue
			}

			if prev.status == PartitionStatusInit {
				s.partitions[id] = partitionInfo{
					status:     PartitionStatusStarting,
					owner:      expectedName,
					nextOwner:  "",
					modVersion: s.version + 1,
				}
			}
		}
	}

	return true
}

func (s *groupState) notifyRunning(id PartitionID, lastVersion groupVersion) bool {
	s.partitions[id].status = PartitionStatusRunning
	s.partitions[id].modVersion = s.version + 1
	return true
}
