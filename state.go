package linken

import (
	"sort"
	"time"
)

type nodeStatus int

const (
	nodeStatusAlive  nodeStatus = 0
	nodeStatusZombie nodeStatus = 1
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

func (s *groupState) reallocateSinglePartition(id PartitionID, expectedName string) {
	prev := s.partitions[id]

	if prev.status == PartitionStatusInit {
		s.partitions[id] = partitionInfo{
			status:     PartitionStatusStarting,
			owner:      expectedName,
			nextOwner:  "",
			modVersion: s.version + 1,
		}
		return
	}

	if prev.owner == expectedName {
		return
	}

	if prev.status == PartitionStatusStarting || prev.status == PartitionStatusRunning {
		s.partitions[id] = partitionInfo{
			status:     PartitionStatusStopping,
			owner:      prev.owner,
			nextOwner:  expectedName,
			modVersion: s.version + 1,
		}
		return
	}

	s.partitions[id].nextOwner = expectedName
}

func (s *groupState) reallocate() {
	if len(s.nodes) == 0 {
		return
	}

	nodes := make([]string, 0, len(s.nodes))
	for nodeName := range s.nodes {
		nodes = append(nodes, nodeName)
	}
	sort.Strings(nodes)

	current := map[string][]PartitionID{}
	for i, p := range s.partitions {
		id := PartitionID(i)

		if p.status == PartitionStatusInit {
			continue
		}

		if p.status == PartitionStatusStopping {
			if p.nextOwner != "" {
				currentName := p.nextOwner
				current[currentName] = append(current[currentName], id)
			}
		} else {
			currentName := p.owner
			current[currentName] = append(current[currentName], id)
		}
	}

	expected := reallocatePartitions(len(s.partitions), nodes, current)

	for expectedName, expectedIDs := range expected {
		for _, id := range expectedIDs {
			s.reallocateSinglePartition(id, expectedName)
		}
	}
}

func (s *groupState) nodeJoin(name string) bool {
	defer s.reallocate()

	_, existed := s.nodes[name]
	if existed {
		return false
	}

	s.nodes[name] = nodeInfo{}
	return true
}

func (s *groupState) notifyRunning(id PartitionID, owner string, lastVersion groupVersion) bool {
	prev := s.partitions[id]

	if prev.owner != owner {
		return false
	}
	if lastVersion != prev.modVersion {
		return false
	}
	if prev.status != PartitionStatusStarting {
		return false
	}

	s.partitions[id].status = PartitionStatusRunning
	s.partitions[id].modVersion = s.version + 1
	return true
}

func (s *groupState) notifyStopped(id PartitionID, owner string, lastVersion groupVersion) bool {
	prev := s.partitions[id]

	if prev.owner != owner {
		return false
	}
	if prev.modVersion != lastVersion {
		return false
	}
	if prev.status != PartitionStatusStopping {
		return false
	}

	if prev.nextOwner != "" {
		s.partitions[id] = partitionInfo{
			status:     PartitionStatusStarting,
			owner:      prev.nextOwner,
			modVersion: s.version + 1,
		}
	} else {
		s.partitions[id] = partitionInfo{
			status:     PartitionStatusInit,
			modVersion: s.version + 1,
		}
		s.reallocate()
	}
	return true
}

func (s *groupState) nodeLeave(name string) bool {
	_, existed := s.nodes[name]
	if !existed {
		return false
	}
	delete(s.nodes, name)

	defer s.reallocate()

	for i, prev := range s.partitions {
		if prev.status == PartitionStatusStopping {
			if prev.owner == name {
				s.partitions[i] = partitionInfo{
					status:     PartitionStatusStarting,
					owner:      prev.nextOwner,
					modVersion: s.version + 1,
				}
			} else if prev.nextOwner == name {
				s.partitions[i].nextOwner = ""
			}
			continue
		}

		if prev.owner == name {
			s.partitions[i] = partitionInfo{
				status:     PartitionStatusInit,
				modVersion: s.version + 1,
			}
		}
	}

	return true
}
