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
	status nodeStatus
}

type groupVersion uint64

//go:generate moq -out state_mocks_test.go . groupTimer groupTimerFactory

type groupTimerFactory interface {
	newTimer(name string, d time.Duration) groupTimer
}

type groupTimer interface {
	stop()
}

type groupState struct {
	factory groupTimerFactory
	options linkenOptions

	version    groupVersion
	nodes      map[string]nodeInfo
	partitions []partitionInfo
	timers     map[string]groupTimer
}

type partitionInfo struct {
	status     PartitionStatus
	owner      string
	nextOwner  string
	modVersion groupVersion // modify version
}

type groupData struct {
	version    groupVersion
	nodes      map[string]struct{}
	partitions []partitionInfo
}

type nullGroupData struct {
	valid bool
	data  groupData
}

func newGroupStateOptions(
	count int, factory groupTimerFactory, prev nullGroupData, opts linkenOptions,
) *groupState {
	nodes := map[string]nodeInfo{}
	partitions := make([]partitionInfo, count)
	version := groupVersion(0)

	if prev.valid {
		version = prev.data.version
		for n := range prev.data.nodes {
			nodes[n] = nodeInfo{
				status: nodeStatusAlive,
			}
		}
		for i := range partitions {
			partitions[i] = prev.data.partitions[i]
		}
	}

	s := &groupState{
		factory: factory,
		options: opts,

		version:    version,
		nodes:      nodes,
		partitions: partitions,
		timers:     map[string]groupTimer{},
	}

	if prev.valid {
		for n := range prev.data.nodes {
			s.nodeDisconnect(n)
		}
		s.reallocate()
		s.version++
	}

	return s
}

func newGroupState(count int) *groupState {
	return newGroupStateOptions(count, nil, nullGroupData{}, computeLinkenOptions())
}

func newGroupStateWithPrev(count int, factory groupTimerFactory, prev groupData) *groupState {
	return newGroupStateOptions(count, factory, nullGroupData{valid: true, data: prev}, computeLinkenOptions())
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

	prev, existed := s.nodes[name]
	if existed && prev.status == nodeStatusAlive {
		return false
	}

	if prev.status == nodeStatusZombie {
		s.timers[name].stop()
		delete(s.timers, name)
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

func (s *groupState) nodeDisconnect(name string) {
	_, existed := s.nodes[name]
	if !existed {
		return
	}

	s.nodes[name] = nodeInfo{
		status: nodeStatusZombie,
	}
	timer := s.factory.newTimer(name, s.options.nodeExpiredDuration)
	s.timers[name] = timer
}

func (s *groupState) nodeExpired(name string) bool {
	delete(s.timers, name)
	return s.nodeLeave(name)
}
