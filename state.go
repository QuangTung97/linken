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

// GroupVersion ...
type GroupVersion uint64

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

	version    GroupVersion
	nodes      map[string]nodeInfo
	partitions []PartitionInfo
	timers     map[string]groupTimer
}

// PartitionInfo ...
type PartitionInfo struct {
	Status     PartitionStatus `json:"status"`
	Owner      string          `json:"owner"`
	NextOwner  string          `json:"nextOwner"`
	ModVersion GroupVersion    `json:"modVersion"`
}

type groupData struct {
	version    GroupVersion
	nodes      map[string]struct{}
	partitions []PartitionInfo
}

type nullGroupData struct {
	valid bool
	data  groupData
}

func newGroupStateOptions(
	count int, factory groupTimerFactory, prev nullGroupData, opts linkenOptions,
) *groupState {
	nodes := map[string]nodeInfo{}
	partitions := make([]PartitionInfo, count)
	version := GroupVersion(0)

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

	if prev.Status == PartitionStatusInit {
		s.partitions[id] = PartitionInfo{
			Status:     PartitionStatusStarting,
			Owner:      expectedName,
			NextOwner:  "",
			ModVersion: s.version + 1,
		}
		return
	}

	if prev.Owner == expectedName {
		return
	}

	if prev.Status == PartitionStatusStarting || prev.Status == PartitionStatusRunning {
		s.partitions[id] = PartitionInfo{
			Status:     PartitionStatusStopping,
			Owner:      prev.Owner,
			NextOwner:  expectedName,
			ModVersion: s.version + 1,
		}
		return
	}

	s.partitions[id].NextOwner = expectedName
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

		if p.Status == PartitionStatusInit {
			continue
		}

		if p.Status == PartitionStatusStopping {
			if p.NextOwner != "" {
				currentName := p.NextOwner
				current[currentName] = append(current[currentName], id)
			}
		} else {
			currentName := p.Owner
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
	prev, existed := s.nodes[name]
	if existed && prev.status == nodeStatusAlive {
		return false
	}

	if prev.status == nodeStatusZombie {
		s.timers[name].stop()
		delete(s.timers, name)

		s.nodes[name] = nodeInfo{}
		return false
	}

	defer s.reallocate()

	s.nodes[name] = nodeInfo{}
	return true
}

func (s *groupState) notifyRunning(id PartitionID, owner string, lastVersion GroupVersion) bool {
	prev := s.partitions[id]

	if prev.Owner != owner {
		return false
	}
	if lastVersion != prev.ModVersion {
		return false
	}
	if prev.Status != PartitionStatusStarting {
		return false
	}

	s.partitions[id].Status = PartitionStatusRunning
	s.partitions[id].ModVersion = s.version + 1
	return true
}

func (s *groupState) notifyStopped(id PartitionID, owner string, lastVersion GroupVersion) bool {
	prev := s.partitions[id]

	if prev.Owner != owner {
		return false
	}
	if prev.ModVersion != lastVersion {
		return false
	}
	if prev.Status != PartitionStatusStopping {
		return false
	}

	if prev.NextOwner != "" {
		s.partitions[id] = PartitionInfo{
			Status:     PartitionStatusStarting,
			Owner:      prev.NextOwner,
			ModVersion: s.version + 1,
		}
	} else {
		s.partitions[id] = PartitionInfo{
			Status:     PartitionStatusInit,
			ModVersion: s.version + 1,
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
		if prev.Status == PartitionStatusStopping {
			if prev.Owner == name {
				s.partitions[i] = PartitionInfo{
					Status:     PartitionStatusStarting,
					Owner:      prev.NextOwner,
					ModVersion: s.version + 1,
				}
			} else if prev.NextOwner == name {
				s.partitions[i].NextOwner = ""
			}
			continue
		}

		if prev.Owner == name {
			s.partitions[i] = PartitionInfo{
				Status:     PartitionStatusInit,
				ModVersion: s.version + 1,
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

func (s *groupState) toGroupData() GroupData {
	nodes := make([]string, 0, len(s.nodes))
	for n := range s.nodes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	return GroupData{
		Version:    s.version,
		Nodes:      nodes,
		Partitions: s.partitions,
	}
}
