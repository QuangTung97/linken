package linken

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGroupState_Init(t *testing.T) {
	s := newGroupState(3)
	assert.Equal(t, groupVersion(0), s.version)
	assert.Equal(t, 0, len(s.nodes))

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusInit},
		{status: PartitionStatusInit},
		{status: PartitionStatusInit},
	}, s.partitions)
}

func TestGroupState_First_Join(t *testing.T) {
	s := newGroupState(3)

	changed := s.nodeJoin("node01")
	assert.Equal(t, true, changed)

	assert.Equal(t, map[string]nodeInfo{
		"node01": {status: nodeStatusAlive},
	}, s.nodes)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
	}, s.partitions)

}

func TestGroupState_Second_Join(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.nodeJoin("node02")

	assert.Equal(t, true, changed)

	assert.Equal(t, map[string]nodeInfo{
		"node01": {status: nodeStatusAlive},
		"node02": {status: nodeStatusAlive},
	}, s.nodes)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStopping, owner: "node01", nextOwner: "node02", modVersion: 2},
	}, s.partitions)
}

func TestGroupState_NotifyRunning_After_First_Join(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyRunning(0, 1)

	assert.Equal(t, true, changed)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusRunning, owner: "node01", modVersion: 2},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
	}, s.partitions)
}
