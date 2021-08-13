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

	changed := s.notifyRunning(0, "node01", 1)

	assert.Equal(t, true, changed)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusRunning, owner: "node01", modVersion: 2},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
	}, s.partitions)
}

func TestGroupState_Join_Same_Node(t *testing.T) {
	s := newGroupState(3)

	changed := s.nodeJoin("node01")
	assert.Equal(t, true, changed)
	s.version++

	changed = s.nodeJoin("node01")
	assert.Equal(t, false, changed)

	assert.Equal(t, map[string]nodeInfo{
		"node01": {status: nodeStatusAlive},
	}, s.nodes)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
	}, s.partitions)
}

func TestGroupState_NotifyRunning_2_Times_After_First_Join(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyRunning(0, "node01", 1)
	assert.Equal(t, true, changed)

	changed = s.notifyRunning(0, "node01", 1)
	assert.Equal(t, false, changed)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusRunning, owner: "node01", modVersion: 2},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
	}, s.partitions)
}

func TestGroupState_NotifyRunning_With_Wrong_Owner_After_First_Join(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyRunning(0, "node02", 1)
	assert.Equal(t, false, changed)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
	}, s.partitions)
}

func TestGroupState_Node_Leave(t *testing.T) {
	s := newGroupState(3)
	changed := s.nodeJoin("node01")
	assert.Equal(t, true, changed)
	s.version++

	changed = s.nodeJoin("node02")
	assert.Equal(t, true, changed)
	s.version++

	changed = s.nodeLeave("node02")
	assert.Equal(t, true, changed)
	s.version++

	assert.Equal(t, map[string]nodeInfo{
		"node01": {status: nodeStatusAlive},
	}, s.nodes)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 3},
	}, s.partitions)
}

func TestGroupState_Node_Leave_Second_Times(t *testing.T) {
	s := newGroupState(3)
	changed := s.nodeJoin("node01")
	s.version++

	changed = s.nodeJoin("node02")
	s.version++

	changed = s.nodeLeave("node02")
	s.version++

	changed = s.nodeLeave("node02")
	assert.Equal(t, false, changed)

	assert.Equal(t, map[string]nodeInfo{
		"node01": {status: nodeStatusAlive},
	}, s.nodes)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 3},
	}, s.partitions)
}

func TestGroupState_Notify_Stopped_After_Rebalance(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	changed := s.notifyStopped(2, "node01", 2)
	assert.Equal(t, true, changed)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node02", modVersion: 3},
	}, s.partitions)
}

func TestGroupState_Notify_Stopped_2_Times(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	changed := s.notifyStopped(2, "node01", 2)
	assert.Equal(t, true, changed)

	changed = s.notifyStopped(2, "node01", 2)
	assert.Equal(t, false, changed)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node02", modVersion: 3},
	}, s.partitions)
}

func TestGroupState_Notify_Stopped_With_Wrong_Name(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	changed := s.notifyStopped(2, "node03", 2)
	assert.Equal(t, false, changed)

	assert.Equal(t, []partitionInfo{
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStarting, owner: "node01", modVersion: 1},
		{status: PartitionStatusStopping, owner: "node01", nextOwner: "node02", modVersion: 2},
	}, s.partitions)
}

func TestGroupState_Notify_Stopped_With_Wrong_Status(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyStopped(1, "node01", 1)
	assert.Equal(t, false, changed)
}
