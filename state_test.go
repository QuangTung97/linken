package linken

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGroupState_Init(t *testing.T) {
	s := newGroupState(3)
	assert.Equal(t, GroupVersion(0), s.version)
	assert.Equal(t, 0, len(s.nodes))

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusInit},
		{Status: PartitionStatusInit},
		{Status: PartitionStatusInit},
	}, s.partitions)
}

func TestGroupState_First_Join(t *testing.T) {
	s := newGroupState(3)

	changed := s.nodeJoin("node01")
	assert.Equal(t, true, changed)

	assert.Equal(t, map[string]nodeInfo{
		"node01": {status: nodeStatusAlive},
	}, s.nodes)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
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

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 2},
	}, s.partitions)
}

func TestGroupState_NotifyRunning_After_First_Join(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyRunning(0, "node01", 1)

	assert.Equal(t, true, changed)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
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

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
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

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
	}, s.partitions)
}

func TestGroupState_NotifyRunning_With_Wrong_Owner_After_First_Join(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyRunning(0, "node02", 1)
	assert.Equal(t, false, changed)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
	}, s.partitions)
}

func TestGroupState_NotifyRunning_With_Wrong_Status(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	changed := s.notifyRunning(2, "node01", 2)
	assert.Equal(t, false, changed)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 2},
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

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", ModVersion: 2},
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

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", ModVersion: 2},
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

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 3},
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

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 3},
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

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 2},
	}, s.partitions)
}

func TestGroupState_Notify_Stopped_With_Wrong_Status(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyStopped(1, "node01", 1)
	assert.Equal(t, false, changed)
}

func TestGroupState_NotifyRunning_LastVersion_Not_Equal(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyRunning(0, "node01", 2)
	assert.Equal(t, false, changed)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
	}, s.partitions)
}

func TestGroupState_Notify_Stopped_LastVersion_Not_Equal(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	s.notifyStopped(2, "node01", 1)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 2},
	}, s.partitions)
}

func TestGroupState_Node_Leave_Owner_When_Stopping(t *testing.T) {
	s := newGroupState(3)
	changed := s.nodeJoin("node01")
	assert.Equal(t, true, changed)
	s.version++

	changed = s.nodeJoin("node02")
	assert.Equal(t, true, changed)
	s.version++

	changed = s.nodeLeave("node01")
	assert.Equal(t, true, changed)
	s.version++

	assert.Equal(t, map[string]nodeInfo{
		"node02": {status: nodeStatusAlive},
	}, s.nodes)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 3},
		{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 3},
		{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 3},
	}, s.partitions)
}

func TestGroupState_Notify_Stopped_When_Next_Owner_Empty(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	s.nodeLeave("node02")
	s.version++

	changed := s.notifyStopped(2, "node01", 2)
	assert.Equal(t, true, changed)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 4},
	}, s.partitions)
}

func TestGroupState_Node_Leave_Become_Empty(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.nodeLeave("node01")
	assert.Equal(t, true, changed)
	s.version++

	assert.Equal(t, GroupVersion(2), s.version)
	assert.Equal(t, map[string]nodeInfo{}, s.nodes)
	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusInit, ModVersion: 2},
		{Status: PartitionStatusInit, ModVersion: 2},
		{Status: PartitionStatusInit, ModVersion: 2},
	}, s.partitions)
}

func TestGroupState_Multi_Partition_Running(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	changed := s.notifyRunning(0, "node01", 1)
	assert.Equal(t, true, changed)
	changed = s.notifyRunning(1, "node01", 1)
	assert.Equal(t, true, changed)
	changed = s.notifyRunning(2, "node01", 1)
	assert.Equal(t, true, changed)

	s.version++

	assert.Equal(t, GroupVersion(2), s.version)
	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
	}, s.partitions)
}

func TestGroupState_Partition_Running_Then_Rebalance(t *testing.T) {
	s := newGroupState(3)
	s.nodeJoin("node01")
	s.version++

	s.notifyRunning(0, "node01", 1)
	s.notifyRunning(1, "node01", 1)
	s.notifyRunning(2, "node01", 1)
	s.version++

	changed := s.nodeJoin("node02")
	assert.Equal(t, true, changed)
	s.version++

	assert.Equal(t, GroupVersion(3), s.version)
	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 3},
	}, s.partitions)
}

func TestGroupState_Partition_Stopping_Then_Rebalance(t *testing.T) {
	s := newGroupState(6)
	s.nodeJoin("node01")
	s.version++

	s.notifyRunning(0, "node01", 1)
	s.notifyRunning(1, "node01", 1)
	s.version++

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
	}, s.partitions)

	s.nodeJoin("node02")
	s.version++

	assert.Equal(t, GroupVersion(3), s.version)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 3},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 3},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 3},
	}, s.partitions)

	changed := s.nodeJoin("node03")
	assert.Equal(t, true, changed)
	s.version++

	assert.Equal(t, GroupVersion(4), s.version)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node03", ModVersion: 4},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 3},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 3},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node03", ModVersion: 3},
	}, s.partitions)
}

func TestGroupState_Rebalance_When_Stopping_Next_Owner_Empty(t *testing.T) {
	s := newGroupState(6)
	s.nodeJoin("node01")
	s.version++

	s.notifyRunning(0, "node01", 1)
	s.notifyRunning(1, "node01", 1)
	s.version++

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
	}, s.partitions)

	s.nodeJoin("node02")
	s.version++

	s.nodeLeave("node02")
	s.version++

	assert.Equal(t, GroupVersion(4), s.version)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "", ModVersion: 3},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "", ModVersion: 3},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "", ModVersion: 3},
	}, s.partitions)

	s.nodeJoin("node03")
	s.version++

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node03", ModVersion: 3},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node03", ModVersion: 3},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node03", ModVersion: 3},
	}, s.partitions)
}

func TestGroupState_NodeDisconnect_And_Expired(t *testing.T) {
	factory := &groupTimerFactoryMock{}

	s := newGroupStateOptions(3, factory, nil,
		computeLinkenOptions(WithNodeExpiredDuration(10*time.Second)))

	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	assert.Equal(t, map[string]nodeInfo{
		"node01": {},
		"node02": {},
	}, s.nodes)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 2},
	}, s.partitions)

	mockTimer := &groupTimerMock{}
	factory.newTimerFunc = func(name string, d time.Duration) groupTimer { return mockTimer }

	s.nodeDisconnect("node02")

	assert.Equal(t, map[string]nodeInfo{
		"node01": {},
		"node02": {
			status: nodeStatusZombie,
		},
	}, s.nodes)

	assert.Equal(t, 1, len(factory.newTimerCalls()))
	assert.Equal(t, "node02", factory.newTimerCalls()[0].Name)
	assert.Equal(t, 10*time.Second, factory.newTimerCalls()[0].D)

	assert.Equal(t, 1, len(s.timers))

	changed := s.nodeExpired("node02")
	assert.Equal(t, true, changed)
	assert.Equal(t, 0, len(s.timers))

	assert.Equal(t, map[string]nodeInfo{
		"node01": {},
	}, s.nodes)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "", ModVersion: 2},
	}, s.partitions)

	changed = s.nodeExpired("node02")
	assert.Equal(t, false, changed)
	assert.Equal(t, 0, len(s.timers))

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "", ModVersion: 2},
	}, s.partitions)
}

func TestGroupState_NodeDisconnect_After_Leave(t *testing.T) {
	factory := &groupTimerFactoryMock{}

	s := newGroupStateOptions(3, factory, nil,
		computeLinkenOptions(WithNodeExpiredDuration(10*time.Second)))

	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	s.nodeLeave("node02")
	s.version++

	s.nodeDisconnect("node02")
	assert.Equal(t, map[string]nodeInfo{
		"node01": {},
	}, s.nodes)
}

func TestGroupState_NodeJoin_After_Disconnect(t *testing.T) {
	factory := &groupTimerFactoryMock{}

	s := newGroupStateOptions(3, factory, nil,
		computeLinkenOptions(WithNodeExpiredDuration(10*time.Second)))

	s.nodeJoin("node01")
	s.version++

	s.nodeJoin("node02")
	s.version++

	mockTimer := &groupTimerMock{}
	factory.newTimerFunc = func(name string, d time.Duration) groupTimer { return mockTimer }

	s.nodeDisconnect("node02")

	mockTimer.stopFunc = func() {}

	changed := s.nodeJoin("node02")
	assert.Equal(t, false, changed)

	assert.Equal(t, map[string]nodeInfo{
		"node01": {},
		"node02": {},
	}, s.nodes)
	assert.Equal(t, 1, len(mockTimer.stopCalls()))
	assert.Equal(t, 0, len(s.timers))

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 2},
	}, s.partitions)
}

func TestGroupState_With_Prev_State(t *testing.T) {
	factory := &groupTimerFactoryMock{}
	factory.newTimerFunc = func(name string, d time.Duration) groupTimer {
		return &groupTimerMock{}
	}

	s := newGroupStateWithPrev(3, factory, &GroupData{
		Version: 10,
		Nodes:   []string{"node01", "node02", "node03"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 8},
			{Status: PartitionStatusRunning, Owner: "node02", ModVersion: 9},
			{Status: PartitionStatusRunning, Owner: "node03", ModVersion: 10},
		},
	})

	assert.Equal(t, GroupVersion(11), s.version)
	assert.Equal(t, map[string]nodeInfo{
		"node01": {status: nodeStatusZombie},
		"node02": {status: nodeStatusZombie},
		"node03": {status: nodeStatusZombie},
	}, s.nodes)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 8},
		{Status: PartitionStatusRunning, Owner: "node02", ModVersion: 9},
		{Status: PartitionStatusRunning, Owner: "node03", ModVersion: 10},
	}, s.partitions)

	calls := factory.newTimerCalls()
	assert.Equal(t, 3, len(calls))

	names := map[string]struct{}{
		calls[0].Name: {},
		calls[1].Name: {},
		calls[2].Name: {},
	}
	assert.Equal(t, map[string]struct{}{
		"node01": {},
		"node02": {},
		"node03": {},
	}, names)
}

func TestGroupState_With_Prev_State_Need_Reallocate(t *testing.T) {
	factory := &groupTimerFactoryMock{}
	factory.newTimerFunc = func(name string, d time.Duration) groupTimer {
		return &groupTimerMock{}
	}

	s := newGroupStateWithPrev(3, factory, &GroupData{
		Version: 10,
		Nodes:   []string{"node01", "node02"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 8},
			{Status: PartitionStatusRunning, Owner: "node02", ModVersion: 9},
			{Status: PartitionStatusRunning, Owner: "node03", ModVersion: 10},
		},
	})

	assert.Equal(t, GroupVersion(11), s.version)
	assert.Equal(t, map[string]nodeInfo{
		"node01": {status: nodeStatusZombie},
		"node02": {status: nodeStatusZombie},
	}, s.nodes)

	assert.Equal(t, []PartitionInfo{
		{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 8},
		{Status: PartitionStatusRunning, Owner: "node02", ModVersion: 9},
		{Status: PartitionStatusStopping, Owner: "node03", ModVersion: 11, NextOwner: "node01"},
	}, s.partitions)

	calls := factory.newTimerCalls()
	assert.Equal(t, 2, len(calls))

	names := map[string]struct{}{
		calls[0].Name: {},
		calls[1].Name: {},
	}
	assert.Equal(t, map[string]struct{}{
		"node01": {},
		"node02": {},
	}, names)
}
