package linken

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func getGroupDataChan(ch <-chan GroupData) GroupData {
	select {
	case d := <-ch:
		return d
	default:
		return GroupData{}
	}
}

func getCurrentGroupData(l *Linken, group string) GroupData {
	ch := make(chan GroupData, 1)
	l.Watch(group, WatchRequest{
		FromVersion:  0,
		ResponseChan: ch,
	})
	return getGroupDataChan(ch)
}

func TestLinken_Join_ErrInvalidPartitionCount(t *testing.T) {
	l := New()
	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	err = l.Join("group01", "node02", 4, nil)
	assert.Equal(t, ErrInvalidPartitionCount, err)
}

func TestLinken_Watch_After_Changed(t *testing.T) {
	l := New()
	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	err = l.Join("group01", "node02", 3, nil)
	assert.Equal(t, nil, err)

	assert.Equal(t, GroupData{
		Version: 2,
		Nodes:   []string{"node01", "node02"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 2},
		},
	}, getCurrentGroupData(l, "group01"))
}

func TestLinken_Watch_Before_Changed(t *testing.T) {
	l := New()

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  0,
		ResponseChan: ch,
	})

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{
		Version: 1,
		Nodes:   []string{"node01"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		},
	}, d)

	err = l.Join("group01", "node02", 3, nil)
	assert.Equal(t, nil, err)

	d = getGroupDataChan(ch)
	assert.Equal(t, GroupData{}, d)
}

func TestLinken_Watch_But_Nothing_Changed(t *testing.T) {
	l := New()

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  2,
		ResponseChan: ch,
	})

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{}, d)
}

func TestLinken_Join_With_Prev_State(t *testing.T) {
	l := New()
	err := l.Join("group01", "node01", 3, &GroupData{
		Version: 20,
		Nodes:   []string{"node01", "node02", "node03"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 18},
			{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 19},
			{Status: PartitionStatusStarting, Owner: "node03", ModVersion: 20},
		},
	})
	assert.Equal(t, nil, err)

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  20,
		ResponseChan: ch,
	})

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{
		Version: 21,
		Nodes:   []string{"node01", "node02", "node03"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 18},
			{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 19},
			{Status: PartitionStatusStarting, Owner: "node03", ModVersion: 20},
		},
	}, d)
}

func TestLinken_Join_With_Prev_State_When_Alreay_Watched(t *testing.T) {
	l := New()

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  0,
		ResponseChan: ch,
	})

	err := l.Join("group01", "node01", 3, &GroupData{
		Version: 20,
		Nodes:   []string{"node01", "node02", "node03"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 18},
			{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 19},
			{Status: PartitionStatusStarting, Owner: "node03", ModVersion: 20},
		},
	})
	assert.Equal(t, nil, err)

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{
		Version: 21,
		Nodes:   []string{"node01", "node02", "node03"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 18},
			{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 19},
			{Status: PartitionStatusStarting, Owner: "node03", ModVersion: 20},
		},
	}, d)
}

func TestLinken_Leave_Normal(t *testing.T) {
	l := New()
	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	err = l.Join("group01", "node02", 3, nil)
	assert.Equal(t, nil, err)

	l.Leave("group01", "node02")

	assert.Equal(t, GroupData{
		Version: 3,
		Nodes:   []string{"node01"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStopping, Owner: "node01", ModVersion: 2},
		},
	}, getCurrentGroupData(l, "group01"))
}

func TestLinken_Leave_Not_Joined(t *testing.T) {
	l := New()
	l.Leave("group01", "node-random")

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  0,
		ResponseChan: ch,
	})

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{
		Version: 1,
		Nodes:   []string{"node01"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		},
	}, d)
}

func TestLinken_Disconnect_And_Then_Expired(t *testing.T) {
	l := New(WithNodeExpiredDuration(10 * time.Millisecond))

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	l.Disconnect("group01", "node01")

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  2,
		ResponseChan: ch,
	})

	time.Sleep(15 * time.Millisecond)

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{
		Version: 2,
		Nodes:   []string{},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusInit, Owner: "", ModVersion: 2},
			{Status: PartitionStatusInit, Owner: "", ModVersion: 2},
			{Status: PartitionStatusInit, Owner: "", ModVersion: 2},
		},
	}, d)

	// Remove all group state after all data are empty
	l.mut.RLock()
	assert.Equal(t, 0, len(l.groups))
	l.mut.RUnlock()
}

func TestLinken_Disconnect_And_Watch_Before_Expired(t *testing.T) {
	l := New(WithNodeExpiredDuration(10 * time.Millisecond))

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	l.Disconnect("group01", "node01")

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  2,
		ResponseChan: ch,
	})

	time.Sleep(5 * time.Millisecond)

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{}, d)
}

func TestLinken_NotifyRunning(t *testing.T) {
	l := New()

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	l.Notify("group01", "node01", []NotifyPartitionData{
		{Action: NotifyActionTypeRunning, Partition: 0, LastVersion: 1},
		{Action: NotifyActionTypeRunning, Partition: 1, LastVersion: 1},
	})

	assert.Equal(t, GroupData{
		Version: 2,
		Nodes:   []string{"node01"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
			{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
		},
	}, getCurrentGroupData(l, "group01"))
}

func TestLinken_NotifyStopped(t *testing.T) {
	l := New()

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	l.Notify("group01", "node01", []NotifyPartitionData{
		{Action: NotifyActionTypeRunning, Partition: 0, LastVersion: 1},
		{Action: NotifyActionTypeRunning, Partition: 1, LastVersion: 1},
		{Action: NotifyActionTypeRunning, Partition: 2, LastVersion: 1},
	})

	err = l.Join("group01", "node02", 3, nil)
	assert.Equal(t, nil, err)

	assert.Equal(t, GroupData{
		Version: 3,
		Nodes:   []string{"node01", "node02"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
			{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
			{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 3},
		},
	}, getCurrentGroupData(l, "group01"))

	l.Notify("group01", "node01", []NotifyPartitionData{
		{Action: NotifyActionTypeStopped, Partition: 2, LastVersion: 3},
	})

	assert.Equal(t, GroupData{
		Version: 4,
		Nodes:   []string{"node01", "node02"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
			{Status: PartitionStatusRunning, Owner: "node01", ModVersion: 2},
			{Status: PartitionStatusStarting, Owner: "node02", ModVersion: 4},
		},
	}, getCurrentGroupData(l, "group01"))
}

func TestLinken_RemoveWatch(t *testing.T) {
	l := New()

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  0,
		ResponseChan: ch,
	})

	l.RemoveWatch("group01", ch)

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{}, d)
}

func TestLinken_RemoveWatch_Before_Anything(t *testing.T) {
	l := New()

	ch01 := make(chan GroupData, 1)
	l.RemoveWatch("group01", ch01)

	ch02 := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  0,
		ResponseChan: ch02,
	})

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	d := getGroupDataChan(ch01)
	assert.Equal(t, GroupData{}, d)

	d = getGroupDataChan(ch02)
	assert.Equal(t, GroupVersion(1), d.Version)
	assert.Equal(t, []string{"node01"}, d.Nodes)
}

func TestLinken_RemoveWatch_In_List_Of_Multi_Watches(t *testing.T) {
	l := New()

	ch01 := make(chan GroupData, 1)
	ch02 := make(chan GroupData, 1)
	ch03 := make(chan GroupData, 1)

	l.Watch("group01", WatchRequest{ResponseChan: ch01})
	l.Watch("group01", WatchRequest{ResponseChan: ch02})
	l.Watch("group01", WatchRequest{ResponseChan: ch03})

	l.RemoveWatch("group01", ch01)

	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	d := getGroupDataChan(ch01)
	assert.Equal(t, GroupData{}, d)

	d = getGroupDataChan(ch02)
	assert.Equal(t, GroupVersion(1), d.Version)
	assert.Equal(t, []string{"node01"}, d.Nodes)

	d = getGroupDataChan(ch03)
	assert.Equal(t, GroupVersion(1), d.Version)
	assert.Equal(t, []string{"node01"}, d.Nodes)
}

func TestLinken_Delete_Group_After_Remove_All_Watches(t *testing.T) {
	l := New()

	ch01 := make(chan GroupData, 1)
	ch02 := make(chan GroupData, 1)
	ch03 := make(chan GroupData, 1)

	l.Watch("group01", WatchRequest{ResponseChan: ch01})
	l.Watch("group01", WatchRequest{ResponseChan: ch02})
	l.Watch("group01", WatchRequest{ResponseChan: ch03})

	l.RemoveWatch("group01", ch01)
	l.RemoveWatch("group01", ch02)
	l.RemoveWatch("group01", ch03)

	assert.Equal(t, 0, len(l.groups))
}

func TestRemoveWatchListEntry(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		a := make(chan GroupData)
		b := make(chan GroupData)
		c := make(chan GroupData)
		d := make(chan GroupData)
		e := make(chan GroupData)

		result := removeWaitListEntry([]chan<- GroupData{a, b, c, d, e}, c)
		assert.Equal(t, []chan<- GroupData{a, b, e, d}, result)
	})

	t.Run("empty", func(t *testing.T) {
		e := make(chan GroupData)

		result := removeWaitListEntry(nil, e)
		assert.Equal(t, []chan<- GroupData(nil), result)
	})

	t.Run("not-in", func(t *testing.T) {
		a := make(chan GroupData)
		b := make(chan GroupData)
		c := make(chan GroupData)
		d := make(chan GroupData)
		e := make(chan GroupData)

		result := removeWaitListEntry([]chan<- GroupData{a, b, c, d}, e)
		assert.Equal(t, []chan<- GroupData{a, b, c, d}, result)
	})
}
