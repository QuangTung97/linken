package linken

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func getGroupDataChan(ch <-chan GroupData) GroupData {
	select {
	case d := <-ch:
		return d
	default:
		return GroupData{}
	}
}

func TestLinken_Join_ErrInvalidPartitionCount(t *testing.T) {
	l := New()
	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	err = l.Join("group01", "node02", 4, nil)
	assert.Equal(t, ErrInvalidPartitionCount, err)
}

func TestLinken(t *testing.T) {
	l := New()
	err := l.Join("group01", "node01", 3, nil)
	assert.Equal(t, nil, err)

	err = l.Join("group01", "node02", 3, nil)
	assert.Equal(t, nil, err)

	ch := make(chan GroupData, 1)
	l.Watch("group01", WatchRequest{
		FromVersion:  0,
		ResponseChan: ch,
	})

	d := getGroupDataChan(ch)
	assert.Equal(t, GroupData{
		Version: 2,
		Nodes:   []string{"node01", "node02"},
		Partitions: []PartitionInfo{
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStarting, Owner: "node01", ModVersion: 1},
			{Status: PartitionStatusStopping, Owner: "node01", NextOwner: "node02", ModVersion: 2},
		},
	}, d)
}
