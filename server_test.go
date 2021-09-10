package linken

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValidateJoinCmd(t *testing.T) {
	table := []struct {
		name string
		cmd  ServerCommand
		err  error
	}{
		{
			name: "wrong-type",
			cmd: ServerCommand{
				Type: ServerCommandTypeNotify,
			},
			err: errors.New("invalid cmd type, must be 'join'"),
		},
		{
			name: "join-empty",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: nil,
			},
			err: errors.New("'join' field must not be empty"),
		},
		{
			name: "group-empty",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{},
			},
			err: errors.New("'groupName' field must not be empty"),
		},
		{
			name: "node-empty",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName: "some-group",
				},
			},
			err: errors.New("'nodeName' field must not be empty"),
		},
		{
			name: "count-zero",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					PartitionCount: 0,
				},
			},
			err: errors.New("'partitionCount' field must >= 1"),
		},
		{
			name: "ok-without-prev-state",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					PartitionCount: 3,
				},
			},
			err: nil,
		},
		{
			name: "prev-state-version-zero",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					PartitionCount: 3,
					PrevState: &GroupData{
						Version: 0,
					},
				},
			},
			err: errors.New("previous state 'version' field must >= 1"),
		},
		{
			name: "prev-state-node-empty",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					PartitionCount: 3,
					PrevState: &GroupData{
						Version: 10,
						Nodes:   nil,
					},
				},
			},
			err: errors.New("previous state 'nodes' field must not be empty"),
		},
		{
			name: "prev-state-partitions-missing",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					PartitionCount: 3,
					PrevState: &GroupData{
						Version: 10,
						Nodes:   []string{"node01"},
					},
				},
			},
			err: errors.New("previous state 'partitions' field is missing"),
		},
		{
			name: "prev-state-status-invalid",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					PartitionCount: 3,
					PrevState: &GroupData{
						Version: 10,
						Nodes:   []string{"node01"},
						Partitions: []PartitionInfo{
							{
								Status:     4,
								Owner:      "",
								ModVersion: 10,
							},
							{
								Status:     PartitionStatusStopping,
								Owner:      "",
								ModVersion: 8,
							},
							{
								Status:     PartitionStatusInit,
								Owner:      "",
								ModVersion: 9,
							},
						},
					},
				},
			},
			err: errors.New("previous state partitions 'status' field is invalid"),
		},
		{
			name: "prev-state-mod-version-too-big",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					PartitionCount: 3,
					PrevState: &GroupData{
						Version: 10,
						Nodes:   []string{"node01"},
						Partitions: []PartitionInfo{
							{
								Status:     PartitionStatusInit,
								Owner:      "",
								ModVersion: 11,
							},
							{
								Status:     PartitionStatusStopping,
								Owner:      "",
								ModVersion: 8,
							},
							{
								Status:     PartitionStatusStarting,
								Owner:      "",
								ModVersion: 9,
							},
						},
					},
				},
			},
			err: errors.New("previous state partitions 'modVersion' field is too big"),
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			err := validateJoinCmd(e.cmd)
			assert.Equal(t, e.err, err)
		})
	}
}
