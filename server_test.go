package linken

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValidateJoinCmd(t *testing.T) {
	table := []struct {
		name    string
		cmd     ServerCommand
		secrets map[string]GroupSecret
		err     error
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
		{
			name: "invalid-join-secret",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					Secret:         "123",
					PartitionCount: 3,
				},
			},
			secrets: map[string]GroupSecret{
				"some-group": {
					Write: "some-write-secret",
				},
			},
			err: errors.New("invalid 'secret' for write permission"),
		},
		{
			name: "missing-group-secrets",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
				Join: &ServerJoinCommand{
					GroupName:      "some-group",
					NodeName:       "some-node",
					Secret:         "123",
					PartitionCount: 3,
				},
			},
			secrets: map[string]GroupSecret{
				"other-group": {
					Write: "some-write-secret",
				},
			},
			err: errors.New("group secret not existed"),
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			err := validateJoinCmd(e.cmd, e.secrets)
			assert.Equal(t, e.err, err)
		})
	}
}

func TestValidateNotifyCmd(t *testing.T) {
	table := []struct {
		name  string
		count int
		cmd   ServerCommand
		err   error
	}{
		{
			name: "wrong-type",
			cmd: ServerCommand{
				Type: ServerCommandTypeJoin,
			},
			err: errors.New("invalid cmd type, must be 'notify'"),
		},
		{
			name: "invalid-action",
			cmd: ServerCommand{
				Type: ServerCommandTypeNotify,
				Notify: []NotifyPartitionData{
					{
						Action: 0,
					},
				},
			},
			err: errors.New("invalid 'action' field"),
		},
		{
			name: "invalid-action",
			cmd: ServerCommand{
				Type: ServerCommandTypeNotify,
				Notify: []NotifyPartitionData{
					{
						Action: 3,
					},
				},
			},
			err: errors.New("invalid 'action' field"),
		},
		{
			name:  "invalid-partition",
			count: 3,
			cmd: ServerCommand{
				Type: ServerCommandTypeNotify,
				Notify: []NotifyPartitionData{
					{
						Action:    NotifyActionTypeRunning,
						Partition: 0,
					},
					{
						Action:    NotifyActionTypeStopped,
						Partition: 3,
					},
				},
			},
			err: errors.New("'partition' field is too big"),
		},
		{
			name:  "ok",
			count: 3,
			cmd: ServerCommand{
				Type: ServerCommandTypeNotify,
				Notify: []NotifyPartitionData{
					{
						Action:    NotifyActionTypeRunning,
						Partition: 0,
					},
					{
						Action:    NotifyActionTypeStopped,
						Partition: 2,
					},
				},
			},
			err: nil,
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			err := validateNotifyCmd(e.cmd, e.count)
			assert.Equal(t, e.err, err)
		})
	}
}

func TestValidateReadonlyCommand(t *testing.T) {
	table := []struct {
		name    string
		req     ServerWatchRequest
		secrets map[string]GroupSecret
		err     error
	}{
		{
			name: "group-empty",
			err:  errors.New("groupName must not be empty"),
		},
		{
			name: "missing-group-secret",
			req: ServerWatchRequest{
				GroupName: "group01",
			},
			secrets: map[string]GroupSecret{
				"other": {
					Read: "read-secret",
				},
			},
			err: errors.New("group secret not existed"),
		},
		{
			name: "invalid-secret",
			req: ServerWatchRequest{
				GroupName: "group01",
				Secret:    "some-secret",
			},
			secrets: map[string]GroupSecret{
				"group01": {
					Read: "read-secret",
				},
			},
			err: errors.New("invalid 'secret' for read permission"),
		},
		{
			name: "ok-with-secret",
			req: ServerWatchRequest{
				GroupName: "group01",
				Secret:    "read-secret",
			},
			secrets: map[string]GroupSecret{
				"group01": {
					Read: "read-secret",
				},
			},
			err: nil,
		},
		{
			name: "ok-without-secret",
			req: ServerWatchRequest{
				GroupName: "group01",
				Secret:    "some-secret",
			},
			err: nil,
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			err := validateReadonlyCommand(e.req, e.secrets)
			assert.Equal(t, e.err, err)
		})
	}
}
