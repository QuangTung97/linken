package linken

import (
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type partitionUpdated struct {
	id    PartitionID
	owner string
}

func TestWebsocketClient(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	nodeCalls := 0
	var listenedNodes []string
	var updated []partitionUpdated

	client := NewWebsocketClient(
		"ws://localhost:8765/core",
		"group01", "node01", 3,
		WithClientDialer(websocket.DefaultDialer),
		WithClientNodeListener(func(nodes []string) {
			nodeCalls++
			listenedNodes = nodes
		}),
		WithClientPartitionListener(func(p PartitionID, owner string) {
			updated = append(updated, partitionUpdated{
				id: p, owner: owner,
			})
		}),
		WithClientLogger(tc.logger),
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.Run()
	}()

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, []string{"node01"}, listenedNodes)
	assert.Equal(t, 1, nodeCalls)
	assert.Equal(t, []partitionUpdated{
		{id: 0, owner: "node01"},
		{id: 1, owner: "node01"},
		{id: 2, owner: "node01"},
	}, updated)

	// ANOTHER CLIENT
	anotherClient := NewWebsocketClient(
		"ws://localhost:8765/core",
		"group01", "node02", 3,
		WithClientLogger(tc.logger),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		anotherClient.Run()
	}()

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, []string{"node01", "node02"}, listenedNodes)
	assert.Equal(t, 2, nodeCalls)

	assert.Equal(t, []partitionUpdated{
		{id: 0, owner: "node01"},
		{id: 1, owner: "node01"},
		{id: 2, owner: "node01"},
		{id: 2, owner: ""},
		{id: 2, owner: "node02"},
	}, updated)

	client.Shutdown()
	anotherClient.Shutdown()
	wg.Wait()
}

func TestWebsocketClient_Server_Restart_Not_Yet_ReRun_Client(t *testing.T) {
	tc := newTestCase()

	nodeCalls := 0
	var listenedNodes []string
	var updated []partitionUpdated

	client := NewWebsocketClient(
		"ws://localhost:8765/core",
		"group01", "node01", 3,
		WithClientDialer(websocket.DefaultDialer),
		WithClientNodeListener(func(nodes []string) {
			nodeCalls++
			listenedNodes = nodes
		}),
		WithClientPartitionListener(func(p PartitionID, owner string) {
			updated = append(updated, partitionUpdated{id: p, owner: owner})
		}),
		WithClientLogger(tc.logger),
		WithClientRetryDuration(150*time.Millisecond),
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.Run()
	}()

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, []string{"node01"}, listenedNodes)
	assert.Equal(t, 1, nodeCalls)
	assert.Equal(t, []partitionUpdated{
		{id: 0, owner: "node01"},
		{id: 1, owner: "node01"},
		{id: 2, owner: "node01"},
	}, updated)

	// Server Restart
	tc.shutdown()

	tc = newTestCase()
	defer tc.shutdown()

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 1, nodeCalls)

	client.Shutdown()
	wg.Wait()
}

func TestWebsocketClient_Server_Restart(t *testing.T) {
	tc := newTestCase()

	nodeCalls := 0
	var listenedNodes []string
	var updated []partitionUpdated

	client := NewWebsocketClient(
		"ws://localhost:8765/core",
		"group01", "node01", 3,
		WithClientDialer(websocket.DefaultDialer),
		WithClientNodeListener(func(nodes []string) {
			nodeCalls++
			listenedNodes = nodes
		}),
		WithClientPartitionListener(func(p PartitionID, owner string) {
			updated = append(updated, partitionUpdated{id: p, owner: owner})
		}),
		WithClientLogger(tc.logger),
		WithClientRetryDuration(150*time.Millisecond),
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.Run()
	}()

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, []string{"node01"}, listenedNodes)
	assert.Equal(t, 1, nodeCalls)
	assert.Equal(t, []partitionUpdated{
		{id: 0, owner: "node01"},
		{id: 1, owner: "node01"},
		{id: 2, owner: "node01"},
	}, updated)

	// Server Restart
	tc.shutdown()

	tc = newTestCase()
	defer tc.shutdown()

	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, 2, nodeCalls)
	assert.Equal(t, []string{"node01"}, listenedNodes)

	assert.Equal(t, []partitionUpdated{
		{id: 0, owner: "node01"},
		{id: 1, owner: "node01"},
		{id: 2, owner: "node01"},
		{id: 0, owner: "node01"},
		{id: 1, owner: "node01"},
		{id: 2, owner: "node01"},
	}, updated)

	client.Shutdown()
	wg.Wait()
}
