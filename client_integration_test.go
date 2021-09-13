package linken

import (
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestWebsocketClient(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	nodeCalls := 0
	var listenedNodes []string

	client := NewWebsocketClient(
		"ws://localhost:8765/core",
		"group01", "node01", 3,
		WithClientDialer(websocket.DefaultDialer),
		WithClientNodeListener(func(nodes []string) {
			nodeCalls++
			listenedNodes = nodes
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

	client.Shutdown()
	wg.Wait()

	assert.Equal(t, []string{"node01"}, listenedNodes)
	assert.Equal(t, 1, nodeCalls)
}
