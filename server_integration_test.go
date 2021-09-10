// +build integration

package linken

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func formatJSON(s string) string {
	fmt.Println("Formatting:", s)
	data := json.RawMessage(s)
	result, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(result)
}

type testCase struct {
	wg      *sync.WaitGroup
	handler *WebsocketHandler
	server  *http.Server
	conn    *websocket.Conn
}

func newTestCase(options ...Option) *testCase {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	handler := NewWebsocketHandler(WithLogger(logger))

	mux := http.NewServeMux()
	mux.Handle("/core", handler)
	server := &http.Server{
		Addr:    ":8765",
		Handler: mux,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:8765/core", nil)
	if err != nil {
		panic(err)
	}

	return &testCase{
		wg:      wg,
		handler: handler,
		server:  server,
		conn:    conn,
	}
}

func TestWebsocketHandler_Normal(t *testing.T) {
	tc := newTestCase()
	conn := tc.conn

	joinReq := `
{
  "type": "join",
  "join": {
    "groupName": "group01",
    "nodeName": "node01",
    "partitionCount": 3
  }
}
`
	_ = conn.WriteMessage(websocket.TextMessage, []byte(joinReq))

	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, nil, err)
	assert.Equal(t, websocket.TextMessage, msgType)

	expected := `
{
  "version": 1,
  "nodes": [
    "node01"
  ],
  "partitions": [
    {
      "status": 1,
      "owner": "node01",
      "nextOwner": "",
      "modVersion": 1
    },
    {
      "status": 1,
      "owner": "node01",
      "nextOwner": "",
      "modVersion": 1
    },
    {
      "status": 1,
      "owner": "node01",
      "nextOwner": "",
      "modVersion": 1
    }
  ]
}
`
	assert.Equal(t, strings.TrimSpace(expected), formatJSON(string(data)))

	_ = conn.WriteMessage(websocket.TextMessage, []byte(`
{
  "type": "notify",
  "notify": [
    {
      "action": 1,
      "partition": 0,
      "initVersion": 1
    },
    {
      "action": 1,
      "partition": 1,
      "initVersion": 1
    }
  ]
}
`))

	msgType, data, err = conn.ReadMessage()
	assert.Equal(t, nil, err)
	assert.Equal(t, websocket.TextMessage, msgType)

	expected = `
{
  "version": 2,
  "nodes": [
    "node01"
  ],
  "partitions": [
    {
      "status": 2,
      "owner": "node01",
      "nextOwner": "",
      "modVersion": 2
    },
    {
      "status": 2,
      "owner": "node01",
      "nextOwner": "",
      "modVersion": 2
    },
    {
      "status": 1,
      "owner": "node01",
      "nextOwner": "",
      "modVersion": 1
    }
  ]
}
`
	assert.Equal(t, strings.TrimSpace(expected), formatJSON(string(data)))

	err = conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		panic(err)
	}

	msgType, data, err = conn.ReadMessage()
	assert.Equal(t, &websocket.CloseError{Code: websocket.CloseNormalClosure}, err)
	assert.Equal(t, -1, msgType)
	assert.Equal(t, "", string(data))

	tc.handler.Shutdown()

	err = tc.server.Shutdown(context.Background())
	if err != nil {
		panic(err)
	}

	tc.wg.Wait()
}
