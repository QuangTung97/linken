// +build integration

package linken

import (
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net/http"
	"strings"
	"testing"
	"time"
)

func formatJSON(s string) string {
	data := json.RawMessage(s)
	result, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(result)
}

func TestWebsocketHandler(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/core", NewWebsocketHandler(WithLogger(logger)))
	server := &http.Server{
		Addr:    ":8765",
		Handler: mux,
	}

	go func() {
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
	err = conn.WriteMessage(websocket.TextMessage, []byte(joinReq))
	if err != nil {
		panic(err)
	}

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

	err = server.Shutdown(context.Background())
	if err != nil {
		panic(err)
	}
}
