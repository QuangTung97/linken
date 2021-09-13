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
}

func connectToServer() *websocket.Conn {
	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:8765/core", nil)
	if err != nil {
		panic(err)
	}
	return conn
}

func connectToServerReadonly() *websocket.Conn {
	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:8765/readonly", nil)
	if err != nil {
		panic(err)
	}
	return conn
}

func connWriteText(t *testing.T, conn *websocket.Conn, s string) {
	t.Helper()
	err := conn.WriteMessage(websocket.TextMessage, []byte(s))
	assert.Equal(t, nil, err)
}

func connReadText(t *testing.T, conn *websocket.Conn) string {
	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, nil, err)
	assert.Equal(t, websocket.TextMessage, msgType)
	return string(data)
}

func assertCloseEOF(t *testing.T, conn *websocket.Conn) {
	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, &websocket.CloseError{
		Code: websocket.CloseAbnormalClosure,
		Text: "unexpected EOF",
	}, err)
	assert.Equal(t, -1, msgType)
	assert.Equal(t, "", string(data))
}

func newTestCase(options ...Option) *testCase {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	options = append(options, WithLogger(logger))
	handler := NewWebsocketHandler(options...)

	mux := http.NewServeMux()
	mux.Handle("/core", handler)
	mux.Handle("/readonly", handler.Readonly())
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

	return &testCase{
		wg:      wg,
		handler: handler,
		server:  server,
	}
}

func (tc *testCase) shutdown() {
	tc.handler.Shutdown()
	err := tc.server.Shutdown(context.Background())
	if err != nil {
		panic(err)
	}

	tc.wg.Wait()
}

func closeWebsocket(t *testing.T, conn *websocket.Conn) {
	t.Helper()

	err := conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		panic(err)
	}

	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, &websocket.CloseError{Code: websocket.CloseNormalClosure}, err)
	assert.Equal(t, -1, msgType)
	assert.Equal(t, "", string(data))
}

func joinNodeForTest(t *testing.T, conn *websocket.Conn, groupName string, nodeName string, count int) {
	t.Helper()

	_ = conn.WriteJSON(ServerCommand{
		Type: ServerCommandTypeJoin,
		Join: &ServerJoinCommand{
			GroupName:      groupName,
			NodeName:       nodeName,
			PartitionCount: count,
		},
	})

	msgType, _, err := conn.ReadMessage()
	assert.Equal(t, nil, err)
	assert.Equal(t, websocket.TextMessage, msgType)
}

func TestWebsocketHandler_HandShake_Normal(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn := connectToServer()
	defer func() { _ = conn.Close() }()

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

	closeWebsocket(t, conn)
	tc.handler.Shutdown()
}

func TestWebsocketHandler_HandShake_JSON_Error(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn := connectToServer()
	defer func() { _ = conn.Close() }()

	joinReq := `123`
	_ = conn.WriteMessage(websocket.TextMessage, []byte(joinReq))

	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, &websocket.CloseError{
		Code: websocket.CloseAbnormalClosure,
		Text: "unexpected EOF",
	}, err)
	assert.Equal(t, -1, msgType)
	assert.Equal(t, "", string(data))

	tc.handler.Shutdown()
}

func TestWebsocketHandler_HandShake_Wrong_Type(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn := connectToServer()
	defer func() { _ = conn.Close() }()

	joinReq := `
{
  "type": "notify"
}

`
	_ = conn.WriteMessage(websocket.TextMessage, []byte(joinReq))

	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, &websocket.CloseError{
		Code: websocket.CloseAbnormalClosure,
		Text: "unexpected EOF",
	}, err)
	assert.Equal(t, -1, msgType)
	assert.Equal(t, "", string(data))

	tc.handler.Shutdown()
}

func TestWebsocketHandler_HandShake_With_PrevState(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn := connectToServer()
	defer func() { _ = conn.Close() }()

	joinReq := `
{
  "type": "join",
  "join": {
    "groupName": "group01",
    "nodeName": "node01",
    "partitionCount": 3,
    "prevState": {
      "version": 10,
      "nodes": ["node01", "node02"],
      "partitions": [
        {
          "status": 1,
          "owner": "node01",
          "modVersion": 9
        },
        {
          "status": 1,
          "owner": "node01",
          "modVersion": 9
        },
        {
          "status": 1,
          "owner": "node02",
          "modVersion": 10
        }
      ]
    }
  }
}
`
	_ = conn.WriteMessage(websocket.TextMessage, []byte(joinReq))

	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, nil, err)
	assert.Equal(t, websocket.TextMessage, msgType)

	expected := `
{
  "version": 11,
  "nodes": [
    "node01",
    "node02"
  ],
  "partitions": [
    {
      "status": 1,
      "owner": "node01",
      "nextOwner": "",
      "modVersion": 9
    },
    {
      "status": 1,
      "owner": "node01",
      "nextOwner": "",
      "modVersion": 9
    },
    {
      "status": 1,
      "owner": "node02",
      "nextOwner": "",
      "modVersion": 10
    }
  ]
}
`
	assert.Equal(t, strings.TrimSpace(expected), formatJSON(string(data)))

	closeWebsocket(t, conn)
	tc.handler.Shutdown()
}

func TestWebsocketHandler_Notify(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn := connectToServer()
	defer func() { _ = conn.Close() }()

	joinNodeForTest(t, conn, "group01", "node01", 3)

	_ = conn.WriteMessage(websocket.TextMessage, []byte(`
{
  "type": "notify",
  "notify": [
    {
      "action": 1,
      "partition": 0,
      "lastVersion": 1
    },
    {
      "action": 1,
      "partition": 1,
      "lastVersion": 1
    }
  ]
}
`))

	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, nil, err)
	assert.Equal(t, websocket.TextMessage, msgType)

	expected := `
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

	closeWebsocket(t, conn)
	tc.handler.Shutdown()
}

func TestWebsocketHandler_Client_Closed_Unexpected(t *testing.T) {
	tc := newTestCase(WithNodeExpiredDuration(50 * time.Millisecond))
	defer tc.shutdown()

	conn1 := connectToServer()
	defer func() { _ = conn1.Close() }()

	conn2 := connectToServer()
	defer func() { _ = conn2.Close() }()

	joinNodeForTest(t, conn1, "group01", "node01", 3)
	joinNodeForTest(t, conn2, "group01", "node02", 3)

	err := conn1.Close()
	assert.Equal(t, nil, err)

	msgType, data, err := conn2.ReadMessage()
	assert.Equal(t, nil, err)
	assert.Equal(t, websocket.TextMessage, msgType)

	expected := `
{
  "version": 3,
  "nodes": [
    "node02"
  ],
  "partitions": [
    {
      "status": 1,
      "owner": "node02",
      "nextOwner": "",
      "modVersion": 3
    },
    {
      "status": 1,
      "owner": "node02",
      "nextOwner": "",
      "modVersion": 3
    },
    {
      "status": 1,
      "owner": "node02",
      "nextOwner": "",
      "modVersion": 3
    }
  ]
}
`
	assert.Equal(t, strings.TrimSpace(expected), formatJSON(string(data)))

	tc.handler.Shutdown()
}

func TestWebsocketHandler_Two_Clients_Closed(t *testing.T) {
	tc := newTestCase()

	conn1 := connectToServer()
	defer func() { _ = conn1.Close() }()

	conn2 := connectToServer()
	defer func() { _ = conn2.Close() }()

	joinNodeForTest(t, conn1, "group01", "node01", 3)
	joinNodeForTest(t, conn2, "group01", "node02", 3)

	err := conn1.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.Equal(t, nil, err)

	err = conn2.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.Equal(t, nil, err)

	tc.handler.Shutdown()
	tc.shutdown()

	tc.handler.linken.mut.RLock()
	assert.Equal(t, 0, len(tc.handler.linken.groups))
	tc.handler.linken.mut.RUnlock()
}

func TestWebsocketHandler_Notify_Failed_Validation(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn := connectToServer()
	defer func() { _ = conn.Close() }()

	joinNodeForTest(t, conn, "group01", "node01", 3)

	_ = conn.WriteMessage(websocket.TextMessage, []byte(`
{
  "type": "notify",
  "notify": [
    {
      "action": 1,
      "partition": 3,
      "lastVersion": 1
    }
  ]
}
`))

	msgType, data, err := conn.ReadMessage()
	assert.Equal(t, &websocket.CloseError{
		Code: websocket.CloseAbnormalClosure,
		Text: "unexpected EOF",
	}, err)
	assert.Equal(t, -1, msgType)
	assert.Equal(t, "", string(data))

	tc.handler.Shutdown()
}

func TestWebsocketHandler_Multiple_Group(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn1 := connectToServer()
	defer func() { _ = conn1.Close() }()

	conn2 := connectToServer()
	defer func() { _ = conn2.Close() }()

	err := conn1.WriteJSON(ServerCommand{
		Type: ServerCommandTypeJoin,
		Join: &ServerJoinCommand{
			GroupName:      "group01",
			NodeName:       "node01",
			PartitionCount: 2,
		},
	})
	assert.Equal(t, nil, err)

	err = conn2.WriteJSON(ServerCommand{
		Type: ServerCommandTypeJoin,
		Join: &ServerJoinCommand{
			GroupName:      "group02",
			NodeName:       "node02",
			PartitionCount: 3,
		},
	})
	assert.Equal(t, nil, err)

	_, data, err := conn1.ReadMessage()
	assert.Equal(t, nil, err)
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
    }
  ]
}
`
	assert.Equal(t, strings.TrimSpace(expected), formatJSON(string(data)))

	_, data, err = conn2.ReadMessage()
	assert.Equal(t, nil, err)
	expected = `
{
  "version": 1,
  "nodes": [
    "node02"
  ],
  "partitions": [
    {
      "status": 1,
      "owner": "node02",
      "nextOwner": "",
      "modVersion": 1
    },
    {
      "status": 1,
      "owner": "node02",
      "nextOwner": "",
      "modVersion": 1
    },
    {
      "status": 1,
      "owner": "node02",
      "nextOwner": "",
      "modVersion": 1
    }
  ]
}
`
	assert.Equal(t, strings.TrimSpace(expected), formatJSON(string(data)))

	tc.handler.Shutdown()
}

func TestWebsocketHandler_Readonly_Single_Node(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn := connectToServer()
	defer func() { _ = conn.Close() }()

	joinNodeForTest(t, conn, "group01", "node01", 3)

	read := connectToServerReadonly()
	defer func() { _ = read.Close() }()

	connWriteText(t, read, `
{
  "groupName": "group01"
}
`)

	resp := connReadText(t, read)
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
	assert.Equal(t, strings.TrimSpace(expected), formatJSON(resp))

	tc.handler.Shutdown()
}

func TestWebsocketHandler_Readonly_Failed_Group_Empty(t *testing.T) {
	tc := newTestCase()
	defer tc.shutdown()

	conn := connectToServer()
	defer func() { _ = conn.Close() }()

	joinNodeForTest(t, conn, "group01", "node01", 3)

	read := connectToServerReadonly()
	defer func() { _ = read.Close() }()

	connWriteText(t, read, `
{
}
`)

	assertCloseEOF(t, read)

	tc.handler.Shutdown()
}
