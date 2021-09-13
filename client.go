package linken

import (
	"context"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
)

// WebsocketClient ...
type WebsocketClient struct {
	client  *http.Client
	url     string
	options clientOptions

	groupName string
	nodeName  string
	count     int

	rootCtx context.Context
	cancel  func()

	prevState *GroupData
}

// NewWebsocketClient ...
func NewWebsocketClient(
	url string, groupName string, nodeName string, count int,
	options ...ClientOption,
) *WebsocketClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &WebsocketClient{
		url:     url,
		options: computeClientOptions(options...),

		groupName: groupName,
		nodeName:  nodeName,
		count:     count,

		rootCtx: ctx,
		cancel:  cancel,
	}
}

// Run ...
func (c *WebsocketClient) Run() {
	for {
		c.runInLoop()
		if c.rootCtx.Err() != nil {
			return
		}
		// sleep context
	}
}

func (c *WebsocketClient) runInLoop() {
	logger := c.options.logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, _, err := c.options.dialer.DialContext(c.rootCtx, "ws://localhost:8765/core", nil)
	if err != nil {
		logger.Error("Dial server failed", zap.Error(err))
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	go func() {
		select {
		case <-ctx.Done():
		case <-c.rootCtx.Done():
			err := conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				logger.Error("Error while close conn", zap.Error(err))
			}
		}
	}()

	err = conn.WriteJSON(ServerCommand{
		Type: ServerCommandTypeJoin,
		Join: &ServerJoinCommand{
			GroupName:      c.groupName,
			NodeName:       c.nodeName,
			PartitionCount: c.count,
			PrevState:      c.prevState,
		},
	})
	if err != nil {
		logger.Error("Error while WriteJSON", zap.Error(err))
		return
	}

	for {
		var data GroupData
		err := conn.ReadJSON(&data)
		if c.rootCtx.Err() != nil {
			return
		}
		if err != nil {
			logger.Error("Error while ReadJSON", zap.Error(err))
			return
		}

		c.options.nodeListener(data.Nodes)
		c.prevState = &data
	}
}

// Shutdown ...
func (c *WebsocketClient) Shutdown() {
	c.cancel()
}
