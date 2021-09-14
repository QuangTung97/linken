package linken

import (
	"context"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"sync"
	"time"
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

func sleepContext(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Run ...
func (c *WebsocketClient) Run() {
	for {
		c.runInLoop()
		sleepContext(c.rootCtx, c.options.retryDuration)
		if c.rootCtx.Err() != nil {
			return
		}
	}
}

func (c *WebsocketClient) runInLoop() {
	logger := c.options.logger

	conn, _, err := c.options.dialer.DialContext(c.rootCtx, "ws://localhost:8765/core", nil)
	if err != nil {
		logger.Error("Dial server failed", zap.Error(err))
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	err = conn.WriteJSON(ServerCommand{
		Type: ServerCommandTypeJoin,
		Join: &ServerJoinCommand{
			GroupName:      c.groupName,
			NodeName:       c.nodeName,
			PartitionCount: c.count,
			Secret:         c.options.secret,
			PrevState:      c.prevState,
		},
	})
	if err != nil {
		logger.Error("Error while WriteJSON", zap.Error(err))
		return
	}

	notifyCh := make(chan []NotifyPartitionData, 1)
	ctx, cancel := context.WithCancel(c.rootCtx)

	c.prevState = nil

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer cancel()

		c.notifyServer(ctx, conn, notifyCh)
	}()

	go func() {
		defer wg.Done()
		defer cancel()

		for {
			continuing := c.runSingleHandlingLoop(ctx, conn, notifyCh)
			if c.rootCtx.Err() != nil {
				return
			}
			if !continuing {
				return
			}
		}
	}()

	wg.Wait()
}

func (c *WebsocketClient) runNodeListener(data GroupData) {
	var prevNodes []string
	if c.prevState != nil {
		prevNodes = c.prevState.Nodes
	}

	if nodesChanged(prevNodes, data.Nodes) {
		c.options.nodeListener(data.Nodes)
	}
}

func (c *WebsocketClient) runPartitionListener(data GroupData) {
	for i, p := range data.Partitions {
		id := PartitionID(i)

		prev := PartitionInfo{}
		if c.prevState != nil {
			prev = c.prevState.Partitions[i]
		}

		prevOwner := ""
		if prev.Status == PartitionStatusRunning {
			prevOwner = c.prevState.Partitions[i].Owner
		}

		owner := ""
		if p.Status == PartitionStatusRunning {
			owner = p.Owner
		}

		if prevOwner != owner {
			c.options.partitionListener(id, owner)
		}
	}
}

func (c *WebsocketClient) notifyServer(ctx context.Context, conn *websocket.Conn, ch <-chan []NotifyPartitionData) {
	logger := c.options.logger
	defer closeConnGracefully(c.rootCtx, conn, logger)

	for {
		select {
		case <-ctx.Done():
			return

		case notifyList := <-ch:
			err := conn.WriteJSON(ServerCommand{
				Type:   ServerCommandTypeNotify,
				Notify: notifyList,
			})
			if err != nil {
				logger.Error("Error while WriteJSON", zap.Error(err))
				return
			}
		}
	}
}

func (c *WebsocketClient) runSingleHandlingLoop(
	ctx context.Context, conn *websocket.Conn, notifyCh chan<- []NotifyPartitionData,
) bool {
	logger := c.options.logger

	var data GroupData
	err := conn.ReadJSON(&data)
	if err != nil {
		if errorIsCloseNormal(err) {
			return false
		}
		logger.Error("Error while ReadJSON", zap.Error(err))
		return false
	}

	c.runNodeListener(data)
	c.runPartitionListener(data)

	var prevPartitions []PartitionInfo
	if c.prevState != nil {
		prevPartitions = c.prevState.Partitions
	}

	notifyList := computeClientNotifyList(c.nodeName, prevPartitions, data.Partitions)
	if len(notifyList) > 0 {
		select {
		case <-ctx.Done():
		case notifyCh <- notifyList:
		}
	}

	c.prevState = &data
	return true
}

// Shutdown ...
func (c *WebsocketClient) Shutdown() {
	c.cancel()
}

func nodesChanged(prevNodes []string, current []string) bool {
	prev := map[string]bool{}
	for _, n := range prevNodes {
		prev[n] = false
	}

	for _, n := range current {
		_, existed := prev[n]
		if !existed {
			return true
		}
		prev[n] = true
	}

	for _, visited := range prev {
		if !visited {
			return true
		}
	}

	return false
}

func computeClientNotifyList(
	nodeName string, prevPartitions []PartitionInfo, current []PartitionInfo,
) []NotifyPartitionData {
	var notifyList []NotifyPartitionData
	for id, p := range current {
		prev := PartitionInfo{}
		if len(prevPartitions) > 0 {
			prev = prevPartitions[id]
		}

		if p.ModVersion <= prev.ModVersion {
			continue
		}

		if p.Owner != nodeName {
			continue
		}

		if p.Status == PartitionStatusStarting {
			notifyList = append(notifyList, NotifyPartitionData{
				Action:      NotifyActionTypeRunning,
				Partition:   PartitionID(id),
				LastVersion: p.ModVersion,
			})
			continue
		}

		if p.Status == PartitionStatusStopping {
			notifyList = append(notifyList, NotifyPartitionData{
				Action:      NotifyActionTypeStopped,
				Partition:   PartitionID(id),
				LastVersion: p.ModVersion,
			})
		}
	}
	return notifyList
}
