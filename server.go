package linken

import (
	"context"
	"errors"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"sync"
)

// ServerCommandType ...
type ServerCommandType string

const (
	// ServerCommandTypeJoin ...
	ServerCommandTypeJoin ServerCommandType = "join"
	// ServerCommandTypeNotify ...
	ServerCommandTypeNotify ServerCommandType = "notify"
)

// ServerCommand ...
type ServerCommand struct {
	Type   ServerCommandType     `json:"type"`
	Join   *ServerJoinCommand    `json:"join"`
	Notify []NotifyPartitionData `json:"notify"`
}

// ServerJoinCommand ...
type ServerJoinCommand struct {
	GroupName      string     `json:"groupName"`
	NodeName       string     `json:"nodeName"`
	PartitionCount int        `json:"partitionCount"`
	Secret         string     `json:"secret"`
	PrevState      *GroupData `json:"prevState"`
}

// WebsocketHandler ...
type WebsocketHandler struct {
	options linkenOptions

	upgrader websocket.Upgrader
	linken   *Linken
	rootCtx  context.Context
	cancel   func()
}

var _ http.Handler = &WebsocketHandler{}

// NewWebsocketHandler ...
func NewWebsocketHandler(options ...Option) *WebsocketHandler {
	l := New(options...)
	opts := computeLinkenOptions(options...)
	ctx, cancel := context.WithCancel(context.Background())

	return &WebsocketHandler{
		options: opts,
		linken:  l,
		rootCtx: ctx,
		cancel:  cancel,
	}
}

// Shutdown does graceful shutdown
func (h *WebsocketHandler) Shutdown() {
	h.cancel()
}

// ServeHTTP ...
func (h *WebsocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r = r.WithContext(ctx)

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.options.logger.Error("Fail to upgrade to websocket", zap.Error(err))
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	sess, ok := h.handShake(conn)
	if !ok {
		return
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		select {
		case <-h.rootCtx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	go func() {
		defer wg.Done()

		h.receiveNotify(h.rootCtx, sess, conn)
		cancel()
	}()

	go func() {
		defer wg.Done()

		h.sendStateUpdate(ctx, sess, conn)
		cancel()
	}()

	<-ctx.Done()

	wg.Wait()
}

type sessionData struct {
	groupName      string
	nodeName       string
	partitionCount int
	initVersion    GroupVersion
}

func validatePrevState(prev *GroupData, partitionCount int) error {
	if prev.Version <= 0 {
		return errors.New("previous state 'version' field must >= 1")
	}
	if len(prev.Nodes) == 0 {
		return errors.New("previous state 'nodes' field must not be empty")
	}
	if len(prev.Partitions) != partitionCount {
		return errors.New("previous state 'partitions' field is missing")
	}

	for _, p := range prev.Partitions {
		if p.Status < 0 || p.Status > PartitionStatusStopping {
			return errors.New("previous state partitions 'status' field is invalid")
		}
		if p.ModVersion > prev.Version {
			return errors.New("previous state partitions 'modVersion' field is too big")
		}
	}
	return nil
}
func validateJoinCmd(cmd ServerCommand) error {
	if cmd.Type != ServerCommandTypeJoin {
		return errors.New("invalid cmd type, must be 'join'")
	}
	if cmd.Join == nil {
		return errors.New("'join' field must not be empty")
	}

	join := cmd.Join
	if len(join.GroupName) == 0 {
		return errors.New("'groupName' field must not be empty")
	}
	if len(join.NodeName) == 0 {
		return errors.New("'nodeName' field must not be empty")
	}
	if join.PartitionCount <= 0 {
		return errors.New("'partitionCount' field must >= 1")
	}

	if join.PrevState != nil {
		err := validatePrevState(join.PrevState, join.PartitionCount)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateNotifyCmd(cmd ServerCommand, partitionCount int) error {
	if cmd.Type != ServerCommandTypeNotify {
		return errors.New("invalid cmd type, must be 'notify'")
	}
	for _, p := range cmd.Notify {
		if p.Action < NotifyActionTypeRunning || p.Action > NotifyActionTypeStopped {
			return errors.New("invalid 'action' field")
		}
		if p.Partition >= PartitionID(partitionCount) {
			return errors.New("'partition' field is too big")
		}
	}
	return nil
}

func (h *WebsocketHandler) handShake(conn *websocket.Conn) (sessionData, bool) {
	logger := h.options.logger

	var cmd ServerCommand
	err := conn.ReadJSON(&cmd)
	if err != nil {
		logger.Error("Error while ReadJSON", zap.Error(err))
		return sessionData{}, false
	}

	err = validateJoinCmd(cmd)
	if err != nil {
		logger.Error("Validate Join Command", zap.Error(err))
		return sessionData{}, false
	}

	joinCmd := cmd.Join
	err = h.linken.Join(joinCmd.GroupName, joinCmd.NodeName, joinCmd.PartitionCount, joinCmd.PrevState)
	if err != nil {
		logger.Error("Error while Join", zap.Error(err))
		return sessionData{}, false
	}

	ch := make(chan GroupData, 1)
	h.linken.Watch(joinCmd.GroupName, WatchRequest{
		FromVersion:  0,
		ResponseChan: ch,
	})

	groupData := <-ch

	err = conn.WriteJSON(groupData)
	if err != nil {
		logger.Error("Error while WriteJSON", zap.Error(err))
		return sessionData{}, false
	}

	return sessionData{
		groupName:      joinCmd.GroupName,
		nodeName:       joinCmd.NodeName,
		partitionCount: joinCmd.PartitionCount,
		initVersion:    groupData.Version,
	}, true
}

func (h *WebsocketHandler) receiveNotify(ctx context.Context, sess sessionData, conn *websocket.Conn) {
	logger := h.options.logger
	gracefulClosed := false
	defer func() {
		if !gracefulClosed {
			h.linken.Disconnect(sess.groupName, sess.nodeName)
		}
	}()

	for {
		var cmd ServerCommand
		err := conn.ReadJSON(&cmd)
		if ctx.Err() != nil {
			h.linken.Leave(sess.groupName, sess.nodeName)
			gracefulClosed = true
			return
		}
		if err != nil {
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseNormalClosure {
				h.linken.Leave(sess.groupName, sess.nodeName)
				gracefulClosed = true
				return
			}

			logger.Error("Error while ReadJSON", zap.Error(err))
			return
		}

		err = validateNotifyCmd(cmd, sess.partitionCount)
		if err != nil {
			logger.Error("Validate Notify Command", zap.Error(err))
			return
		}

		h.linken.Notify(sess.groupName, sess.nodeName, cmd.Notify)
	}
}

func (h *WebsocketHandler) sendStateUpdate(ctx context.Context, sess sessionData, conn *websocket.Conn) {
	fromVersion := sess.initVersion + 1
	ch := make(chan GroupData, 1)

	for {
		h.linken.Watch(sess.groupName, WatchRequest{
			FromVersion:  fromVersion,
			ResponseChan: ch,
		})

		select {
		case data := <-ch:
			err := conn.WriteJSON(data)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				h.options.logger.Error("Error while WriteJSON", zap.Error(err))
				return
			}
			fromVersion = data.Version + 1

		case <-ctx.Done():
			h.linken.RemoveWatch(sess.groupName, ch)
			return
		}
	}
}
