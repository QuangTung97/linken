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

// ServerWatchRequest ...
type ServerWatchRequest struct {
	GroupName string `json:"groupName"`
	Secret    string `json:"secret"`
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

func mergeContext(ctx context.Context, rootCtx context.Context) (context.Context, func()) {
	resultCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-resultCtx.Done(): // avoid goroutine leak
		case <-rootCtx.Done():
			cancel()
		}
	}()
	return resultCtx, cancel
}

// ServeHTTP ...
func (h *WebsocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := mergeContext(r.Context(), h.rootCtx)

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
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer cancel()

		h.receiveNotify(h.rootCtx, sess, conn)
	}()

	go func() {
		defer wg.Done()
		defer cancel()

		h.sendStateUpdate(ctx, sess, conn)
	}()

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

func validateJoinCmdBasicParams(join *ServerJoinCommand, groupSecrets map[string]GroupSecret) error {
	if len(join.GroupName) == 0 {
		return errors.New("'groupName' field must not be empty")
	}
	if len(join.NodeName) == 0 {
		return errors.New("'nodeName' field must not be empty")
	}
	if join.PartitionCount <= 0 {
		return errors.New("'partitionCount' field must >= 1")
	}

	if len(groupSecrets) > 0 {
		secret, ok := groupSecrets[join.GroupName]
		if !ok {
			return errors.New("group secret not existed")
		}
		if join.Secret != secret.Write {
			return errors.New("invalid 'secret' for write permission")
		}
	}
	return nil
}

func validateJoinCmd(cmd ServerCommand, groupSecrets map[string]GroupSecret) error {
	if cmd.Type != ServerCommandTypeJoin {
		return errors.New("invalid cmd type, must be 'join'")
	}
	if cmd.Join == nil {
		return errors.New("'join' field must not be empty")
	}

	join := cmd.Join
	err := validateJoinCmdBasicParams(join, groupSecrets)
	if err != nil {
		return err
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

	err = validateJoinCmd(cmd, h.options.groupSecrets)
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
			if errorIsCloseNormal(err) {
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
	logger := h.options.logger
	defer closeConnGracefully(h.rootCtx, conn, logger)

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
				logger.Error("Error while WriteJSON", zap.Error(err))
				return
			}
			fromVersion = data.Version + 1

		case <-ctx.Done():
			h.linken.RemoveWatch(sess.groupName, ch)
			return
		}
	}
}

func closeConnGracefully(rootCtx context.Context, conn *websocket.Conn, logger *zap.Logger) {
	if rootCtx.Err() != nil {
		err := conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			logger.Error("Error while close conn", zap.Error(err))
		}
	}
}

func (h *WebsocketHandler) readonlyFunc(w http.ResponseWriter, r *http.Request) {
	logger := h.options.logger

	ctx, cancel := mergeContext(r.Context(), h.rootCtx)
	defer cancel()

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("Fail to upgrade to websocket", zap.Error(err))
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	var req ServerWatchRequest
	err = conn.ReadJSON(&req)
	if err != nil {
		logger.Error("Error while ReadJSON", zap.Error(err))
		return
	}

	err = validateReadonlyCommand(req, h.options.groupSecrets)
	if err != nil {
		logger.Error("Validate Readonly Failed", zap.Error(err))
		return
	}

	h.sendStateUpdate(ctx, sessionData{groupName: req.GroupName}, conn)
}

// Readonly ...
func (h *WebsocketHandler) Readonly() http.Handler {
	return http.HandlerFunc(h.readonlyFunc)
}

func errorIsCloseNormal(err error) bool {
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		return false
	}
	return closeErr.Code == websocket.CloseNormalClosure
}

func validateReadonlyCommand(req ServerWatchRequest, secrets map[string]GroupSecret) error {
	if len(req.GroupName) == 0 {
		return errors.New("groupName must not be empty")
	}
	if len(secrets) > 0 {
		secret, ok := secrets[req.GroupName]
		if !ok {
			return errors.New("group secret not existed")
		}
		if req.Secret != secret.Read {
			return errors.New("invalid 'secret' for read permission")
		}
	}
	return nil
}
