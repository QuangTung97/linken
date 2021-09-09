package linken

import (
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
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
}

var _ http.Handler = &WebsocketHandler{}

// NewWebsocketHandler ...
func NewWebsocketHandler(options ...Option) *WebsocketHandler {
	l := New(options...)
	opts := computeLinkenOptions(options...)
	return &WebsocketHandler{
		options: opts,
		linken:  l,
	}
}

// ServeHTTP ...
func (h *WebsocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.options.logger.Error("Fail to upgrade to websocket", zap.Error(err))
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	var cmd ServerCommand
	err = conn.ReadJSON(&cmd)
	if err != nil {
		h.options.logger.Error("Error while ReadJSON", zap.Error(err))
		return
	}

	joinCmd := cmd.Join
	err = h.linken.Join(joinCmd.GroupName, joinCmd.NodeName, joinCmd.PartitionCount, joinCmd.PrevState)
	if err != nil {
		h.options.logger.Error("Error while Join", zap.Error(err))
		return
	}

	ch := make(chan GroupData, 1)
	h.linken.Watch(joinCmd.GroupName, WatchRequest{
		FromVersion:  0,
		ResponseChan: ch,
	})

	groupData := <-ch

	err = conn.WriteJSON(groupData)
	if err != nil {
		h.options.logger.Error("Error while WriteJSON", zap.Error(err))
		return
	}
}
