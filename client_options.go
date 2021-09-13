package linken

import (
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// ClientNodeListener ...
type ClientNodeListener func(nodes []string)

// ClientPartitionListener ...
type ClientPartitionListener func(partition PartitionID, owner string)

type clientOptions struct {
	dialer            *websocket.Dialer
	nodeListener      ClientNodeListener
	partitionListener ClientPartitionListener
	logger            *zap.Logger
}

// ClientOption ...
type ClientOption func(opts *clientOptions)

func computeClientOptions(options ...ClientOption) clientOptions {
	opts := clientOptions{
		dialer:            websocket.DefaultDialer,
		nodeListener:      func(nodes []string) {},
		partitionListener: func(partition PartitionID, owner string) {},
		logger:            zap.NewNop(),
	}
	for _, o := range options {
		o(&opts)
	}
	return opts
}

// WithClientNodeListener ...
func WithClientNodeListener(listener ClientNodeListener) ClientOption {
	return func(opts *clientOptions) {
		opts.nodeListener = listener
	}
}

// WithClientPartitionListener ...
func WithClientPartitionListener(listener ClientPartitionListener) ClientOption {
	return func(opts *clientOptions) {
		opts.partitionListener = listener
	}
}

// WithClientDialer ...
func WithClientDialer(dialer *websocket.Dialer) ClientOption {
	return func(opts *clientOptions) {
		opts.dialer = dialer
	}
}

// WithClientLogger ...
func WithClientLogger(logger *zap.Logger) ClientOption {
	return func(opts *clientOptions) {
		opts.logger = logger
	}
}