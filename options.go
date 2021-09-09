package linken

import (
	"go.uber.org/zap"
	"time"
)

type linkenOptions struct {
	nodeExpiredDuration time.Duration
	logger              *zap.Logger
}

// Option ...
type Option func(opts *linkenOptions)

func computeLinkenOptions(options ...Option) linkenOptions {
	result := linkenOptions{
		nodeExpiredDuration: 30 * time.Second,
		logger:              zap.NewNop(),
	}
	for _, o := range options {
		o(&result)
	}
	return result
}

// WithNodeExpiredDuration ...
func WithNodeExpiredDuration(d time.Duration) Option {
	return func(opts *linkenOptions) {
		opts.nodeExpiredDuration = d
	}
}

// WithLogger ...
func WithLogger(logger *zap.Logger) Option {
	return func(opts *linkenOptions) {
		opts.logger = logger
	}
}
