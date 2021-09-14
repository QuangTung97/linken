package linken

import (
	"go.uber.org/zap"
	"time"
)

// GroupSecret ...
type GroupSecret struct {
	Write string // write secret
	Read  string // read secret
}

type linkenOptions struct {
	nodeExpiredDuration time.Duration
	logger              *zap.Logger
	groupSecrets        map[string]GroupSecret
}

// Option ...
type Option func(opts *linkenOptions)

func computeLinkenOptions(options ...Option) linkenOptions {
	result := linkenOptions{
		nodeExpiredDuration: 30 * time.Second,
		logger:              zap.NewNop(),
		groupSecrets:        map[string]GroupSecret{},
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

// WithGroupSecret ...
func WithGroupSecret(groupName string, secret GroupSecret) Option {
	return func(opts *linkenOptions) {
		opts.groupSecrets[groupName] = secret
	}
}
