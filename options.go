package linken

import "time"

type linkenOptions struct {
	nodeExpiredDuration time.Duration
}

// Option ...
type Option func(opts *linkenOptions)

func computeLinkenOptions(options ...Option) linkenOptions {
	result := linkenOptions{
		nodeExpiredDuration: 30 * time.Second,
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
