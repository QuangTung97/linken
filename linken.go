package linken

// Linken ...
type Linken struct {
}

// InitGroupData ...
type InitGroupData struct {
	Name           string     `json:"name"`
	PartitionCount int        `json:"partitionCount"`
	PrevState      *GroupData `json:"prevState"`
}

// GroupData ...
type GroupData struct {
	Version    int             `json:"version"`
	Nodes      []string        `json:"nodes"`
	Partitions []PartitionInfo `json:"partitions"`
}

// New ...
func New(options ...Option) *Linken {
	return &Linken{}
}
