package linken

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReallocatePartitions(t *testing.T) {
	table := []struct {
		name     string
		count    int
		nodes    []string
		current  partitionAssigns
		expected partitionAssigns
	}{
		{
			name:    "3-parts-current-nil",
			count:   3,
			nodes:   []string{"A", "B"},
			current: nil,
			expected: map[string][]PartitionID{
				"A": {0, 1},
				"B": {2},
			},
		},
		{
			name:    "5-parts-current-nil",
			count:   5,
			nodes:   []string{"A", "B"},
			current: nil,
			expected: map[string][]PartitionID{
				"A": {0, 1, 2},
				"B": {3, 4},
			},
		},
		{
			name:    "5-parts-single-node-current-nil",
			count:   5,
			nodes:   []string{"A"},
			current: nil,
			expected: map[string][]PartitionID{
				"A": {0, 1, 2, 3, 4},
			},
		},
		{
			name:    "5-parts-more-nodes-than-partitions",
			count:   5,
			nodes:   []string{"A", "B", "C", "D", "E", "F"},
			current: nil,
			expected: map[string][]PartitionID{
				"A": {0}, "B": {1}, "C": {2},
				"D": {3}, "E": {4}, "F": nil,
			},
		},
		{
			name:  "5-current-single-node",
			count: 5,
			nodes: []string{"A", "B"},
			current: map[string][]PartitionID{
				"A": {3},
			},
			expected: map[string][]PartitionID{
				"A": {3, 0, 1},
				"B": {2, 4},
			},
		},
		{
			name:  "5-current-single-node-with-more-partitions",
			count: 5,
			nodes: []string{"A", "B"},
			current: map[string][]PartitionID{
				"A": {3, 0, 2, 4},
			},
			expected: map[string][]PartitionID{
				"A": {3, 0, 2},
				"B": {1, 4},
			},
		},
		{
			name:  "7-parts-current-single-node-with-more-partitions",
			count: 7,
			nodes: []string{"A", "B", "C"},
			current: map[string][]PartitionID{
				"A": {6, 3, 2, 5},
				"C": {0, 1, 4},
			},
			expected: map[string][]PartitionID{
				"A": {6, 3, 2},
				"B": {4, 5},
				"C": {0, 1},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			result := reallocatePartitions(e.count, e.nodes, e.current)
			assert.Equal(t, e.expected, result)
		})
	}
}
