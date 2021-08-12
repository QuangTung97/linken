package linken

// PartitionID ...
type PartitionID uint32

// PartitionStatus ...
type PartitionStatus int

const (
	// PartitionStatusInit ...
	PartitionStatusInit PartitionStatus = 0

	// PartitionStatusStarting ...
	PartitionStatusStarting PartitionStatus = 1

	// PartitionStatusRunning ...
	PartitionStatusRunning PartitionStatus = 2

	// PartitionStatusStopping ...
	PartitionStatusStopping PartitionStatus = 3
)

type partitionAssigns map[string][]PartitionID

func reallocatePartitions(count int, nodes []string, current partitionAssigns) partitionAssigns {
	high := (count + len(nodes) - 1) / len(nodes)
	low := count / len(nodes)
	highCount := count - low*len(nodes)

	allocatedPartitions := make([]bool, count)

	allocated := make([][]PartitionID, len(nodes))
	for i, node := range nodes {
		numPartitions := low
		if i < highCount {
			numPartitions = high
		}

		n := len(current[node])
		if n > numPartitions {
			n = numPartitions
		}

		allocated[i] = current[node][:n]
		for _, p := range allocated[i] {
			allocatedPartitions[p] = true
		}
	}

	var freePartitions []PartitionID
	for p, used := range allocatedPartitions {
		if !used {
			freePartitions = append(freePartitions, PartitionID(p))
		}
	}

	result := partitionAssigns{}
	for i, node := range nodes {
		numPartitions := low
		if i < highCount {
			numPartitions = high
		}
		missing := numPartitions - len(allocated[i])

		result[node] = append(result[node], allocated[i]...)
		result[node] = append(result[node], freePartitions[:missing]...)
		freePartitions = freePartitions[missing:]
	}
	return result
}
