package linken

import (
	"sync"
	"time"
)

// InitGroupData ...
type InitGroupData struct {
	GroupName      string     `json:"groupName"`
	NodeName       string     `json:"nodeName"`
	Secret         string     `json:"secret"`
	PartitionCount int        `json:"partitionCount"`
	PrevState      *GroupData `json:"prevState"`
}

// GroupData ...
type GroupData struct {
	Version    GroupVersion    `json:"version"`
	Nodes      []string        `json:"nodes"`
	Partitions []PartitionInfo `json:"partitions"`
}

// NotifyPartitionData ...
type NotifyPartitionData struct {
	Partition   PartitionID  `json:"partition"`
	LastVersion GroupVersion `json:"lastVersion"`
}

// Linken ...
type Linken struct {
	options linkenOptions

	mut    sync.RWMutex
	groups map[string]*linkenGroup
}

type linkenGroup struct {
	mut      sync.Mutex
	state    *groupState
	waitList []chan<- GroupData
}

// New ...
func New(options ...Option) *Linken {
	return &Linken{}
}

// Join ...
func (l *Linken) Join(groupName string, nodeName string, count int, prevState *GroupData) {
	l.mut.RLock()
	group, ok := l.groups[groupName]
	if ok {
		group.mut.Lock()
		l.mut.RUnlock()
		group.nodeJoin(nodeName)
		return
	}
	l.mut.RUnlock()

	l.mut.Lock()
	group, ok = l.groups[groupName]
	if !ok {
		// TODO from prevState
		prev := nullGroupData{}
		group = &linkenGroup{
			state: newGroupStateOptions(count, groupTimerFactoryImpl{}, prev, l.options),
		}
		l.groups[groupName] = group
	}

	group.mut.Lock()
	l.mut.Unlock()
	group.nodeJoin(nodeName)
}

func (g *linkenGroup) nodeJoin(name string) {
	defer g.mut.Unlock()

	changed := g.state.nodeJoin(name)
	g.groupChanged(changed)
}

func (g *linkenGroup) nodeDisconnect(name string) {
	defer g.mut.Unlock()

	g.state.nodeDisconnect(name)
}

func (g *linkenGroup) nodeLeave(name string) {
	defer g.mut.Unlock()

	changed := g.state.nodeLeave(name)
	g.groupChanged(changed)
}

func (g *linkenGroup) nodeExpired(name string) {
	defer g.mut.Unlock()

	changed := g.state.nodeExpired(name)
	g.groupChanged(changed)
}

func (g *linkenGroup) notifyRunning(id PartitionID, owner string, lastVersion GroupVersion) {
	defer g.mut.Unlock()

	changed := g.state.notifyRunning(id, owner, lastVersion)
	g.groupChanged(changed)

}

func (g *linkenGroup) groupChanged(changed bool) {
	if changed {
		g.state.version++
		// TODO send to wait list
	}
}

type groupTimerImpl struct {
	timer *time.Timer
}

var _ groupTimer = groupTimerImpl{}

func (s groupTimerImpl) stop() {
	s.timer.Stop()
}

type groupTimerFactoryImpl struct {
	groupName string
}

var _ groupTimerFactory = groupTimerFactoryImpl{}

func (f groupTimerFactoryImpl) newTimer(name string, d time.Duration) groupTimer {
	return groupTimerImpl{
		timer: time.AfterFunc(d, func() {
			// TODO timer expired
		}),
	}
}
