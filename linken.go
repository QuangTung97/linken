package linken

import (
	"errors"
	"sync"
	"time"
)

// ErrInvalidPartitionCount ...
var ErrInvalidPartitionCount = errors.New("number of partitions not matched")

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

// WatchRequest ...
type WatchRequest struct {
	FromVersion  GroupVersion
	ResponseChan chan<- GroupData
}

// Linken ...
type Linken struct {
	options linkenOptions

	mut    sync.RWMutex
	groups map[string]*linkenGroup
}

type linkenGroup struct {
	mut      sync.Mutex
	count    int
	state    *groupState
	waitList []chan<- GroupData
}

// New ...
func New(options ...Option) *Linken {
	return &Linken{
		groups: map[string]*linkenGroup{},
	}
}

func (l *Linken) getGroup(
	groupName string, inputHandler func(g *linkenGroup) error,
	initFn func() *linkenGroup,
) error {
	handler := func(g *linkenGroup) error {
		defer g.mut.Unlock()
		return inputHandler(g)
	}

	l.mut.RLock()
	group, ok := l.groups[groupName]
	if ok {
		group.mut.Lock()
		l.mut.RUnlock()
		return handler(group)
	}
	l.mut.RUnlock()

	l.mut.Lock()
	group, ok = l.groups[groupName]
	if !ok {
		group = initFn()
		l.groups[groupName] = group
	}
	group.mut.Lock()
	l.mut.Unlock()
	return handler(group)
}

func (l *Linken) initLinkenGroup(g *linkenGroup, count int, prevState *GroupData) {
	g.count = count
	g.state = newGroupStateOptions(count, groupTimerFactoryImpl{}, prevState, l.options)
}

// Join ...
func (l *Linken) Join(groupName string, nodeName string, count int, prevState *GroupData) error {
	return l.getGroup(groupName, func(g *linkenGroup) error {
		if g.state == nil {
			l.initLinkenGroup(g, count, prevState)
		}
		return g.nodeJoin(nodeName, count)
	}, func() *linkenGroup {
		g := &linkenGroup{}
		l.initLinkenGroup(g, count, prevState)
		return g
	})
}

// Watch ...
func (l *Linken) Watch(groupName string, req WatchRequest) {
	_ = l.getGroup(groupName, func(g *linkenGroup) error {
		if g.state == nil || g.state.version < req.FromVersion {
			g.waitList = append(g.waitList, req.ResponseChan)
			return nil
		}
		req.ResponseChan <- g.state.toGroupData()
		return nil
	}, func() *linkenGroup {
		return &linkenGroup{}
	})
}

func (g *linkenGroup) nodeJoin(name string, count int) error {
	if g.count != count {
		return ErrInvalidPartitionCount
	}

	changed := g.state.nodeJoin(name)
	g.groupChanged(changed)
	return nil
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

		data := g.state.toGroupData()
		for _, ch := range g.waitList {
			ch <- data
		}
		g.waitList = g.waitList[:0]
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
