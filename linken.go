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
		options: computeLinkenOptions(options...),
		groups:  map[string]*linkenGroup{},
	}
}

func (l *Linken) tryToDeleteGroup(groupName string) {
	l.mut.Lock()
	defer l.mut.Unlock()

	group, ok := l.groups[groupName]
	if !ok {
		return
	}

	group.mut.Lock()
	defer group.mut.Unlock()

	if group.needDelete() {
		delete(l.groups, groupName)
	}
}

func (l *Linken) getGroup(
	groupName string, inputHandler func(g *linkenGroup) error,
	initFn func() *linkenGroup,
) error {
	needDelete, err := l.getGroupReturningNeedDelete(groupName, inputHandler, initFn)
	if needDelete {
		l.tryToDeleteGroup(groupName)
	}
	return err
}

func (l *Linken) getGroupReturningNeedDelete(
	groupName string, inputHandler func(g *linkenGroup) error,
	initFn func() *linkenGroup,
) (bool, error) {
	handler := func(g *linkenGroup) (bool, error) {
		defer g.mut.Unlock()
		err := inputHandler(g)
		return g.needDelete(), err
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

func (l *Linken) initLinkenGroup(g *linkenGroup, groupName string, count int, prevState *GroupData) {
	g.count = count
	g.state = newGroupStateOptions(count, groupTimerFactoryImpl{
		groupName: groupName,
		root:      l,
	}, prevState, l.options)
}

// Join ...
func (l *Linken) Join(groupName string, nodeName string, count int, prevState *GroupData) error {
	return l.getGroup(groupName, func(g *linkenGroup) error {
		needResponseWatches := false
		if g.state == nil {
			l.initLinkenGroup(g, groupName, count, prevState)
			if prevState != nil {
				needResponseWatches = true
			}
		}
		return g.nodeJoin(nodeName, count, needResponseWatches)
	}, func() *linkenGroup {
		g := &linkenGroup{}
		l.initLinkenGroup(g, groupName, count, prevState)
		return g
	})
}

func (l *Linken) getGroupWithoutInit(groupName string, fn func(g *linkenGroup)) {
	_ = l.getGroup(groupName, func(g *linkenGroup) error {
		if g.state == nil {
			return nil
		}
		fn(g)
		return nil
	}, func() *linkenGroup {
		return &linkenGroup{}
	})
}

// Leave ...
func (l *Linken) Leave(groupName string, nodeName string) {
	l.getGroupWithoutInit(groupName, func(g *linkenGroup) {
		g.nodeLeave(nodeName)
	})
}

// Disconnect ...
func (l *Linken) Disconnect(groupName string, nodeName string) {
	l.getGroupWithoutInit(groupName, func(g *linkenGroup) {
		g.nodeDisconnect(nodeName)
	})
}

func (l *Linken) nodeTimerExpired(groupName string, nodeName string) {
	l.getGroupWithoutInit(groupName, func(g *linkenGroup) {
		g.nodeExpired(nodeName)
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

func (g *linkenGroup) nodeJoin(name string, count int, needResponseWatches bool) error {
	if g.count != count {
		return ErrInvalidPartitionCount
	}

	changed := g.state.nodeJoin(name)
	if changed {
		g.state.version++
	}
	if changed || needResponseWatches {
		g.pushResponseToWatchClients()
	}
	return nil
}

func (g *linkenGroup) nodeDisconnect(name string) {
	g.state.nodeDisconnect(name)
}

func (g *linkenGroup) nodeLeave(name string) {
	changed := g.state.nodeLeave(name)
	g.groupChanged(changed)
}

func (g *linkenGroup) nodeExpired(name string) {
	changed := g.state.nodeExpired(name)
	g.groupChanged(changed)
}

func (g *linkenGroup) notifyRunning(id PartitionID, owner string, lastVersion GroupVersion) {
	changed := g.state.notifyRunning(id, owner, lastVersion)
	g.groupChanged(changed)
}

func (g *linkenGroup) pushResponseToWatchClients() {
	data := g.state.toGroupData()
	for _, ch := range g.waitList {
		ch <- data
	}
	g.waitList = g.waitList[:0]
}

func (g *linkenGroup) groupChanged(changed bool) {
	if changed {
		g.state.version++
		g.pushResponseToWatchClients()
	}
}

func (g *linkenGroup) needDelete() bool {
	return g.state != nil && len(g.state.nodes) == 0 && len(g.waitList) == 0
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
	root      *Linken
}

var _ groupTimerFactory = groupTimerFactoryImpl{}

func (f groupTimerFactoryImpl) newTimer(name string, d time.Duration) groupTimer {
	return groupTimerImpl{
		timer: time.AfterFunc(d, func() {
			f.root.nodeTimerExpired(f.groupName, name)
		}),
	}
}
