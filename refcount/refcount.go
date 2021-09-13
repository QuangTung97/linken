package refcount

import (
	"sync/atomic"
	"unsafe"
)

type pointerHeader struct {
	data       interface{}
	count      uint32
	destructor func()
}

// Pointer ...
type Pointer struct {
	header unsafe.Pointer
}

// New ...
func New(v interface{}, destructor func()) Pointer {
	h := &pointerHeader{
		data:       v,
		count:      1,
		destructor: destructor,
	}
	return Pointer{
		header: unsafe.Pointer(h),
	}
}

func (p *Pointer) getHeader() *pointerHeader {
	return (*pointerHeader)(atomic.LoadPointer(&p.header))
}

// Get ...
func (p *Pointer) Get() interface{} {
	h := p.getHeader()
	if h == nil {
		return nil
	}
	return h.data
}

// Clone ...
func (p *Pointer) Clone() Pointer {
	for {
		h := p.getHeader()
		if h == nil {
			return Pointer{}
		}

		ok := h.tryToIncreaseCount()
		if !ok {
			continue
		}
		return Pointer{
			header: unsafe.Pointer(h),
		}
	}
}

// Destroy ...
func (p *Pointer) Destroy() {
	h := p.getHeader()
	if h == nil {
		return
	}
	atomic.StorePointer(&p.header, nil)
	h.decrease()
}

// Assign ...
func (p *Pointer) Assign(other Pointer) {
	tmp := other.Clone()
	h := p.getHeader()
	atomic.StorePointer(&p.header, tmp.header)
	if h != nil {
		h.decrease()
	}
}

func (h *pointerHeader) decrease() {
	newVal := atomic.AddUint32(&h.count, ^uint32(0))
	if newVal == 0 {
		h.destructor()
	}
}

func (h *pointerHeader) tryToIncreaseCount() bool {
	for {
		last := atomic.LoadUint32(&h.count)
		if last == 0 {
			return false
		}
		swapped := atomic.CompareAndSwapUint32(&h.count, last, last+1)
		if swapped {
			return true
		}
	}
}
