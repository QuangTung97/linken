package refcount

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
)

type resource struct {
	num int
}

func newResource(n int) *resource {
	return &resource{num: n}
}

func (r *resource) destroy() {
	r.num = 11
}

func TestPointer_Stress_Test(t *testing.T) {
	calls := uint32(0)
	r := newResource(30)
	p := New(r, func() {
		atomic.AddUint32(&calls, 1)
		r.destroy()
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()

		for i := 0; i < 100000; i++ {
			res := newResource(30)
			newPtr := New(res, func() {
				atomic.AddUint32(&calls, 1)
				res.destroy()
			})
			p.Assign(newPtr)
			newPtr.Destroy()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 200000; i++ {
			tmp := p.Clone()
			r := tmp.Get().(*resource)
			if r.num != 30 {
				panic(fmt.Sprint("Wrong value, real value: ", r.num))
			}
			tmp.Destroy()
		}
	}()

	wg.Wait()
	p.Destroy()

	assert.Equal(t, uint32(100001), calls)
}
