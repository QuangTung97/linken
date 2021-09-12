package refcount

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPointer_Stress_Test(t *testing.T) {
	calls := uint32(0)
	p := New(20, func() {
		atomic.AddUint32(&calls, 1)
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()

		for i := 0; i < 100000; i++ {
			newPtr := New(30, func() {
				atomic.AddUint32(&calls, 1)
			})
			p.Assign(newPtr)
			newPtr.Destroy()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 200000; i++ {
			tmp := p.Clone()
			tmp.Destroy()
		}
	}()

	wg.Wait()
	p.Destroy()

	assert.Equal(t, uint32(100001), calls)
}
