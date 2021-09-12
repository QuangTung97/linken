package refcount

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPointer_Empty_Pointer(t *testing.T) {
	var p Pointer
	assert.Equal(t, nil, p.Get())
}

func TestPointer_New(t *testing.T) {
	p := New(10, func() {})
	assert.Equal(t, 10, p.Get())
}

func TestPointer_Destroy(t *testing.T) {
	calls := 0
	p := New(10, func() { calls++ })

	p.Destroy()
	assert.Equal(t, nil, p.Get())
	assert.Equal(t, 1, calls)
}

func TestPointer_Clone(t *testing.T) {
	p := New(10, func() {})

	p1 := p.Clone()

	assert.Equal(t, 10, p.Get())
	assert.Equal(t, 10, p1.Get())
}

func TestPointer_Clone_And_Destroy(t *testing.T) {
	calls := 0
	p := New(10, func() { calls++ })

	p1 := p.Clone()

	p.Destroy()

	assert.Equal(t, nil, p.Get())
	assert.Equal(t, 10, p1.Get())
	assert.Equal(t, 0, calls)
}

func TestPointer_Clone_And_Destroy_2_Times(t *testing.T) {
	calls := 0
	p := New(10, func() { calls++ })

	p1 := p.Clone()

	p.Destroy()
	p1.Destroy()

	assert.Equal(t, nil, p.Get())
	assert.Equal(t, nil, p1.Get())
	assert.Equal(t, 1, calls)
}

func TestPointer_Assign(t *testing.T) {
	calls01 := 0
	p1 := New(10, func() { calls01++ })

	calls02 := 0
	p2 := New(20, func() { calls02++ })

	p1.Assign(p2)

	assert.Equal(t, 1, calls01)
	assert.Equal(t, 0, calls02)

	p2.Destroy()
	assert.Equal(t, 1, calls01)
	assert.Equal(t, 0, calls02)

	p1.Destroy()
	assert.Equal(t, 1, calls01)
	assert.Equal(t, 1, calls02)
}
