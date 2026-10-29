package queue

// heapOf is a minimal generic adapter over container/heap.
type heapOf[T any] struct {
	s    []T
	less func(a, b T) bool
}

func newHeap[T any](less func(a, b T) bool) *heapOf[T] { return &heapOf[T]{less: less} }

func (h *heapOf[T]) Len() int           { return len(h.s) }
func (h *heapOf[T]) Less(i, j int) bool { return h.less(h.s[i], h.s[j]) }
func (h *heapOf[T]) Swap(i, j int)      { h.s[i], h.s[j] = h.s[j], h.s[i] }
func (h *heapOf[T]) Push(x any)         { h.s = append(h.s, x.(T)) }
func (h *heapOf[T]) Pop() any {
	n := len(h.s)
	x := h.s[n-1]
	var zero T
	h.s[n-1] = zero
	h.s = h.s[:n-1]
	return x
}
