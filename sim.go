package main

import "container/heap"

// 离散事件仿真引擎：所有组件（供给耗时、重试退避、启动延迟、节点故障）
// 都以虚拟时间事件驱动，一次仿真实验在毫秒内跑完数百秒虚拟时间。

type event struct {
	t   float64
	seq int
	fn  func()
}

type evHeap []event

func (h evHeap) Len() int { return len(h) }
func (h evHeap) Less(i, j int) bool {
	if h[i].t != h[j].t {
		return h[i].t < h[j].t
	}
	return h[i].seq < h[j].seq
}
func (h evHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *evHeap) Push(x any)   { *h = append(*h, x.(event)) }
func (h *evHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

type Sim struct {
	Now float64
	q   evHeap
	seq int
}

func NewSim() *Sim { return &Sim{} }

// After 在 d 秒（虚拟时间）后执行 fn
func (s *Sim) After(d float64, fn func()) {
	s.seq++
	heap.Push(&s.q, event{t: s.Now + d, seq: s.seq, fn: fn})
}

// Run 消费事件直到队列空或超过 until
func (s *Sim) Run(until float64) {
	for len(s.q) > 0 {
		e := heap.Pop(&s.q).(event)
		if e.t > until {
			break
		}
		s.Now = e.t
		e.fn()
	}
}
