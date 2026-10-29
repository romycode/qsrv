// Package metrics is a minimal Prometheus text-format (0.0.4) exporter:
// counters and histograms with labels, plus gauges computed at scrape time.
// Deliberately dependency-free; covers exactly what qsrv needs.
package metrics

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Label is one name/value pair.
type Label struct{ Name, Value string }

func L(name, value string) Label { return Label{name, value} }

func fmtLabels(ls []Label, extra ...Label) string {
	all := append(slices.Clone(ls), extra...)
	if len(all) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, l := range all {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteString(`="`)
		b.WriteString(escape(l.Value))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

var escaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)

func escape(s string) string { return escaper.Replace(s) }

func fmtFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// --- histogram ---

type Histogram struct {
	bounds []float64
	counts []atomic.Uint64 // per bucket (non-cumulative); last = +Inf
	sum    atomic.Uint64   // float64 bits
	count  atomic.Uint64
}

func newHistogram(bounds []float64) *Histogram {
	return &Histogram{bounds: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
}

func (h *Histogram) Observe(v float64) {
	i, _ := slices.BinarySearch(h.bounds, v)
	h.counts[i].Add(1)
	h.count.Add(1)
	for {
		old := h.sum.Load()
		if h.sum.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

func (h *Histogram) write(w io.Writer, name string, ls []Label) {
	var cum uint64
	for i, b := range h.bounds {
		cum += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket%s %d\n", name, fmtLabels(ls, L("le", fmtFloat(b))), cum)
	}
	cum += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket%s %d\n", name, fmtLabels(ls, L("le", "+Inf")), cum)
	fmt.Fprintf(w, "%s_sum%s %s\n", name, fmtLabels(ls), fmtFloat(math.Float64frombits(h.sum.Load())))
	fmt.Fprintf(w, "%s_count%s %d\n", name, fmtLabels(ls), cum)
}

// --- vectors (label set -> series) ---

type series[T any] struct {
	labels []Label
	v      T
}

type vec[T any] struct {
	name, help, typ string
	mu              sync.RWMutex
	m               map[string]*series[T]
	mk              func() T
}

func (v *vec[T]) get(ls []Label) T {
	key := fmtLabels(ls)
	v.mu.RLock()
	s, ok := v.m[key]
	v.mu.RUnlock()
	if ok {
		return s.v
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if s, ok = v.m[key]; !ok {
		s = &series[T]{labels: slices.Clone(ls), v: v.mk()}
		v.m[key] = s
	}
	return s.v
}

// remove drops every series whose label `name` equals value (e.g. a deleted queue).
func (v *vec[T]) remove(name, value string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for k, s := range v.m {
		for _, l := range s.labels {
			if l.Name == name && l.Value == value {
				delete(v.m, k)
				break
			}
		}
	}
}

func (v *vec[T]) sorted() []*series[T] {
	v.mu.RLock()
	keys := make([]string, 0, len(v.m))
	for k := range v.m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := make([]*series[T], len(keys))
	for i, k := range keys {
		out[i] = v.m[k]
	}
	v.mu.RUnlock()
	return out
}

type CounterVec struct{ vec[*atomic.Uint64] }

func (c *CounterVec) With(ls ...Label) *atomic.Uint64 { return c.get(ls) }
func (c *CounterVec) Add(n uint64, ls ...Label)       { c.get(ls).Add(n) }
func (c *CounterVec) Inc(ls ...Label)                 { c.get(ls).Add(1) }

type HistogramVec struct{ vec[*Histogram] }

func (h *HistogramVec) With(ls ...Label) *Histogram { return h.get(ls) }

// --- registry ---

// GaugeFunc emits gauge samples at scrape time.
type GaugeFunc func(emit func(value float64, ls ...Label))

type gauge struct {
	name, help string
	fn         GaugeFunc
}

type Registry struct {
	mu       sync.Mutex
	counters []*CounterVec
	hists    []*HistogramVec
	gauges   []gauge
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) Counter(name, help string) *CounterVec {
	c := &CounterVec{vec[*atomic.Uint64]{name: name, help: help, typ: "counter", m: map[string]*series[*atomic.Uint64]{}, mk: func() *atomic.Uint64 { return new(atomic.Uint64) }}}
	r.mu.Lock()
	r.counters = append(r.counters, c)
	r.mu.Unlock()
	return c
}

func (r *Registry) Histogram(name, help string, bounds []float64) *HistogramVec {
	slices.Sort(bounds)
	h := &HistogramVec{vec[*Histogram]{name: name, help: help, typ: "histogram", m: map[string]*series[*Histogram]{}, mk: func() *Histogram { return newHistogram(bounds) }}}
	r.mu.Lock()
	r.hists = append(r.hists, h)
	r.mu.Unlock()
	return h
}

func (r *Registry) Gauge(name, help string, fn GaugeFunc) {
	r.mu.Lock()
	r.gauges = append(r.gauges, gauge{name, help, fn})
	r.mu.Unlock()
}

// Forget drops all counter/histogram series carrying label name=value.
func (r *Registry) Forget(name, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.counters {
		c.remove(name, value)
	}
	for _, h := range r.hists {
		h.remove(name, value)
	}
}

func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	counters, hists, gauges := slices.Clone(r.counters), slices.Clone(r.hists), slices.Clone(r.gauges)
	r.mu.Unlock()

	for _, c := range counters {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
		for _, s := range c.sorted() {
			fmt.Fprintf(w, "%s%s %d\n", c.name, fmtLabels(s.labels), s.v.Load())
		}
	}
	for _, h := range hists {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
		for _, s := range h.sorted() {
			s.v.write(w, h.name, s.labels)
		}
	}
	for _, g := range gauges {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name)
		g.fn(func(v float64, ls ...Label) {
			fmt.Fprintf(w, "%s%s %s\n", g.name, fmtLabels(ls), fmtFloat(v))
		})
	}
}

// ExponentialBuckets returns n bounds starting at start, each factor times the previous.
func ExponentialBuckets(start, factor float64, n int) []float64 {
	b := make([]float64, n)
	for i := range b {
		b[i] = start
		start *= factor
	}
	return b
}
