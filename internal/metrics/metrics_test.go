package metrics

import (
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("x_total", "things")
	c.Inc(L("queue", `we"ird`))
	c.Add(2, L("queue", "b"))
	h := r.Histogram("lat_seconds", "latency", []float64{0.1, 0.01})
	h.With(L("op", "a")).Observe(0.005)
	h.With(L("op", "a")).Observe(0.05)
	h.With(L("op", "a")).Observe(5)
	r.Gauge("depth", "d", func(emit func(float64, ...Label)) { emit(3, L("queue", "b")) })

	var sb strings.Builder
	r.Write(&sb)
	out := sb.String()
	for _, want := range []string{
		`x_total{queue="we\"ird"} 1`,
		`x_total{queue="b"} 2`,
		`lat_seconds_bucket{op="a",le="0.01"} 1`,
		`lat_seconds_bucket{op="a",le="0.1"} 2`,
		`lat_seconds_bucket{op="a",le="+Inf"} 3`,
		`lat_seconds_sum{op="a"} 5.055`,
		`lat_seconds_count{op="a"} 3`,
		`depth{queue="b"} 3`,
		"# TYPE lat_seconds histogram",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}

	r.Forget("queue", "b")
	sb.Reset()
	r.Write(&sb)
	if strings.Contains(sb.String(), `x_total{queue="b"}`) {
		t.Fatal("forget did not remove series")
	}
}
