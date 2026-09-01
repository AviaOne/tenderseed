package tenderseed

import (
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metricsReadHeaderTimeout bounds how long a client may take to send its
// request headers. Without it the metrics endpoint is a free slow-loris
// target.
const metricsReadHeaderTimeout = 5 * time.Second

// verifyMetrics exports what the verification does, which nothing upstream
// reports: p2p.Metrics counts peers, bytes and messages, never attempts. The
// series it holds are what an operator, or a later comparison of two builds,
// reads to tell a working backoff from a saturated queue.
type verifyMetrics struct {
	dials *prometheus.CounterVec
}

// newVerifyMetrics registers the counters under the configured namespace.
//
// Registration is the step that validates: a bad name builds a collector that
// only fails when it is registered. The error is returned rather than raised,
// so an unusable metrics_namespace is reported like any other configuration
// error instead of bringing the binary down at start. On the pinned
// dependencies that name is close to unreachable, since the TOML decoder and
// prometheus/common already sanitise it; this is a guard against a future
// version of them behaving differently, and against a double registration.
func newVerifyMetrics(namespace string) (*verifyMetrics, error) {
	dials := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "seed",
		Name:      "verify_dials_total",
		Help:      "Verification decisions taken by the seed reactor, by outcome and by the stage that took them.",
	}, []string{"result", "stage"})

	if err := prometheus.DefaultRegisterer.Register(dials); err != nil {
		var registered prometheus.AlreadyRegisteredError
		if !errors.As(err, &registered) {
			return nil, err
		}
		existing, ok := registered.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			return nil, err
		}
		dials = existing
	}

	// Publish every reachable pair at zero, so a share can be read from the
	// first scrape rather than from the first occurrence. Pairs that cannot
	// occur are not published: an empty series would be a question nobody can
	// answer.
	for _, outcome := range verifyOutcomes {
		dials.WithLabelValues(outcome.result, outcome.stage)
	}
	return &verifyMetrics{dials: dials}, nil
}

func (m *verifyMetrics) observe(result, stage string) {
	m.dials.WithLabelValues(result, stage).Inc()
}

// newMetricsServer builds the HTTP server that exposes Prometheus metrics.
func newMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: metricsReadHeaderTimeout,
	}
}
