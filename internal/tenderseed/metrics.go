package tenderseed

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metricsReadHeaderTimeout bounds how long a client may take to send its
// request headers. Without it the metrics endpoint is a free slow-loris
// target.
const metricsReadHeaderTimeout = 5 * time.Second

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
