package tenderseed

import (
	"net/http"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metricsReadHeaderTimeout bounds how long a client may take to send its
// request headers. allora leaves this out, which makes the metrics endpoint a
// free slow-loris target.
const metricsReadHeaderTimeout = 5 * time.Second

// newMetricsServer builds the HTTP server that exposes Prometheus metrics.
func newMetricsServer(addr string, logger log.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: metricsReadHeaderTimeout,
	}
}
