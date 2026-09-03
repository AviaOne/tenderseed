package tenderseed

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// The stages a seed decision is taken at, and the outcomes each can reach.
const (
	stageServe = "serve"
	stageLearn = "learn"
	stageSweep = "sweep"

	resultServed   = "served"
	resultEmpty    = "empty"
	resultFailed   = "failed"
	resultAccepted = "accepted"
	resultRejected = "rejected"
	resultRetried  = "retried"
	resultDropped  = "dropped"
)

// seedTM2Outcomes lists the pairs that can actually occur, not their cartesian
// product: nothing publishes a series that cannot exist. This mirrors what the
// Cosmos side already does, so an operator reads the same shape on both
// stacks.
//
// One series per interpretable behaviour, never one per branch of code. That
// is why an answer refused for lack of a fresh address and an answer refused
// because the send queue was full are separate, they mean different things,
// while a hang up scheduled and a hang up done straight away are not, they
// mean the same thing under a different setting.
var seedTM2Outcomes = [][2]string{
	{resultServed, stageServe},
	{resultEmpty, stageServe},
	{resultFailed, stageServe},
	{resultAccepted, stageLearn},
	{resultRejected, stageLearn},
	{resultRetried, stageSweep},
	{resultDropped, stageSweep},
}

// seedTM2Metrics exports what a TM2 seed decides and what its book holds.
//
// Nothing upstream reports either: the core p2p layer counts peers, bytes and
// messages, never what a seed chose to serve or refuse. The empty answers are
// the series that matters most, since a seed that serves nothing looks healthy
// from the outside.
type seedTM2Metrics struct {
	decisions *prometheus.CounterVec
	book      *prometheus.GaugeVec
}

// newSeedTM2Metrics registers the series under the configured namespace.
//
// Registration is the step that validates: a bad name builds a collector that
// only fails when registered. The error is returned rather than raised, so an
// unusable metrics_namespace is reported like any other configuration error
// instead of bringing the binary down at start.
func newSeedTM2Metrics(namespace string) (*seedTM2Metrics, error) {
	decisions := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "seed_tm2",
		Name:      "decisions_total",
		Help:      "Decisions taken by the TM2 seed reactor, by outcome and by the stage that took them.",
	}, []string{"result", "stage"})

	registered, err := registerCounterVec(decisions)
	if err != nil {
		return nil, err
	}
	decisions = registered

	book := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "seed_tm2",
		Name:      "book_addresses",
		Help:      "Addresses held by the TM2 seed, by state: known is every address, fresh is those reached recently enough to be served.",
	}, []string{"state"})

	registeredBook, err := registerGaugeVec(book)
	if err != nil {
		return nil, err
	}
	book = registeredBook

	// Publish every reachable pair at zero, so a share can be read from the
	// first scrape rather than from the first occurrence.
	for _, outcome := range seedTM2Outcomes {
		decisions.WithLabelValues(outcome[0], outcome[1])
	}

	book.WithLabelValues("known")
	book.WithLabelValues("fresh")

	return &seedTM2Metrics{decisions: decisions, book: book}, nil
}

func registerCounterVec(vec *prometheus.CounterVec) (*prometheus.CounterVec, error) {
	err := prometheus.DefaultRegisterer.Register(vec)
	if err == nil {
		return vec, nil
	}

	var already prometheus.AlreadyRegisteredError
	if !errors.As(err, &already) {
		return nil, err
	}

	existing, ok := already.ExistingCollector.(*prometheus.CounterVec)
	if !ok {
		return nil, err
	}

	return existing, nil
}

func registerGaugeVec(vec *prometheus.GaugeVec) (*prometheus.GaugeVec, error) {
	err := prometheus.DefaultRegisterer.Register(vec)
	if err == nil {
		return vec, nil
	}

	var already prometheus.AlreadyRegisteredError
	if !errors.As(err, &already) {
		return nil, err
	}

	existing, ok := already.ExistingCollector.(*prometheus.GaugeVec)
	if !ok {
		return nil, err
	}

	return existing, nil
}

// observe records one decision. A nil receiver is the disabled case, so the
// reactor never has to ask whether metrics are on.
func (m *seedTM2Metrics) observe(result, stage string) {
	if m == nil {
		return
	}

	m.decisions.WithLabelValues(result, stage).Inc()
}

// observeMany records the same decision several times over.
func (m *seedTM2Metrics) observeMany(result, stage string, count int) {
	if m == nil || count <= 0 {
		return
	}

	m.decisions.WithLabelValues(result, stage).Add(float64(count))
}

// setBook publishes the size of the book and of its servable part.
func (m *seedTM2Metrics) setBook(known, fresh int) {
	if m == nil {
		return
	}

	m.book.WithLabelValues("known").Set(float64(known))
	m.book.WithLabelValues("fresh").Set(float64(fresh))
}
