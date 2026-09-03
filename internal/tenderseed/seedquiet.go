package tenderseed

import (
	"context"
	"errors"
	"log/slog"
)

// hangUpMessage replaces the core's wording when the seed is the one hanging
// up. "Stopping peer for error" describes what the switch does, not what
// happened, and what happened here is the seed finishing its job.
const hangUpMessage = "hung up after serving"

// quietSeedHandler lowers one log record from error to info: the one the
// switch emits when this seed closes a connection it has finished answering.
//
// The switch has a single exported way to drop a peer, and it reports every
// use of it as an error. For a full node that is right, since a peer only
// leaves on failure. For a seed it is wrong on the main path: hanging up is
// the seed's purpose, so a busy seed would fill its journal with errors that
// are not errors, and any alert set on the error level would be useless.
//
// The test is the error value, not the message text, so a reworded message
// upstream does not silently disable the filter. Nothing else in this binary
// carries that error, so nothing else can be caught by mistake.
//
// The alternative was leaving the noise and documenting it. Filtering was
// chosen because the journal is read by one person over years, and a journal
// whose normal traffic looks like failure stops being read.
type quietSeedHandler struct {
	inner slog.Handler
}

// quietSeedLogger wraps a logger so the seed's own hang-ups stop being
// reported as errors.
func quietSeedLogger(logger *slog.Logger) *slog.Logger {
	return slog.New(quietSeedHandler{inner: logger.Handler()})
}

func (h quietSeedHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h quietSeedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return quietSeedHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h quietSeedHandler) WithGroup(name string) slog.Handler {
	return quietSeedHandler{inner: h.inner.WithGroup(name)}
}

func (h quietSeedHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level != slog.LevelError || !carriesSeedServed(record) {
		return h.inner.Handle(ctx, record)
	}

	// The record was admitted at the error level; at info it may not be.
	if !h.inner.Enabled(ctx, slog.LevelInfo) {
		return nil
	}

	quiet := slog.NewRecord(record.Time, slog.LevelInfo, hangUpMessage, record.PC)

	// The error attribute goes: it named a failure that did not happen.
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != "err" {
			quiet.AddAttrs(attr)
		}
		return true
	})

	return h.inner.Handle(ctx, quiet)
}

// carriesSeedServed reports whether a record carries the reason the seed gives
// when it hangs up on a peer it has served.
func carriesSeedServed(record slog.Record) bool {
	found := false

	record.Attrs(func(attr slog.Attr) bool {
		err, ok := attr.Value.Any().(error)
		if ok && errors.Is(err, errSeedServed) {
			found = true
			return false
		}
		return true
	})

	return found
}
