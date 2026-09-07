package tenderseed

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	p2ptypes "github.com/gnolang/gno/tm2/pkg/p2p/types"
)

// Node is one running seed instance, whatever stack serves it. It is the
// boundary between what the two stacks share and what each one owns.
//
// Everything above this interface is common and is not duplicated: the flags,
// the environment variables, the configuration with its keys and their
// validation, the whole Prometheus surface, and the version subcommand. None
// of them imports a stack. Everything below it is per stack: the transport,
// the address book, the reactor, the switch, and the logger they use.
//
// TrapSignal belongs to the interface for that last reason. The handler logs
// while it shuts down, and the logger type is the one thing that differs
// across the boundary, log.Logger on the Cosmos side against *slog.Logger on
// the TM2 side. Leaving the trap to the stack keeps that type out of the
// common code, which is why internal/cmd/start.go imports no stack at all.
type Node interface {
	// Start starts serving. It returns once the node is running.
	Start() error
	// Wait blocks until the node stops.
	Wait()
	// Stop saves what has to be saved and releases the listening socket.
	Stop() error
	// TrapSignal installs the shutdown handler. Call it after Start.
	TrapSignal()
}

// NewNode builds the seed of the stack declared in the configuration.
//
// The stack is read here and nowhere else, so this switch is the single place
// where the two branches meet. out receives the log stream: each stack builds
// its own logger from it, since they do not share a logger type.
func NewNode(homeDir string, seedConfig Config, out io.Writer) (Node, error) {
	stack, err := seedConfig.SeedStack()
	if err != nil {
		return nil, err
	}

	switch stack {
	case StackCosmos:
		seed, err := NewSeed(homeDir, seedConfig, log.NewTMLogger(log.NewSyncWriter(out)))
		if err != nil {
			return nil, err
		}
		return seed, nil
	case StackTM2:
		seed, err := NewSeedTM2(homeDir, seedConfig, out)
		if err != nil {
			return nil, err
		}
		return seed, nil
	}

	// SeedStack only ever returns one of the two above; this keeps the
	// compiler happy without a default that would hide a third value.
	return nil, fmt.Errorf("stack: unhandled value %q", stack)
}

// NodeID returns the identity this seed announces, in the format of the
// configured stack, generating the key file if it does not exist yet.
//
// The stack decides here and not in the caller because the two identities are
// not interchangeable: a 20 byte hex address on the Cosmos side, a bech32
// string prefixed g1 on the TM2 side, read from two different key file
// formats. Showing one where the other is expected would hand an operator a
// seed address no peer can dial.
func NodeID(homeDir string, seedConfig Config) (string, error) {
	stack, err := seedConfig.SeedStack()
	if err != nil {
		return "", err
	}

	nodeKeyFilePath := seedConfig.NodeKeyFile
	if !filepath.IsAbs(nodeKeyFilePath) {
		nodeKeyFilePath = filepath.Join(homeDir, nodeKeyFilePath)
	}
	if err := os.MkdirAll(filepath.Dir(nodeKeyFilePath), 0o750); err != nil {
		return "", err
	}

	switch stack {
	case StackCosmos:
		nodeKey, err := p2p.LoadOrGenNodeKey(nodeKeyFilePath)
		if err != nil {
			return "", nodeKeyError(nodeKeyFilePath, StackCosmos, err)
		}
		return string(nodeKey.ID()), nil
	case StackTM2:
		nodeKey, err := p2ptypes.LoadOrMakeNodeKey(nodeKeyFilePath)
		if err != nil {
			return "", nodeKeyError(nodeKeyFilePath, StackTM2, err)
		}
		return nodeKey.ID().String(), nil
	}

	return "", fmt.Errorf("stack: unhandled value %q", stack)
}

// nodeKeyError says which file failed and for which stack, without naming a
// cause it has not established.
//
// The decoder that fails says only that some JSON did not fit some Go type:
// it names no file on the TM2 side and neither the file nor the cause on the
// Cosmos side. The likeliest cause is a home directory built for the other
// stack, since the two key formats are not interchangeable, but this wrapper
// sees every failure including a permission denied and a truncated file, and
// it cannot tell them apart. Announcing a stack mismatch on a permission
// error would send an operator down the wrong path, which is exactly the
// fault this fork keeps finding in its own documentation.
func nodeKeyError(path, stack string, err error) error {
	return fmt.Errorf("node key %s could not be read as a %s key. One common "+
		"cause is a home directory built for the other stack, since a home "+
		"belongs to one stack and the two key formats are not "+
		"interchangeable. Underlying error: %w",
		path, stack, err)
}
