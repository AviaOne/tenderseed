package tenderseed

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/gnolang/gno/tm2/pkg/amino"
	"github.com/gnolang/gno/tm2/pkg/crypto"
	p2ptypes "github.com/gnolang/gno/tm2/pkg/p2p/types"
)

// This file is the receiving half of the discovery protocol, written here
// rather than taken from the core, and it exists for one reason.
//
// The core represents an address on the wire as a string and rebuilds it on
// decode by parsing that string. When the string carries a name rather than an
// address, the parser resolves it: a blocking lookup, with no context and no
// deadline, run on the receive loop of the peer that sent the message. A
// stranger therefore chose which names this seed looked up, how many, and how
// long each one took, before a single rule of this reactor had run.
//
// Reserving that to peers we asked would have narrowed the door. Closing it
// means never resolving on this channel, which is what this does: a wire
// address is a string until it has been parsed strictly, and a name is
// refused. Nothing legitimate is lost. Every address a node puts on this wire
// is built either from an established connection or from an address that was
// already resolved, so a name reaching us is either a mistake or an attempt.
//
// On this channel, and not in the binary. Both cores parse the address a peer
// announces in its node info during the handshake, before either accept loop
// compares the connection count to its limit, and that parse resolves a name
// the same way. It is upstream code on both stacks, this fork does not make it
// worse, and the only bound within reach here would be a ceiling on concurrent
// inbound connections, which neither stack sets. Said plainly rather than left
// to be read as covered by the paragraph above.
//
// The types below are ours, registered on a codec of our own. The core's codec
// is untouched, and the type names on the wire are the ones the core emits, so
// what any TM2 node sends is what this reads.

// wireMessage is one decoded discovery message.
type wireMessage interface {
	discoveryMessage()
}

// wireRequest is a peer asking for addresses. Empty by design, as upstream.
type wireRequest struct{}

func (*wireRequest) discoveryMessage() {}

// wireResponse is a peer answering with addresses, left as the strings they
// are on the wire. Parsing happens afterwards, under our own rules.
type wireResponse struct {
	Peers []string
}

func (*wireResponse) discoveryMessage() {}

// seedCodec decodes what arrives on the discovery channel.
//
// The proto3 package name and the type names are the core's, since they are
// what identifies a message on the wire. The Go package path is ours and plays
// no part in that identity.
var seedCodec = newSeedCodec()

func newSeedCodec() *amino.Codec {
	codec := amino.NewCodec()

	codec.RegisterPackage(amino.NewPackage(
		"github.com/AviaOne/tenderseed/internal/tenderseed",
		"p2p",
		"",
	).WithTypes(
		&wireRequest{}, "Request",
		&wireResponse{}, "Response",
	))

	return codec
}

// decodeDiscovery reads one discovery message. It never resolves anything.
func decodeDiscovery(payload []byte) (wireMessage, error) {
	var msg wireMessage

	if err := seedCodec.UnmarshalAny(payload, &msg); err != nil {
		return nil, err
	}

	return msg, nil
}

// parseWireAddress turns one wire address into an address, refusing anything
// that is not literal.
func parseWireAddress(raw string) (*p2ptypes.NetAddress, error) {
	parts := strings.Split(raw, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("malformed address %q", raw)
	}

	id := crypto.ID(parts[0])
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("invalid identity in %q, %w", raw, err)
	}

	host, portString, err := net.SplitHostPort(parts[1])
	if err != nil {
		return nil, fmt.Errorf("malformed host and port in %q, %w", raw, err)
	}

	// The whole point of this file. A name would be resolved by whoever parses
	// it, so it is refused here instead.
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("address %q carries a name rather than an IP", raw)
	}

	port, err := strconv.ParseUint(portString, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("malformed port in %q, %w", raw, err)
	}

	addr := p2ptypes.NewNetAddressFromIPPort(ip, uint16(port))
	addr.ID = id

	return addr, nil
}

// parseWireAddresses parses what a peer answered, and reports how many entries
// were refused.
func parseWireAddresses(raw []string) ([]*p2ptypes.NetAddress, int) {
	addrs := make([]*p2ptypes.NetAddress, 0, len(raw))
	refused := 0

	for _, entry := range raw {
		addr, err := parseWireAddress(entry)
		if err != nil {
			refused++

			continue
		}

		addrs = append(addrs, addr)
	}

	return addrs, refused
}
