package tenderseed

import (
	"strings"
	"testing"

	"github.com/gnolang/gno/tm2/pkg/amino"
	"github.com/gnolang/gno/tm2/pkg/p2p/discovery"
	p2ptypes "github.com/gnolang/gno/tm2/pkg/p2p/types"
)

// TestWireDecodesWhatTheCoreEmits is the interoperability test. What is read
// here has to be exactly what any TM2 node sends, or this seed is deaf.
func TestWireDecodesWhatTheCoreEmits(t *testing.T) {
	t.Parallel()

	t.Run("an answer from the core is read whole", func(t *testing.T) {
		t.Parallel()

		first, second := bookAddr(t, 1300), bookAddr(t, 1301)

		payload, err := amino.MarshalAny(&discovery.Response{
			Peers: []*p2ptypes.NetAddress{first, second},
		})
		if err != nil {
			t.Fatalf("unable to marshal the answer: %v", err)
		}

		msg, err := decodeDiscovery(payload)
		if err != nil {
			t.Fatalf("unable to decode the answer: %v", err)
		}

		answer, ok := msg.(*wireResponse)
		if !ok {
			t.Fatalf("decoded %T, expected an answer", msg)
		}

		if len(answer.Peers) != 2 {
			t.Fatalf("the answer carries %d addresses, expected 2", len(answer.Peers))
		}

		addrs, refused := parseWireAddresses(answer.Peers)
		if refused != 0 {
			t.Fatalf("%d addresses were refused, expected none", refused)
		}

		if addrs[0].String() != first.String() || addrs[1].String() != second.String() {
			t.Fatalf("the addresses came back as %s and %s", addrs[0], addrs[1])
		}
	})

	t.Run("a request from the core is read as a request", func(t *testing.T) {
		t.Parallel()

		payload, err := amino.MarshalAny(&discovery.Request{})
		if err != nil {
			t.Fatalf("unable to marshal the request: %v", err)
		}

		msg, err := decodeDiscovery(payload)
		if err != nil {
			t.Fatalf("unable to decode the request: %v", err)
		}

		if _, ok := msg.(*wireRequest); !ok {
			t.Fatalf("decoded %T, expected a request", msg)
		}
	})
}

// TestWireNeverResolves is the regression test of the defect: decoding used to
// look a name up, on the receive loop, at the sender's choosing.
func TestWireNeverResolves(t *testing.T) {
	t.Parallel()

	t.Run("an address carrying a name is refused", func(t *testing.T) {
		t.Parallel()

		// The reserved TLD never resolves, so a lookup would show up as a
		// slow test rather than a failing one; what is pinned here is that
		// the parser says why it refused without ever trying.
		named := strings.Replace(
			bookAddr(t, 1310).String(),
			bookAddr(t, 1310).DialString(),
			"seed.example.invalid:26656",
			1,
		)

		if _, err := parseWireAddress(named); err == nil {
			t.Fatal("an address carrying a name was accepted")
		} else if !strings.Contains(err.Error(), "carries a name") {
			t.Fatalf("refused with %v, expected the name to be the reason", err)
		}
	})

	t.Run("a malformed entry costs only itself", func(t *testing.T) {
		t.Parallel()

		good := bookAddr(t, 1320)

		addrs, refused := parseWireAddresses([]string{
			"not an address",
			good.String(),
			"@:",
		})

		if refused != 2 {
			t.Fatalf("%d entries were refused, expected 2", refused)
		}

		if len(addrs) != 1 || addrs[0].String() != good.String() {
			t.Fatalf("%d addresses were kept, expected the sound one alone", len(addrs))
		}
	})
}
