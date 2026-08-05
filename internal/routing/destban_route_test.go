package routing

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestRandomRoute_HardExcludesDestBanned(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("sub-1", "Sub", "url", true, false)
	subMgr.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 5 },
		MaxDestBanEntries:      32,
	})
	plat := platform.NewPlatform("dbp", "DestBanPlat", nil, nil)
	pool.RegisterPlatform(plat)

	// single node
	raw := json.RawMessage(`{"type":"ss","server":"1.1.1.1","port":1,"n":"ban-route"}`)
	h := node.HashFromRawOptions(raw)
	sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"tag"}})
	pool.AddNodeFromSub(h, raw, "sub-1")
	entry, _ := pool.GetEntry(h)
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	entry.LatencyTable.LoadEntry("gstatic.com", node.DomainLatencyStats{
		Ewma: 50 * time.Millisecond, LastUpdated: time.Now(),
	})
	pool.RecordResult(h, true)
	pool.RebuildAllPlatforms()

	// ban target domain
	entry.RecordDestResult("x.ai", false, 1, time.Hour)
	if !entry.IsDestBanned("x.ai") {
		t.Fatal("setup: expected dest ban")
	}

	stats := NewIPLoadStats()
	_, err := randomRoute(plat, stats, pool, "x.ai", nil, time.Minute)
	if err != ErrNoNodeForDest {
		t.Fatalf("want ErrNoNodeForDest, got %v", err)
	}

	// other domain still routable
	got, err := randomRoute(plat, stats, pool, "google.com", nil, time.Minute)
	if err != nil {
		t.Fatalf("other domain should route: %v", err)
	}
	if got != h {
		t.Fatalf("got %s want %s", got.Hex(), h.Hex())
	}
}

func TestStickyLeaseHit_RejectsDestBanned(t *testing.T) {
	basePool := newRouterTestPool()
	plat := platform.NewPlatform("plat-sticky-ban", "StickyBan", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	basePool.addPlatform(plat)

	h1, e1 := newRoutableEntry(t, `{"id":"sticky-ban-1"}`, "203.0.113.10")
	h2, e2 := newRoutableEntry(t, `{"id":"sticky-ban-2"}`, "203.0.113.20")
	basePool.addEntry(h1, e1)
	basePool.addEntry(h2, e2)
	basePool.rebuildPlatformView(plat)

	r := newTestRouter(basePool, nil)
	res1, err := r.RouteRequest(plat.Name, "acct-ban", "example.com")
	if err != nil {
		t.Fatalf("first route: %v", err)
	}
	if !res1.LeaseCreated {
		t.Fatal("expected lease create")
	}

	// Ban the leased node for the sticky target domain.
	leased, ok := basePool.GetEntry(res1.NodeHash)
	if !ok {
		t.Fatal("missing leased entry")
	}
	leased.RecordDestResult("example.com", false, 1, time.Hour)
	if !leased.IsDestBanned("example.com") {
		t.Fatal("setup: expected dest ban on leased node")
	}

	res2, err := r.RouteRequest(plat.Name, "acct-ban", "example.com")
	if err != nil {
		t.Fatalf("second route after ban: %v", err)
	}
	if res2.NodeHash == res1.NodeHash {
		t.Fatalf("sticky hit should reject dest-banned node; still on %s", res2.NodeHash.Hex())
	}
	if res2.NodeHash != h1 && res2.NodeHash != h2 {
		t.Fatalf("unexpected node %s", res2.NodeHash.Hex())
	}
}

func TestChooseSameIPRotation_SkipsDestBanned(t *testing.T) {
	basePool := newRouterTestPool()
	plat := platform.NewPlatform("plat-sameip-ban", "SameIPBan", nil, nil)
	basePool.addPlatform(plat)

	ip := "198.51.100.7"
	h1, e1 := newRoutableEntry(t, `{"id":"sameip-ban-1"}`, ip)
	h2, e2 := newRoutableEntry(t, `{"id":"sameip-ban-2"}`, ip)
	basePool.addEntry(h1, e1)
	basePool.addEntry(h2, e2)
	basePool.rebuildPlatformView(plat)

	// Ban h1 for target domain; rotation must pick h2.
	e1.RecordDestResult("x.ai", false, 1, time.Hour)
	if !e1.IsDestBanned("x.ai") {
		t.Fatal("setup ban failed")
	}

	got, ok := chooseSameIPRotationCandidate(
		plat,
		basePool,
		netip.MustParseAddr(ip),
		"x.ai",
		[]string{"cloudflare.com"},
		10*time.Minute,
	)
	if !ok {
		t.Fatal("expected a non-banned same-IP candidate")
	}
	if got != h2 {
		t.Fatalf("want h2 %s, got %s", h2.Hex(), got.Hex())
	}
}
