package service

import (
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/subscription"
)

// ------------------------------------------------------------------
// Nodes
// ------------------------------------------------------------------

// NodeFilters holds query filters for listing nodes.
type NodeFilters struct {
	PlatformID     *string
	SubscriptionID *string
	Enabled        *bool
	Region         *string
	CircuitOpen    *bool
	HasOutbound    *bool
	DestBanActive  *bool // true: at least one active dest ban; false: none
	EgressIP       *string
	ProbedSince    *time.Time
	TagKeyword     *string
}

// ListNodes returns nodes from the pool with optional filters.
func (s *ControlPlaneService) ListNodes(filters NodeFilters) ([]NodeSummary, error) {
	var subLookup node.SubLookupFunc
	if s != nil && s.Pool != nil {
		subLookup = s.Pool.MakeSubLookup()
	}

	// If platform_id filter, get the platform view.
	var platformView map[node.Hash]struct{}
	if filters.PlatformID != nil {
		plat, ok := s.Pool.GetPlatform(*filters.PlatformID)
		if !ok {
			return nil, notFound("platform not found")
		}
		platformView = make(map[node.Hash]struct{}, plat.View().Size())
		plat.View().Range(func(h node.Hash) bool {
			platformView[h] = struct{}{}
			return true
		})
	}

	var subNodes map[node.Hash]struct{}
	if filters.SubscriptionID != nil {
		sub := s.SubMgr.Lookup(*filters.SubscriptionID)
		if sub == nil {
			return nil, notFound("subscription not found")
		}
		subNodes = make(map[node.Hash]struct{})
		sub.ManagedNodes().RangeNodes(func(h node.Hash, managed subscription.ManagedNode) bool {
			if managed.Evicted {
				return true
			}
			subNodes[h] = struct{}{}
			return true
		})
	}

	var result []NodeSummary
	appendIfMatched := func(h node.Hash, entry *node.NodeEntry) {
		if !s.nodeEntryMatchesFilters(entry, filters, subLookup) {
			return
		}
		result = append(result, s.nodeEntryToSummary(h, entry))
	}

	appendIfMatchedHash := func(h node.Hash) {
		entry, ok := s.Pool.GetEntry(h)
		if !ok {
			return
		}
		appendIfMatched(h, entry)
	}

	switch {
	case platformView != nil && subNodes != nil:
		// Iterate the smaller candidate set, then intersect by membership.
		if len(platformView) <= len(subNodes) {
			for h := range platformView {
				if _, ok := subNodes[h]; !ok {
					continue
				}
				appendIfMatchedHash(h)
			}
		} else {
			for h := range subNodes {
				if _, ok := platformView[h]; !ok {
					continue
				}
				appendIfMatchedHash(h)
			}
		}
	case platformView != nil:
		for h := range platformView {
			appendIfMatchedHash(h)
		}
	case subNodes != nil:
		for h := range subNodes {
			appendIfMatchedHash(h)
		}
	default:
		s.Pool.Range(func(h node.Hash, entry *node.NodeEntry) bool {
			appendIfMatched(h, entry)
			return true
		})
	}

	if result == nil {
		result = []NodeSummary{}
	}
	return result, nil
}

func (s *ControlPlaneService) nodeEntryMatchesFilters(
	entry *node.NodeEntry,
	filters NodeFilters,
	subLookup node.SubLookupFunc,
) bool {
	// Enabled/disabled filter.
	if filters.Enabled != nil {
		enabled := true
		if subLookup != nil {
			enabled = entry.HasEnabledSubscription(subLookup)
		}
		if enabled != *filters.Enabled {
			return false
		}
	}

	// Node tag fuzzy search filter.
	if filters.TagKeyword != nil {
		keyword := strings.ToLower(strings.TrimSpace(*filters.TagKeyword))
		if keyword != "" {
			matched := false
			for _, subID := range entry.SubscriptionIDs() {
				sub := s.SubMgr.Lookup(subID)
				if sub == nil {
					continue
				}
				managed, ok := sub.ManagedNodes().LoadNode(entry.Hash)
				if !ok {
					continue
				}
				tags := managed.Tags
				for _, tag := range tags {
					displayTag := sub.Name() + "/" + tag
					if strings.Contains(strings.ToLower(displayTag), keyword) {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
			if !matched {
				return false
			}
		}
	}

	// Region filter.
	if filters.Region != nil {
		region := entry.GetRegion(nil)
		if s.GeoIP != nil {
			region = entry.GetRegion(s.GeoIP.Lookup)
		}
		if region == "" || region != *filters.Region {
			return false
		}
	}
	// Circuit open filter.
	if filters.CircuitOpen != nil {
		if entry.IsCircuitOpen() != *filters.CircuitOpen {
			return false
		}
	}
	// Has outbound filter.
	if filters.HasOutbound != nil {
		if entry.HasOutbound() != *filters.HasOutbound {
			return false
		}
	}
	// Dest-ban active filter.
	if filters.DestBanActive != nil {
		hasActive := entry.ActiveDestBanCount() > 0
		if hasActive != *filters.DestBanActive {
			return false
		}
	}
	// Egress IP filter.
	if filters.EgressIP != nil {
		egressIP := entry.GetEgressIP()
		if !egressIP.IsValid() || egressIP.String() != *filters.EgressIP {
			return false
		}
	}
	// Probed since filter.
	if filters.ProbedSince != nil {
		lastUpdate := entry.LastLatencyProbeAttempt.Load()
		if lastUpdate < filters.ProbedSince.UnixNano() {
			return false
		}
	}
	return true
}

// GetNode returns a single node by hash.
func (s *ControlPlaneService) GetNode(hashStr string) (*NodeSummary, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	entry, ok := s.Pool.GetEntry(h)
	if !ok {
		return nil, notFound("node not found")
	}
	ns := s.nodeEntryToSummary(h, entry)
	return &ns, nil
}

// ProbeEgress triggers a synchronous egress probe and returns results.
func (s *ControlPlaneService) ProbeEgress(hashStr string) (*probe.EgressProbeResult, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	entry, ok := s.Pool.GetEntry(h)
	if !ok {
		return nil, notFound("node not found")
	}
	result, err := s.ProbeMgr.ProbeEgressSync(h)
	if err != nil {
		return nil, internal("egress probe failed", err)
	}
	result.Region = entry.GetRegion(nil)
	if s.GeoIP != nil {
		result.Region = entry.GetRegion(s.GeoIP.Lookup)
	}
	return result, nil
}

// ProbeLatency triggers a synchronous latency probe and returns results.
func (s *ControlPlaneService) ProbeLatency(hashStr string) (*probe.LatencyProbeResult, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	if _, ok := s.Pool.GetEntry(h); !ok {
		return nil, notFound("node not found")
	}
	result, err := s.ProbeMgr.ProbeLatencySync(h)
	if err != nil {
		return nil, internal("latency probe failed", err)
	}
	return result, nil
}

// DestBanItem is one dest-ban entry exposed by the control plane API.
type DestBanItem struct {
	Domain      string  `json:"domain"`
	FailCount   uint32  `json:"fail_count"`
	Active      bool    `json:"active"`
	BannedUntil *string `json:"banned_until,omitempty"`
	LastError   string  `json:"last_error,omitempty"`
	LastFailAt  *string `json:"last_fail_at,omitempty"`
}

// DestBanListResponse is the GET dest-bans payload.
type DestBanListResponse struct {
	NodeHash string        `json:"node_hash"`
	Items    []DestBanItem `json:"items"`
}

// CreateDestBanRequest is the POST dest-bans body.
type CreateDestBanRequest struct {
	Domain string `json:"domain"`
	TTL    string `json:"ttl,omitempty"` // Go duration, e.g. "15m"; empty = runtime default
}

// ListNodeDestBans returns dest-ban entries for a node.
func (s *ControlPlaneService) ListNodeDestBans(hashStr string) (*DestBanListResponse, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	snaps, ok := s.Pool.ListDestBans(h)
	if !ok {
		return nil, notFound("node not found")
	}
	items := make([]DestBanItem, 0, len(snaps))
	for _, snap := range snaps {
		items = append(items, destBanSnapshotToItem(snap))
	}
	return &DestBanListResponse{NodeHash: h.Hex(), Items: items}, nil
}

// CreateNodeDestBan manually soft-bans a domain on a node.
func (s *ControlPlaneService) CreateNodeDestBan(hashStr string, req CreateDestBanRequest) (*DestBanItem, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	domain := normalizeDestBanDomain(req.Domain)
	if domain == "" {
		return nil, invalidArg("domain: required")
	}
	var ttl time.Duration
	if strings.TrimSpace(req.TTL) != "" {
		parsed, perr := time.ParseDuration(strings.TrimSpace(req.TTL))
		if perr != nil || parsed <= 0 {
			return nil, invalidArg("ttl: must be a positive Go duration (e.g. 15m)")
		}
		ttl = parsed
	}
	if !s.Pool.SetDestBan(h, domain, ttl) {
		return nil, notFound("node not found")
	}
	snaps, ok := s.Pool.ListDestBans(h)
	if !ok {
		return nil, notFound("node not found")
	}
	for _, snap := range snaps {
		if snap.Domain == domain {
			item := destBanSnapshotToItem(snap)
			return &item, nil
		}
	}
	return &DestBanItem{Domain: domain, Active: true, FailCount: 1}, nil
}

// DeleteNodeDestBan clears dest-ban state for domain on a node.
func (s *ControlPlaneService) DeleteNodeDestBan(hashStr, domainRaw string) error {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return invalidArg("node_hash: invalid format")
	}
	domain := normalizeDestBanDomain(domainRaw)
	if domain == "" {
		return invalidArg("domain: required")
	}
	cleared, nodeOK := s.Pool.ClearDestBan(h, domain)
	if !nodeOK {
		return notFound("node not found")
	}
	if !cleared {
		return notFound("dest ban not found")
	}
	return nil
}

func normalizeDestBanDomain(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	// Prefer eTLD+1 so manual bans match routing ExtractDomain keys.
	if d := netutil.ExtractDomain(raw); d != "" {
		return d
	}
	return strings.ToLower(raw)
}

func destBanSnapshotToItem(snap node.DestBanSnapshot) DestBanItem {
	item := DestBanItem{
		Domain:    snap.Domain,
		FailCount: snap.Entry.FailCount,
		Active:    snap.Active,
		LastError: snap.Entry.LastError,
	}
	if snap.Entry.BannedUntil > 0 {
		t := time.Unix(0, snap.Entry.BannedUntil).UTC().Format(time.RFC3339Nano)
		item.BannedUntil = &t
	}
	if snap.Entry.LastFailAt > 0 {
		t := time.Unix(0, snap.Entry.LastFailAt).UTC().Format(time.RFC3339Nano)
		item.LastFailAt = &t
	}
	return item
}
