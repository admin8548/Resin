package node

import (
	"sync"
	"time"
)

// DestBanEntry holds the soft-ban state for a single domain on a node.
type DestBanEntry struct {
	FailCount   uint32
	BannedUntil int64 // unix nano; 0 means not banned
	LastError   string
	LastFailAt  int64
}

type destBanSlot struct {
	key          uint64
	domain       string
	entry        DestBanEntry
	occupied     bool
	lastAccessNs int64
}

// DestBanTable is a bounded LRU-style table of dest bans per domain.
// Pattern mirrors LatencyTable: fixed slots + least-recently-accessed eviction.
type DestBanTable struct {
	mu         sync.Mutex
	slots      []destBanSlot
	maxEntries int
}

// NewDestBanTable creates a DestBanTable bounded to maxEntries.
// Pass 0 or negative to skip initialization (returns nil).
func NewDestBanTable(maxEntries int) *DestBanTable {
	if maxEntries <= 0 {
		return nil
	}
	return &DestBanTable{
		slots:      make([]destBanSlot, maxEntries),
		maxEntries: maxEntries,
	}
}

// Record records a passive result for domain.
// On success: clears the entry.
// On failure: increments FailCount; when FailCount >= threshold, sets BannedUntil = now+ttl.
// When full, least-recently-accessed entry is evicted.
// // safe for concurrent calls
func (t *DestBanTable) Record(domain string, success bool, threshold int, ttl time.Duration) {
	if t == nil || domain == "" {
		return
	}
	if threshold <= 0 {
		threshold = 1
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}

	key := domainKey(domain)
	nowNs := time.Now().UnixNano()

	t.mu.Lock()
	defer t.mu.Unlock()

	if idx, found := t.findLocked(key); found {
		if success {
			t.slots[idx].occupied = false
			return
		}
		t.applyFailureLocked(idx, domain, nowNs, threshold, ttl)
		return
	}

	if success {
		return // nothing to clear
	}

	// Insert or evict.
	idx := t.pickSlotLocked()
	t.slots[idx] = destBanSlot{
		key:          key,
		domain:       domain,
		occupied:     true,
		lastAccessNs: nowNs,
	}
	t.applyFailureLocked(idx, domain, nowNs, threshold, ttl)
}

// IsBanned reports whether domain is currently soft-banned.
// Expired entries are cleared lazily.
// // safe for concurrent calls
func (t *DestBanTable) IsBanned(domain string) bool {
	if t == nil || domain == "" {
		return false
	}
	key := domainKey(domain)
	nowNs := time.Now().UnixNano()

	t.mu.Lock()
	defer t.mu.Unlock()

	idx, found := t.findLocked(key)
	if !found {
		return false
	}
	until := t.slots[idx].entry.BannedUntil
	if until > nowNs {
		t.slots[idx].lastAccessNs = nowNs
		return true
	}
	// until==0: counting failures, not yet banned — keep slot.
	// until>0 && until<=now: ban expired — free slot.
	if until > 0 {
		t.slots[idx].occupied = false
	}
	return false
}

// Size returns the number of occupied slots.
// // safe for concurrent calls
func (t *DestBanTable) Size() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for i := range t.slots {
		if t.slots[i].occupied {
			n++
		}
	}
	return n
}

// ActiveBanCount returns how many entries are currently soft-banned (not expired).
// // safe for concurrent calls
func (t *DestBanTable) ActiveBanCount() int {
	if t == nil {
		return 0
	}
	nowNs := time.Now().UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for i := range t.slots {
		if !t.slots[i].occupied {
			continue
		}
		until := t.slots[i].entry.BannedUntil
		if until > nowNs {
			n++
			continue
		}
		if until > 0 {
			t.slots[i].occupied = false
		}
	}
	return n
}

// List returns a snapshot of occupied entries. Expired bans are dropped.
// // safe for concurrent calls
func (t *DestBanTable) List() []DestBanSnapshot {
	if t == nil {
		return nil
	}
	nowNs := time.Now().UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]DestBanSnapshot, 0, 8)
	for i := range t.slots {
		if !t.slots[i].occupied {
			continue
		}
		e := t.slots[i].entry
		if e.BannedUntil > 0 && e.BannedUntil <= nowNs {
			t.slots[i].occupied = false
			continue
		}
		out = append(out, DestBanSnapshot{
			Domain:       t.slots[i].domain,
			Entry:        e,
			Active:       e.BannedUntil > nowNs,
			LastAccessNs: t.slots[i].lastAccessNs,
		})
	}
	return out
}

// DestBanSnapshot is a read-only view of one dest-ban slot.
type DestBanSnapshot struct {
	Domain       string
	Entry        DestBanEntry
	Active       bool
	LastAccessNs int64
}

// SetBan forces a soft-ban for domain until now+ttl.
// // safe for concurrent calls
func (t *DestBanTable) SetBan(domain string, ttl time.Duration) {
	if t == nil || domain == "" {
		return
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	key := domainKey(domain)
	nowNs := time.Now().UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	idx, found := t.findLocked(key)
	if !found {
		idx = t.pickSlotLocked()
		t.slots[idx] = destBanSlot{
			key:          key,
			domain:       domain,
			occupied:     true,
			lastAccessNs: nowNs,
		}
	}
	e := &t.slots[idx].entry
	if e.FailCount == 0 {
		e.FailCount = 1
	}
	e.BannedUntil = nowNs + ttl.Nanoseconds()
	e.LastFailAt = nowNs
	if e.LastError == "" {
		e.LastError = "MANUAL_BAN"
	}
	t.slots[idx].domain = domain
	t.slots[idx].lastAccessNs = nowNs
	t.slots[idx].occupied = true
}

// Clear removes the dest-ban / failure counter for domain.
// Returns true when an entry was present.
// // safe for concurrent calls
func (t *DestBanTable) Clear(domain string) bool {
	if t == nil || domain == "" {
		return false
	}
	key := domainKey(domain)
	t.mu.Lock()
	defer t.mu.Unlock()
	idx, found := t.findLocked(key)
	if !found {
		return false
	}
	t.slots[idx].occupied = false
	return true
}

// Range iterates active entries. Returning false from fn stops iteration.
// Snapshot is taken under lock; fn runs under lock too (keep fn lightweight).
// // safe for concurrent calls
func (t *DestBanTable) Range(fn func(domain string, entry DestBanEntry) bool) {
	if t == nil || fn == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range t.slots {
		if !t.slots[i].occupied {
			continue
		}
		if !fn(t.slots[i].domain, t.slots[i].entry) {
			return
		}
	}
}

func (t *DestBanTable) findLocked(key uint64) (int, bool) {
	for i := range t.slots {
		if t.slots[i].occupied && t.slots[i].key == key {
			return i, true
		}
	}
	return -1, false
}

func (t *DestBanTable) pickSlotLocked() int {
	empty := -1
	oldest := -1
	var oldestNs int64
	for i := range t.slots {
		if !t.slots[i].occupied {
			if empty < 0 {
				empty = i
			}
			continue
		}
		if oldest < 0 || t.slots[i].lastAccessNs < oldestNs {
			oldest = i
			oldestNs = t.slots[i].lastAccessNs
		}
	}
	if empty >= 0 {
		return empty
	}
	if oldest >= 0 {
		return oldest
	}
	return 0
}

func (t *DestBanTable) applyFailureLocked(idx int, domain string, nowNs int64, threshold int, ttl time.Duration) {
	e := &t.slots[idx].entry
	e.FailCount++
	e.LastFailAt = nowNs
	e.LastError = "UPSTREAM_CONNECT_FAILED"
	t.slots[idx].domain = domain
	t.slots[idx].lastAccessNs = nowNs
	if int(e.FailCount) >= threshold {
		e.BannedUntil = nowNs + ttl.Nanoseconds()
	}
}
