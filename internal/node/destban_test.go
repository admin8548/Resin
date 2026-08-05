package node

import (
	"testing"
	"time"
)

func TestDestBanTable_ThresholdAndClear(t *testing.T) {
	tb := NewDestBanTable(8)
	domain := "x.ai"

	tb.Record(domain, false, 2, time.Minute)
	if tb.IsBanned(domain) {
		t.Fatal("not banned after 1 failure with threshold=2")
	}
	tb.Record(domain, false, 2, time.Minute)
	if !tb.IsBanned(domain) {
		t.Fatal("expected ban after 2 failures")
	}

	tb.Record(domain, true, 2, time.Minute)
	if tb.IsBanned(domain) {
		t.Fatal("success should clear ban")
	}
}

func TestDestBanTable_TTLExpiry(t *testing.T) {
	tb := NewDestBanTable(4)
	domain := "grok.com"
	tb.Record(domain, false, 1, time.Millisecond)
	if !tb.IsBanned(domain) {
		t.Fatal("expected immediate ban")
	}
	time.Sleep(5 * time.Millisecond)
	if tb.IsBanned(domain) {
		t.Fatal("expected ban to expire")
	}
}

func TestDestBanTable_PerDomainIsolation(t *testing.T) {
	tb := NewDestBanTable(8)
	tb.Record("x.ai", false, 1, time.Minute)
	if !tb.IsBanned("x.ai") {
		t.Fatal("x.ai should be banned")
	}
	if tb.IsBanned("google.com") {
		t.Fatal("google.com must not be banned")
	}
}

func TestNodeEntry_IsDestBanned(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0, 16)
	if e.IsDestBanned("x.ai") {
		t.Fatal("fresh entry should not be banned")
	}
	e.RecordDestResult("x.ai", false, 1, time.Minute)
	if !e.IsDestBanned("x.ai") {
		t.Fatal("expected ban after threshold=1 failure")
	}
}
