package config

import "time"

// RuntimeConfig holds all hot-updatable global settings.
// These are persisted in the database and served via GET /system/config.
type RuntimeConfig struct {
	// Request log
	RequestLogEnabled                  bool `json:"request_log_enabled"`
	ReverseProxyLogDetailEnabled       bool `json:"reverse_proxy_log_detail_enabled"`
	ReverseProxyLogReqHeadersMaxBytes  int  `json:"reverse_proxy_log_req_headers_max_bytes"`
	ReverseProxyLogReqBodyMaxBytes     int  `json:"reverse_proxy_log_req_body_max_bytes"`
	ReverseProxyLogRespHeadersMaxBytes int  `json:"reverse_proxy_log_resp_headers_max_bytes"`
	ReverseProxyLogRespBodyMaxBytes    int  `json:"reverse_proxy_log_resp_body_max_bytes"`

	// Health check
	MaxConsecutiveFailures          int      `json:"max_consecutive_failures"`
	MaxLatencyTestInterval          Duration `json:"max_latency_test_interval"`
	MaxAuthorityLatencyTestInterval Duration `json:"max_authority_latency_test_interval"`
	MaxEgressTestInterval           Duration `json:"max_egress_test_interval"`

	// Probe
	LatencyTestURL     string   `json:"latency_test_url"`
	LatencyAuthorities []string `json:"latency_authorities"`

	// P2C
	P2CLatencyWindow   Duration `json:"p2c_latency_window"`
	LatencyDecayWindow Duration `json:"latency_decay_window"`

	// Persistence
	CacheFlushInterval       Duration `json:"cache_flush_interval"`
	CacheFlushDirtyThreshold int      `json:"cache_flush_dirty_threshold"`

	// Dest-aware soft ban (node × domain)
	DestBanEnabled    bool     `json:"dest_ban_enabled"`
	DestBanThreshold  int      `json:"dest_ban_threshold"`
	DestBanTTL        Duration `json:"dest_ban_ttl"`
	DestBanScope      string   `json:"dest_ban_scope"` // "etld1" or "host"
	DestBanMaxEntries int      `json:"dest_ban_max_entries"`
}

// NewDefaultRuntimeConfig returns a RuntimeConfig populated with the default
// values specified in DESIGN.md §运行时全局设置项.
func NewDefaultRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		RequestLogEnabled:                  true,
		ReverseProxyLogDetailEnabled:       false,
		ReverseProxyLogReqHeadersMaxBytes:  4096,
		ReverseProxyLogReqBodyMaxBytes:     1024,
		ReverseProxyLogRespHeadersMaxBytes: 1024,
		ReverseProxyLogRespBodyMaxBytes:    1024,

		MaxConsecutiveFailures:          3,
		MaxLatencyTestInterval:          Duration(1 * time.Hour),
		MaxAuthorityLatencyTestInterval: Duration(3 * time.Hour),
		MaxEgressTestInterval:           Duration(24 * time.Hour),

		LatencyTestURL:     "https://www.gstatic.com/generate_204",
		LatencyAuthorities: []string{"gstatic.com", "google.com", "cloudflare.com", "github.com"},

		P2CLatencyWindow:   Duration(10 * time.Minute),
		LatencyDecayWindow: Duration(10 * time.Minute),

		CacheFlushInterval:       Duration(5 * time.Minute),
		CacheFlushDirtyThreshold: 1000,

		DestBanEnabled:   true,
		DestBanThreshold: 2,
		// 7d: (node × dest) can stay broken for days; short TTLs thrash routing on permanent fails.
		DestBanTTL:        Duration(7 * 24 * time.Hour),
		DestBanScope:      "etld1",
		DestBanMaxEntries: 500,
	}
}

// NormalizeDestBanDefaults fills zero-value dest-ban fields with defaults.
// Used after loading persisted configs that predate dest-ban fields.
func (c *RuntimeConfig) NormalizeDestBanDefaults() {
	if c == nil {
		return
	}
	// Old configs lack all dest-ban fields (zero values) → enable with defaults.
	neverConfigured := !c.DestBanEnabled &&
		c.DestBanThreshold == 0 &&
		c.DestBanTTL == 0 &&
		c.DestBanMaxEntries == 0 &&
		c.DestBanScope == ""
	if neverConfigured {
		c.DestBanEnabled = true
	}
	if c.DestBanThreshold <= 0 {
		c.DestBanThreshold = 2
	}
	if c.DestBanTTL <= 0 {
		c.DestBanTTL = Duration(7 * 24 * time.Hour)
	}
	if c.DestBanMaxEntries <= 0 {
		c.DestBanMaxEntries = 500
	}
	if c.DestBanScope == "" {
		c.DestBanScope = "etld1"
	}
}
