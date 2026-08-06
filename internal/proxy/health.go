package proxy

import (
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// HealthRecorder abstracts passive health feedback reporting.
// topology.GlobalNodePool satisfies this interface.
type HealthRecorder interface {
	RecordResult(hash node.Hash, success bool)
	RecordLatency(hash node.Hash, rawTarget string, latency *time.Duration)
}

type passiveHealthRecorder interface {
	RecordPassiveResult(platformID string, hash node.Hash, success bool, domain string)
}

// recordPassiveResultAsync reports passive health after a proxy attempt.
// Failures are recorded synchronously so dest-ban / circuit state is visible
// to the next route pick (threshold=1 must take effect before the following request).
// Successes stay async to avoid adding latency on the common path.
// Concurrent in-flight requests that both routed before either finished can still
// double-hit the same node once; that is inherent to route-then-dial.
func recordPassiveResultAsync(health HealthRecorder, route routing.RouteResult, success bool) {
	if health == nil {
		return
	}
	if recorder, ok := health.(passiveHealthRecorder); ok {
		if !success {
			recorder.RecordPassiveResult(route.PlatformID, route.NodeHash, false, route.TargetDomain)
			return
		}
		go recorder.RecordPassiveResult(route.PlatformID, route.NodeHash, true, route.TargetDomain)
		return
	}
	if !success {
		health.RecordResult(route.NodeHash, false)
		return
	}
	go health.RecordResult(route.NodeHash, true)
}
