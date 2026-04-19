package main

import (
	"log"
	"sync"
	"time"
)

type metricsState struct {
	mu sync.Mutex

	totalRequests        uint64
	cacheHits            uint64
	cacheMisses          uint64
	upstreamFetches      uint64
	upstreamStatus2xx    uint64
	upstreamStatus4xx    uint64
	upstreamStatus5xx    uint64
	upstreamStatus429    uint64
	clientCancels        uint64
	upstreamTimeouts     uint64
	throttleWaits        uint64
	throttleWaitDuration time.Duration
	bytesDownloaded      uint64
	bytesServed          uint64
	bytesSaved           uint64
}

type requestSummary struct {
	url             string
	statusCode      int
	cache           string
	source          string
	upstreamStatus  int
	bytesServed     int64
	bytesDownloaded int64
	throttleWait    time.Duration
	errorClass      string
	totalDuration   time.Duration
}

var aggregateMetrics metricsState

func (m *metricsState) recordRequest() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalRequests++
}

func (m *metricsState) recordCacheHit(bytesServed, bytesSaved int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cacheHits++
	m.bytesServed += uint64(bytesServed)
	m.bytesSaved += uint64(bytesSaved)
}

func (m *metricsState) recordCacheMiss() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cacheMisses++
}

func (m *metricsState) recordUpstreamFetch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upstreamFetches++
}

func (m *metricsState) recordUpstreamStatus(statusCode int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case statusCode == 429:
		m.upstreamStatus429++
		m.upstreamStatus4xx++
	case statusCode >= 500:
		m.upstreamStatus5xx++
	case statusCode >= 400:
		m.upstreamStatus4xx++
	case statusCode >= 200:
		m.upstreamStatus2xx++
	}
}

func (m *metricsState) recordClientCancel() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clientCancels++
}

func (m *metricsState) recordUpstreamTimeout() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upstreamTimeouts++
}

func (m *metricsState) recordThrottleWait(wait time.Duration) {
	if wait <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.throttleWaits++
	m.throttleWaitDuration += wait
}

func (m *metricsState) addBytesDownloaded(bytes int64) {
	if bytes <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bytesDownloaded += uint64(bytes)
}

func (m *metricsState) addBytesServed(bytes int64) {
	if bytes <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bytesServed += uint64(bytes)
}

func (m *metricsState) logSnapshot() {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Printf(
		"metrics total_requests=%d cache_hits=%d cache_misses=%d upstream_fetches=%d upstream_2xx=%d upstream_4xx=%d upstream_5xx=%d upstream_429=%d client_cancels=%d upstream_timeouts=%d throttle_waits=%d throttle_wait_total=%s bytes_downloaded=%d bytes_served=%d bytes_saved=%d",
		m.totalRequests,
		m.cacheHits,
		m.cacheMisses,
		m.upstreamFetches,
		m.upstreamStatus2xx,
		m.upstreamStatus4xx,
		m.upstreamStatus5xx,
		m.upstreamStatus429,
		m.clientCancels,
		m.upstreamTimeouts,
		m.throttleWaits,
		m.throttleWaitDuration,
		m.bytesDownloaded,
		m.bytesServed,
		m.bytesSaved,
	)
}

func startMetricsReporter() {
	if metricsInterval <= 0 {
		return
	}

	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()

	for range ticker.C {
		aggregateMetrics.logSnapshot()
	}
}

func logRequestSummary(summary requestSummary) {
	log.Printf(
		"request url=%q status=%d cache=%s source=%s upstream_status=%d bytes_served=%d bytes_downloaded=%d throttle_wait=%s total=%s error=%q",
		summary.url,
		summary.statusCode,
		summary.cache,
		summary.source,
		summary.upstreamStatus,
		summary.bytesServed,
		summary.bytesDownloaded,
		summary.throttleWait,
		summary.totalDuration,
		summary.errorClass,
	)
}
