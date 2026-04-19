package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestServeFromLegacyCache(t *testing.T) {
	testURL := "http://example.com/download/file.zip"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: "example.com/download", MinValidSize: 1},
	})
	defer restore()

	cacheFile, err := cacheBodyFilename(testURL, false)
	if err != nil {
		t.Fatalf("cacheBodyFilename failed: %v", err)
	}
	body := []byte(strings.Repeat("a", 128))
	if err := os.WriteFile(cacheFile, body, 0644); err != nil {
		t.Fatalf("write cache body failed: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
	req.RemoteAddr = "127.0.0.1:12345"

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != string(body) {
		t.Fatalf("unexpected body: %q", rec.Body.String())
	}
	if got := rec.Header().Get("Content-Length"); got != "128" {
		t.Fatalf("expected content length 128, got %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "" {
		t.Fatalf("expected empty content type for legacy cache, got %q", got)
	}
}

func TestFetchCachesAndPreservesHeaders(t *testing.T) {
	body := strings.Repeat("b", 256)
	lastModified := time.Now().UTC().Format(http.TimeFormat)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="mod.zip"`)
		w.Header().Set("ETag", `"etag-123"`)
		w.Header().Set("Last-Modified", lastModified)
		w.Header().Set("Cache-Control", "public, max-age=60")
		_, _ = w.Write([]byte(body))
	}))
	defer upstream.Close()

	testURL := upstream.URL + "/download"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: strings.TrimPrefix(upstream.URL, "http://") + "/download", MinValidSize: 1},
	})
	defer restore()
	upstreamClient = upstream.Client()

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
	firstReq.RemoteAddr = "127.0.0.1:12345"
	handler(first, firstReq)

	if first.Code != http.StatusOK {
		t.Fatalf("expected first response 200, got %d", first.Code)
	}
	assertDownloadHeaders(t, first.Header(), int64(len(body)), lastModified)

	cacheFile, err := cacheBodyFilename(testURL, false)
	if err != nil {
		t.Fatalf("cacheBodyFilename failed: %v", err)
	}
	metaData, err := os.ReadFile(cacheMetadataFilename(cacheFile))
	if err != nil {
		t.Fatalf("expected metadata sidecar: %v", err)
	}

	var meta cacheMetadata
	if err := json.Unmarshal(metaData, &meta); err != nil {
		t.Fatalf("invalid metadata json: %v", err)
	}
	if meta.Headers.Get("ETag") != `"etag-123"` {
		t.Fatalf("expected cached etag, got %q", meta.Headers.Get("ETag"))
	}

	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
	secondReq.RemoteAddr = "127.0.0.1:12345"
	handler(second, secondReq)

	if second.Code != http.StatusOK {
		t.Fatalf("expected cached response 200, got %d", second.Code)
	}
	if second.Body.String() != body {
		t.Fatalf("unexpected cached body")
	}
	assertDownloadHeaders(t, second.Header(), int64(len(body)), lastModified)
}

func TestUpstreamStatusMapping(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/403":
			http.Error(w, "forbidden", http.StatusForbidden)
		case "/status/404":
			http.Error(w, "missing", http.StatusNotFound)
		case "/status/429":
			http.Error(w, "slow down", http.StatusTooManyRequests)
		default:
			http.Error(w, "boom", http.StatusInternalServerError)
		}
	}))
	defer upstream.Close()

	restore := configureTestRuntime(t, []safeInfo{
		{URL: strings.TrimPrefix(upstream.URL, "http://") + "/status", MinValidSize: 1},
	})
	defer restore()
	upstreamClient = upstream.Client()

	testCases := []struct {
		path   string
		status int
	}{
		{path: "/status/403", status: http.StatusForbidden},
		{path: "/status/404", status: http.StatusNotFound},
		{path: "/status/429", status: http.StatusTooManyRequests},
		{path: "/status/500", status: http.StatusBadGateway},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(upstream.URL+tc.path), nil)
			req.RemoteAddr = "127.0.0.1:12345"
			handler(rec, req)

			if rec.Code != tc.status {
				t.Fatalf("expected %d, got %d", tc.status, rec.Code)
			}
		})
	}
}

func TestFetchTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(strings.Repeat("x", 128)))
	}))
	defer upstream.Close()

	restore := configureTestRuntime(t, []safeInfo{
		{URL: strings.TrimPrefix(upstream.URL, "http://") + "/slow", MinValidSize: 1},
	})
	defer restore()
	upstreamClient = upstream.Client()
	fetchTimeout = 20 * time.Millisecond

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(upstream.URL+"/slow"), nil)
	req.RemoteAddr = "127.0.0.1:12345"
	handler(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504, got %d", rec.Code)
	}
}

func TestSafeJoinRejectsSiblingPrefixEscape(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "cache")
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	if _, err := safeJoin(baseDir, filepath.Join("..", filepath.Base(baseDir)+"-evil", "file.cache")); err == nil {
		t.Fatalf("expected safeJoin to reject sibling-prefix escape")
	}
}

func TestSavedBytesHelpersAndFlush(t *testing.T) {
	restore := configureTestRuntime(t, []safeInfo{})
	defer restore()

	setSavedBytes(25)
	addSavedBytes(100)

	savedBytes, dirty := snapshotSavedBytes()
	if savedBytes != 125 || !dirty {
		t.Fatalf("expected saved bytes=125 dirty=true, got %d dirty=%v", savedBytes, dirty)
	}

	writeSavedFile()

	data, err := os.ReadFile(bytesSavedFilename)
	if err != nil {
		t.Fatalf("read saved file failed: %v", err)
	}
	if string(data) != "125" {
		t.Fatalf("expected saved file to contain 125, got %q", string(data))
	}

	savedBytes, dirty = snapshotSavedBytes()
	if savedBytes != 125 || dirty {
		t.Fatalf("expected saved bytes=125 dirty=false after flush, got %d dirty=%v", savedBytes, dirty)
	}
}

func TestMalformedCacheMetadataFallsBackToRefetch(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte(strings.Repeat("n", 128)))
	}))
	defer upstream.Close()

	testURL := upstream.URL + "/download"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: strings.TrimPrefix(upstream.URL, "http://") + "/download", MinValidSize: 1},
	})
	defer restore()
	upstreamClient = upstream.Client()

	cacheFile, err := cacheBodyFilename(testURL, false)
	if err != nil {
		t.Fatalf("cacheBodyFilename failed: %v", err)
	}
	if err := os.WriteFile(cacheFile, []byte(strings.Repeat("o", 128)), 0644); err != nil {
		t.Fatalf("write cache body failed: %v", err)
	}
	if err := os.WriteFile(cacheMetadataFilename(cacheFile), []byte("{invalid"), 0644); err != nil {
		t.Fatalf("write cache metadata failed: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
	req.RemoteAddr = "127.0.0.1:12345"
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != strings.Repeat("n", 128) {
		t.Fatalf("expected refetched body, got %q", rec.Body.String())
	}

	metaData, err := os.ReadFile(cacheMetadataFilename(cacheFile))
	if err != nil {
		t.Fatalf("expected refreshed metadata sidecar: %v", err)
	}
	var meta cacheMetadata
	if err := json.Unmarshal(metaData, &meta); err != nil {
		t.Fatalf("expected valid refreshed metadata: %v", err)
	}
}

func TestContentLengthMismatchDoesNotCache(t *testing.T) {
	testURL := "http://example.com/download/truncated.zip"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: "example.com/download", MinValidSize: 1},
	})
	defer restore()
	upstreamClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusOK,
				Status:        "200 OK",
				Header:        make(http.Header),
				Body:          io.NopCloser(strings.NewReader(strings.Repeat("t", 100))),
				ContentLength: 200,
				Request:       req,
			}, nil
		}),
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
	req.RemoteAddr = "127.0.0.1:12345"
	handler(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", rec.Code)
	}

	cacheFile, err := cacheBodyFilename(testURL, false)
	if err != nil {
		t.Fatalf("cacheBodyFilename failed: %v", err)
	}
	if _, err := os.Stat(cacheFile); !os.IsNotExist(err) {
		t.Fatalf("expected no cache body, stat err=%v", err)
	}
	if _, err := os.Stat(cacheMetadataFilename(cacheFile)); !os.IsNotExist(err) {
		t.Fatalf("expected no cache metadata, stat err=%v", err)
	}
}

func TestSmallResponsesAreNotCached(t *testing.T) {
	tests := []struct {
		name       string
		bodyLen    int
		minURLSize int
	}{
		{name: "below_min_cache_size", bodyLen: minCacheSize - 1, minURLSize: 1},
		{name: "below_min_url_size", bodyLen: minCacheSize + 50, minURLSize: minCacheSize + 200},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/octet-stream")
				_, _ = w.Write([]byte(strings.Repeat("s", tc.bodyLen)))
			}))
			defer upstream.Close()

			testURL := upstream.URL + "/download"
			restore := configureTestRuntime(t, []safeInfo{
				{URL: strings.TrimPrefix(upstream.URL, "http://") + "/download", MinValidSize: tc.minURLSize},
			})
			defer restore()
			upstreamClient = upstream.Client()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
			req.RemoteAddr = "127.0.0.1:12345"
			handler(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", rec.Code)
			}

			cacheFile, err := cacheBodyFilename(testURL, false)
			if err != nil {
				t.Fatalf("cacheBodyFilename failed: %v", err)
			}
			if _, err := os.Stat(cacheFile); !os.IsNotExist(err) {
				t.Fatalf("expected no cache body, stat err=%v", err)
			}
			if _, err := os.Stat(cacheMetadataFilename(cacheFile)); !os.IsNotExist(err) {
				t.Fatalf("expected no cache metadata, stat err=%v", err)
			}
		})
	}
}

func TestLongTermCacheRoutesToLongCacheDir(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("l", 128)))
	}))
	defer upstream.Close()

	testURL := upstream.URL + "/download"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: strings.TrimPrefix(upstream.URL, "http://") + "/download", longTermCache: true, MinValidSize: 1},
	})
	defer restore()
	upstreamClient = upstream.Client()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
	req.RemoteAddr = "127.0.0.1:12345"
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	longCacheFile, err := cacheBodyFilename(testURL, true)
	if err != nil {
		t.Fatalf("cacheBodyFilename failed: %v", err)
	}
	if _, err := os.Stat(longCacheFile); err != nil {
		t.Fatalf("expected long cache body, got err=%v", err)
	}
	if _, err := os.Stat(cacheMetadataFilename(longCacheFile)); err != nil {
		t.Fatalf("expected long cache metadata, got err=%v", err)
	}

	shortCacheFile, err := cacheBodyFilename(testURL, false)
	if err != nil {
		t.Fatalf("cacheBodyFilename failed: %v", err)
	}
	if _, err := os.Stat(shortCacheFile); !os.IsNotExist(err) {
		t.Fatalf("expected no short cache body, stat err=%v", err)
	}
}

func TestHeaderAllowlistExcludesUnknownHeadersFromCache(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Uncached-Test", "secret")
		_, _ = w.Write([]byte(strings.Repeat("h", 128)))
	}))
	defer upstream.Close()

	testURL := upstream.URL + "/download"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: strings.TrimPrefix(upstream.URL, "http://") + "/download", MinValidSize: 1},
	})
	defer restore()
	upstreamClient = upstream.Client()

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
	firstReq.RemoteAddr = "127.0.0.1:12345"
	handler(first, firstReq)

	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil)
	secondReq.RemoteAddr = "127.0.0.1:12345"
	handler(second, secondReq)

	if got := second.Header().Get("X-Uncached-Test"); got != "" {
		t.Fatalf("expected unallowlisted header to be dropped, got %q", got)
	}
}

func TestClientCanceledBeforeThrottleSlot(t *testing.T) {
	testURL := "http://example.com/download/file.zip"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: "example.com/download", MinValidSize: 1},
	})
	defer restore()
	upstreamLimiter = rate.NewLimiter(rate.Every(time.Hour), 1)
	_ = upstreamLimiter.Allow()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL), nil).WithContext(ctx)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusRequestTimeout {
		t.Fatalf("expected 408, got %d", rec.Code)
	}

	aggregateMetrics.mu.Lock()
	defer aggregateMetrics.mu.Unlock()
	if aggregateMetrics.clientCancels != 1 {
		t.Fatalf("expected 1 client cancel, got %d", aggregateMetrics.clientCancels)
	}
}

func TestClientCancelWhileServingCachedResponseKeepsCache(t *testing.T) {
	testURL := "http://example.com/download/file.zip"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: "example.com/download", MinValidSize: 1},
	})
	defer restore()

	cacheFile, err := cacheBodyFilename(testURL, false)
	if err != nil {
		t.Fatalf("cacheBodyFilename failed: %v", err)
	}
	body := []byte(strings.Repeat("c", 256))
	meta := cacheMetadata{
		Version:       cacheFormatVersion,
		Status:        http.StatusOK,
		Headers:       http.Header{"Content-Type": []string{"application/octet-stream"}},
		ContentLength: int64(len(body)),
	}
	if err := os.WriteFile(cacheFile, body, 0644); err != nil {
		t.Fatalf("write cache body failed: %v", err)
	}
	if err := writeCacheMetadata(cacheMetadataFilename(cacheFile), meta); err != nil {
		t.Fatalf("write cache metadata failed: %v", err)
	}

	summary := requestSummary{}
	err = serveFromCache(&failingWriter{
		header:    make(http.Header),
		failAfter: 64,
	}, redactQuery(testURL), cacheFile, &summary)
	if err != nil {
		t.Fatalf("expected nil error on client cancel, got %v", err)
	}
	if summary.errorClass != "client_canceled" {
		t.Fatalf("expected client_canceled summary, got %q", summary.errorClass)
	}
	if _, err := os.Stat(cacheFile); err != nil {
		t.Fatalf("expected cache body to remain, got err=%v", err)
	}
	if _, err := os.Stat(cacheMetadataFilename(cacheFile)); err != nil {
		t.Fatalf("expected cache metadata to remain, got err=%v", err)
	}
}

func TestCleanupRemovesExpiredBodyAndMetadataAndOrphans(t *testing.T) {
	restore := configureTestRuntime(t, []safeInfo{})
	defer restore()

	expiredBody := filepath.Join(shortCacheDir, "expired"+cacheSuffix)
	expiredMeta := cacheMetadataFilename(expiredBody)
	orphanMeta := filepath.Join(shortCacheDir, "orphan"+cacheSuffix+cacheMetaSuffix)

	if err := os.WriteFile(expiredBody, []byte("body"), 0644); err != nil {
		t.Fatalf("write expired body failed: %v", err)
	}
	if err := os.WriteFile(expiredMeta, []byte(`{"status":200}`), 0644); err != nil {
		t.Fatalf("write expired meta failed: %v", err)
	}
	if err := os.WriteFile(orphanMeta, []byte(`{"status":200}`), 0644); err != nil {
		t.Fatalf("write orphan meta failed: %v", err)
	}

	oldTime := time.Now().Add(-shortCacheDuration - time.Minute)
	for _, name := range []string{expiredBody, expiredMeta, orphanMeta} {
		if err := os.Chtimes(name, oldTime, oldTime); err != nil {
			t.Fatalf("chtimes failed for %s: %v", name, err)
		}
	}

	cleanupShortCache()

	for _, name := range []string{expiredBody, expiredMeta, orphanMeta} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be removed, stat err=%v", name, err)
		}
	}
}

func TestThrottleWaitMetricsIncrease(t *testing.T) {
	testURL := "http://example.com/download/throttled.zip"
	restore := configureTestRuntime(t, []safeInfo{
		{URL: "example.com/download", MinValidSize: 1},
	})
	defer restore()

	upstreamLimiter = rate.NewLimiter(rate.Every(80*time.Millisecond), 1)
	upstreamClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusOK,
				Status:        "200 OK",
				Header:        http.Header{"Content-Type": []string{"application/octet-stream"}},
				Body:          io.NopCloser(strings.NewReader(strings.Repeat("z", 128))),
				ContentLength: 128,
				Request:       req,
			}, nil
		}),
	}

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL+"?1"), nil)
	firstReq.RemoteAddr = "127.0.0.1:12345"
	handler(first, firstReq)

	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(testURL+"?2"), nil)
	secondReq.RemoteAddr = "127.0.0.1:12345"
	start := time.Now()
	handler(second, secondReq)
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected second request to wait for throttle, elapsed=%s", elapsed)
	}

	aggregateMetrics.mu.Lock()
	defer aggregateMetrics.mu.Unlock()
	if aggregateMetrics.throttleWaits < 2 {
		t.Fatalf("expected throttle waits to be recorded, got %d", aggregateMetrics.throttleWaits)
	}
	if aggregateMetrics.throttleWaitDuration <= 0 {
		t.Fatalf("expected throttle wait duration to be positive")
	}
}

func TestMetricsAccountingAcrossScenarios(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte(strings.Repeat("m", 128)))
		case "/slow":
			time.Sleep(100 * time.Millisecond)
			_, _ = w.Write([]byte(strings.Repeat("m", 128)))
		case "/429":
			http.Error(w, "slow down", http.StatusTooManyRequests)
		default:
			http.Error(w, "missing", http.StatusNotFound)
		}
	}))
	defer upstream.Close()

	restore := configureTestRuntime(t, []safeInfo{
		{URL: strings.TrimPrefix(upstream.URL, "http://") + "/", MinValidSize: 1},
	})
	defer restore()
	upstreamClient = upstream.Client()

	okURL := upstream.URL + "/ok"
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(okURL), nil)
		req.RemoteAddr = "127.0.0.1:12345"
		handler(rec, req)
	}

	fetchTimeout = 20 * time.Millisecond
	timeoutRec := httptest.NewRecorder()
	timeoutReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(upstream.URL+"/slow"), nil)
	timeoutReq.RemoteAddr = "127.0.0.1:12345"
	handler(timeoutRec, timeoutReq)
	fetchTimeout = defaultFetchTimeout

	statusRec := httptest.NewRecorder()
	statusReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(upstream.URL+"/429"), nil)
	statusReq.RemoteAddr = "127.0.0.1:12345"
	handler(statusRec, statusReq)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelRec := httptest.NewRecorder()
	cancelReq := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(upstream.URL+"/cancel"), nil).WithContext(ctx)
	cancelReq.RemoteAddr = "127.0.0.1:12345"
	handler(cancelRec, cancelReq)

	aggregateMetrics.mu.Lock()
	defer aggregateMetrics.mu.Unlock()

	if aggregateMetrics.totalRequests != 5 {
		t.Fatalf("expected 5 total requests, got %d", aggregateMetrics.totalRequests)
	}
	if aggregateMetrics.cacheHits != 1 {
		t.Fatalf("expected 1 cache hit, got %d", aggregateMetrics.cacheHits)
	}
	if aggregateMetrics.cacheMisses != 4 {
		t.Fatalf("expected 4 cache misses, got %d", aggregateMetrics.cacheMisses)
	}
	if aggregateMetrics.upstreamFetches != 3 {
		t.Fatalf("expected 3 upstream fetches, got %d", aggregateMetrics.upstreamFetches)
	}
	if aggregateMetrics.upstreamStatus2xx != 1 {
		t.Fatalf("expected 1 upstream 2xx, got %d", aggregateMetrics.upstreamStatus2xx)
	}
	if aggregateMetrics.upstreamStatus429 != 1 || aggregateMetrics.upstreamStatus4xx != 1 {
		t.Fatalf("expected 1 upstream 429 and 1 4xx, got 429=%d 4xx=%d", aggregateMetrics.upstreamStatus429, aggregateMetrics.upstreamStatus4xx)
	}
	if aggregateMetrics.upstreamTimeouts != 1 {
		t.Fatalf("expected 1 upstream timeout, got %d", aggregateMetrics.upstreamTimeouts)
	}
	if aggregateMetrics.clientCancels != 1 {
		t.Fatalf("expected 1 client cancel, got %d", aggregateMetrics.clientCancels)
	}
}

func configureTestRuntime(t *testing.T, allowed []safeInfo) func() {
	t.Helper()

	oldShortCacheDir := shortCacheDir
	oldLongCacheDir := longCacheDir
	oldBytesSavedFilename := bytesSavedFilename
	oldSafeURLs := safeURLs
	oldFetchTimeout := fetchTimeout
	oldUpstreamLimiter := upstreamLimiter
	oldUpstreamClient := upstreamClient
	oldAggregateMetrics := aggregateMetrics
	oldCacheLocks := cacheLocks
	oldSavedBytes, oldSavedBytesDirty := snapshotSavedBytes()

	rootDir := t.TempDir()
	shortCacheDir = filepath.Join(rootDir, "short") + string(os.PathSeparator)
	longCacheDir = filepath.Join(rootDir, "long") + string(os.PathSeparator)
	bytesSavedFilename = filepath.Join(rootDir, "saved.txt")
	safeURLs = allowed
	fetchTimeout = defaultFetchTimeout
	upstreamLimiter = rate.NewLimiter(rate.Inf, 1)
	upstreamClient = newUpstreamClient()
	aggregateMetrics = metricsState{}
	setSavedBytes(0)
	cacheLocks = make(map[string]*sync.Mutex)

	if err := os.MkdirAll(shortCacheDir, 0755); err != nil {
		t.Fatalf("mkdir short cache failed: %v", err)
	}
	if err := os.MkdirAll(longCacheDir, 0755); err != nil {
		t.Fatalf("mkdir long cache failed: %v", err)
	}

	return func() {
		shortCacheDir = oldShortCacheDir
		longCacheDir = oldLongCacheDir
		bytesSavedFilename = oldBytesSavedFilename
		safeURLs = oldSafeURLs
		fetchTimeout = oldFetchTimeout
		upstreamLimiter = oldUpstreamLimiter
		upstreamClient = oldUpstreamClient
		aggregateMetrics = oldAggregateMetrics
		setSavedBytesState(oldSavedBytes, oldSavedBytesDirty)
		cacheLocks = oldCacheLocks
	}
}

func assertDownloadHeaders(t *testing.T, headers http.Header, bodyLen int64, lastModified string) {
	t.Helper()

	if got := headers.Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("unexpected content type: %q", got)
	}
	if got := headers.Get("Content-Disposition"); got != `attachment; filename="mod.zip"` {
		t.Fatalf("unexpected content disposition: %q", got)
	}
	if got := headers.Get("ETag"); got != `"etag-123"` {
		t.Fatalf("unexpected etag: %q", got)
	}
	if got := headers.Get("Last-Modified"); got != lastModified {
		t.Fatalf("unexpected last-modified: %q", got)
	}
	if got := headers.Get("Cache-Control"); got != "public, max-age=60" {
		t.Fatalf("unexpected cache-control: %q", got)
	}
	if got := headers.Get("Content-Length"); got != strconv.FormatInt(bodyLen, 10) {
		t.Fatalf("unexpected content-length: %q", got)
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type failingWriter struct {
	header     http.Header
	statusCode int
	written    int
	failAfter  int
}

func (w *failingWriter) Header() http.Header {
	return w.header
}

func (w *failingWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	remaining := w.failAfter - w.written
	if remaining <= 0 {
		return 0, net.ErrClosed
	}
	if len(p) > remaining {
		w.written += remaining
		return remaining, net.ErrClosed
	}
	w.written += len(p)
	return len(p), nil
}
