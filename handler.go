package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func handler(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	summary := requestSummary{
		url:        redactQuery(strings.TrimPrefix(r.URL.Path, "/")),
		cache:      "none",
		source:     "local",
		statusCode: http.StatusInternalServerError,
	}
	defer func() {
		summary.totalDuration = time.Since(startedAt)
		logRequestSummary(summary)
	}()

	aggregateMetrics.recordRequest()

	if !isLocalhost(r.RemoteAddr) {
		log.Println("Forbidden:", r.RemoteAddr)
		summary.statusCode = http.StatusForbidden
		summary.errorClass = "forbidden_remote"
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	urlPath, _ := url.PathUnescape(r.URL.Path)
	urlPath = strings.TrimPrefix(urlPath, "/")
	cleanedURLPath := redactQuery(urlPath)
	summary.url = cleanedURLPath

	good, long, minURLSize := isAllowedURL(urlPath)
	if !good {
		log.Println("Forbidden:", cleanedURLPath)
		summary.statusCode = http.StatusForbidden
		summary.errorClass = "forbidden_path"
		http.Error(w, "Forbidden path", http.StatusForbidden)
		return
	}

	cacheFile, err := cacheBodyFilename(urlPath, long)
	if err != nil {
		log.Println("Invalid cache path")
		summary.statusCode = http.StatusInternalServerError
		summary.errorClass = "cache_path"
		http.Error(w, "Invalid cache path", http.StatusInternalServerError)
		return
	}

	cacheKey := generateCacheKey(urlPath)
	urlCacheLock := getCacheLock(cacheKey)
	urlCacheLock.Lock()
	defer urlCacheLock.Unlock()

	if err := serveFromCache(w, cleanedURLPath, cacheFile, &summary); err == nil {
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Printf("Cache read failed for %s: %v", cleanedURLPath, err)
	}

	aggregateMetrics.recordCacheMiss()
	summary.cache = "miss"
	summary.source = "upstream"

	waitStartedAt := time.Now()
	if err := waitForUpstreamSlot(r.Context()); err != nil {
		summary.throttleWait = time.Since(waitStartedAt)
		aggregateMetrics.recordThrottleWait(summary.throttleWait)
		summary.statusCode = http.StatusRequestTimeout
		summary.errorClass = "client_canceled"
		aggregateMetrics.recordClientCancel()
		log.Println(cleanedURLPath + " : request canceled before upstream fetch")
		http.Error(w, cleanedURLPath+" : request canceled before upstream fetch", http.StatusRequestTimeout)
		return
	}
	summary.throttleWait = time.Since(waitStartedAt)
	aggregateMetrics.recordThrottleWait(summary.throttleWait)

	fetchCtx, cancel := context.WithTimeout(r.Context(), fetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, urlPath, nil)
	if err != nil {
		summary.statusCode = http.StatusBadRequest
		summary.errorClass = "invalid_upstream_url"
		log.Println(cleanedURLPath + " : invalid upstream URL")
		http.Error(w, cleanedURLPath+" : invalid upstream URL", http.StatusBadRequest)
		return
	}

	aggregateMetrics.recordUpstreamFetch()
	resp, err := upstreamClient.Do(req)
	if err != nil {
		statusCode, detail, errorClass := classifyFetchError(err)
		summary.statusCode = statusCode
		summary.errorClass = errorClass
		updateErrorMetrics(errorClass)
		log.Println(cleanedURLPath + " : " + detail)
		http.Error(w, cleanedURLPath+" : "+detail, statusCode)
		return
	}
	defer resp.Body.Close()

	summary.upstreamStatus = resp.StatusCode
	aggregateMetrics.recordUpstreamStatus(resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		summary.statusCode = mapUpstreamStatus(resp.StatusCode)
		summary.errorClass = upstreamStatusClass(resp.StatusCode)
		log.Printf("%s: upstream returned %s", cleanedURLPath, resp.Status)
		http.Error(w, fmt.Sprintf("%v: upstream returned %v", cleanedURLPath, resp.Status), summary.statusCode)
		return
	}

	downloadedFile, bodyLen, err := downloadUpstreamBody(cleanedURLPath, cacheFile, resp, &summary)
	if err != nil {
		statusCode, detail, errorClass := classifyFetchError(err)
		summary.statusCode = statusCode
		summary.errorClass = errorClass
		updateErrorMetrics(errorClass)
		log.Println(cleanedURLPath + " : " + detail)
		http.Error(w, cleanedURLPath+" : "+detail, statusCode)
		return
	}
	defer os.Remove(downloadedFile)

	headers := preserveResponseHeaders(resp.Header, bodyLen)
	meta := cacheMetadata{
		Version:       cacheFormatVersion,
		Status:        http.StatusOK,
		Headers:       headers,
		ContentLength: bodyLen,
	}

	servePath := downloadedFile
	if shouldCacheResponse(bodyLen, minURLSize) {
		if err := commitCacheEntry(downloadedFile, cacheFile, meta); err != nil {
			log.Printf("Failed to commit cache for %s: %v", cleanedURLPath, err)
		} else {
			servePath = cacheFile
		}
	} else if bodyLen < minCacheSize {
		log.Println("Not caching (minCacheSize):", bodyLen, "bytes:", cleanedURLPath)
	} else {
		log.Println("Not caching (min url size):", bodyLen, "bytes:", cleanedURLPath)
	}

	summary.statusCode = http.StatusOK
	summary.errorClass = ""
	summary.bytesServed, err = streamFileByName(w, servePath, http.StatusOK, headers, bodyLen)
	aggregateMetrics.addBytesServed(summary.bytesServed)
	if err != nil {
		if isClientCanceledError(err) {
			summary.errorClass = "client_canceled"
			aggregateMetrics.recordClientCancel()
			log.Printf("%s : client canceled while writing response", cleanedURLPath)
			return
		}
		summary.errorClass = "response_write"
		log.Printf("%s : failed to write response: %v", cleanedURLPath, err)
	}
}

func serveFromCache(w http.ResponseWriter, cleanedURLPath, cacheFile string, summary *requestSummary) error {
	bodyFile, meta, err := openCachedBody(cacheFile)
	if err != nil {
		return err
	}
	defer bodyFile.Close()

	summary.cache = "hit"
	summary.source = "cache"
	summary.statusCode = meta.Status
	summary.upstreamStatus = meta.Status

	bytesServed, err := streamFile(w, bodyFile, meta.Status, meta.Headers, meta.ContentLength)
	summary.bytesServed = bytesServed
	if err != nil {
		if isClientCanceledError(err) {
			summary.errorClass = "client_canceled"
			aggregateMetrics.recordClientCancel()
			log.Printf("%s : client canceled while writing cached response", cleanedURLPath)
			return nil
		}
		summary.errorClass = "cache_write"
		return err
	}

	addSavedBytes(uint64(bytesServed))
	aggregateMetrics.recordCacheHit(bytesServed, bytesServed)
	log.Println("From Cache:", cleanedURLPath)

	return nil
}

func cacheBodyFilename(urlPath string, long bool) (string, error) {
	rootDir := shortCacheDir
	if long {
		rootDir = longCacheDir
	}

	return safeJoin(rootDir, generateCacheKey(urlPath))
}

func downloadUpstreamBody(cleanedURLPath, cacheFile string, resp *http.Response, summary *requestSummary) (string, int64, error) {
	tempFile, err := os.CreateTemp(filepath.Dir(cacheFile), filepath.Base(cacheFile)+".tmp-*")
	if err != nil {
		return "", 0, err
	}

	tempName := tempFile.Name()
	defer func() {
		tempFile.Close()
	}()

	written, copyErr := io.Copy(tempFile, resp.Body)
	summary.bytesDownloaded = written
	aggregateMetrics.addBytesDownloaded(written)
	if copyErr != nil {
		os.Remove(tempName)
		return "", written, copyErr
	}

	if resp.ContentLength >= 0 && written != resp.ContentLength {
		os.Remove(tempName)
		return "", written, fmt.Errorf("content length mismatch: expected %d got %d", resp.ContentLength, written)
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempName)
		return "", written, err
	}

	log.Println("Downloaded:", cleanedURLPath)
	return tempName, written, nil
}

func commitCacheEntry(tempName, cacheFile string, meta cacheMetadata) error {
	if err := os.Rename(tempName, cacheFile); err != nil {
		return err
	}

	if err := writeCacheMetadata(cacheMetadataFilename(cacheFile), meta); err != nil {
		log.Printf("Failed to write cache metadata for %s: %v", cacheFile, err)
	}

	return nil
}

func shouldCacheResponse(bodyLen int64, minURLSize int) bool {
	if bodyLen < minCacheSize {
		return false
	}
	return bodyLen > int64(minURLSize)
}

func classifyFetchError(err error) (int, string, string) {
	if errors.Is(err, context.Canceled) {
		return http.StatusRequestTimeout, "request canceled", "client_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "upstream timeout", "upstream_timeout"
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return http.StatusGatewayTimeout, "upstream timeout", "upstream_timeout"
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if errors.Is(urlErr.Err, context.Canceled) {
			return http.StatusRequestTimeout, "request canceled", "client_canceled"
		}
		if errors.Is(urlErr.Err, context.DeadlineExceeded) {
			return http.StatusGatewayTimeout, "upstream timeout", "upstream_timeout"
		}
		var innerNetErr net.Error
		if errors.As(urlErr.Err, &innerNetErr) && innerNetErr.Timeout() {
			return http.StatusGatewayTimeout, "upstream timeout", "upstream_timeout"
		}
		return http.StatusBadGateway, "failed to reach upstream", "upstream_transport"
	}

	if isContentLengthMismatch(err) {
		return http.StatusBadGateway, "data ended early", "incomplete_upstream_body"
	}

	return http.StatusBadGateway, "failed to fetch upstream response", "upstream_transport"
}

func mapUpstreamStatus(statusCode int) int {
	switch {
	case statusCode >= 500:
		return http.StatusBadGateway
	case statusCode == http.StatusTooManyRequests:
		return http.StatusTooManyRequests
	case statusCode >= 400:
		return statusCode
	default:
		return http.StatusBadGateway
	}
}

func updateErrorMetrics(errorClass string) {
	switch errorClass {
	case "client_canceled":
		aggregateMetrics.recordClientCancel()
	case "upstream_timeout":
		aggregateMetrics.recordUpstreamTimeout()
	}
}

func upstreamStatusClass(statusCode int) string {
	switch {
	case statusCode == 429:
		return "upstream_429"
	case statusCode >= 500:
		return "upstream_5xx"
	case statusCode >= 400:
		return "upstream_4xx"
	default:
		return ""
	}
}

func isClientCanceledError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}

func isContentLengthMismatch(err error) bool {
	return err != nil && strings.Contains(err.Error(), "content length mismatch")
}
