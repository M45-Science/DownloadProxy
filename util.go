package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var writeFile = os.WriteFile

// Redact queries to hide tokens
func redactQuery(url string) string {
	parts := strings.Split(url, "?")
	if len(parts) == 2 {
		return parts[0] + "?<redacted>"
	} else {
		return url
	}
}

// Generates a sha256
func generateCacheKey(url string) string {
	h := sha256.New()
	h.Write([]byte(url))
	return hex.EncodeToString(h.Sum(nil)) + cacheSuffix
}

func cacheMetadataFilename(bodyFilename string) string {
	return bodyFilename + cacheMetaSuffix
}

func cacheLockKey(filename string) string {
	return strings.TrimSuffix(filename, cacheMetaSuffix)
}

func openCachedBody(filename string) (*os.File, cacheMetadata, error) {
	bodyFile, err := os.Open(filename)
	if err != nil {
		return nil, cacheMetadata{}, err
	}

	info, err := bodyFile.Stat()
	if err != nil {
		bodyFile.Close()
		return nil, cacheMetadata{}, err
	}

	meta, err := readCacheMetadata(cacheMetadataFilename(filename))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			bodyFile.Close()
			return nil, cacheMetadata{}, err
		}

		return bodyFile, cacheMetadata{
			Version:       0,
			Status:        http.StatusOK,
			Headers:       make(http.Header),
			ContentLength: info.Size(),
		}, nil
	}

	if meta.Status == 0 {
		meta.Status = http.StatusOK
	}
	if meta.Headers == nil {
		meta.Headers = make(http.Header)
	}
	if meta.ContentLength <= 0 {
		meta.ContentLength = info.Size()
	}

	return bodyFile, meta, nil
}

func readCacheMetadata(filename string) (cacheMetadata, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return cacheMetadata{}, err
	}

	var meta cacheMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return cacheMetadata{}, err
	}

	return meta, nil
}

func writeCacheMetadata(filename string, meta cacheMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return writeFile(filename, data, 0644)
}

func preserveResponseHeaders(headers http.Header, contentLength int64) http.Header {
	preserved := make(http.Header)

	for _, key := range []string{
		"Content-Type",
		"Content-Disposition",
		"ETag",
		"Last-Modified",
		"Cache-Control",
	} {
		values := headers.Values(key)
		for _, value := range values {
			preserved.Add(key, value)
		}
	}

	if contentLength >= 0 {
		preserved.Set("Content-Length", strconv.FormatInt(contentLength, 10))
	}

	return preserved
}

func applyResponseHeaders(w http.ResponseWriter, headers http.Header, contentLength int64) {
	for key, values := range headers {
		w.Header().Del(key)
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if contentLength >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	}
}

func streamFile(w http.ResponseWriter, file *os.File, statusCode int, headers http.Header, contentLength int64) (int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	applyResponseHeaders(w, headers, contentLength)
	w.WriteHeader(statusCode)

	return io.Copy(w, file)
}

func streamFileByName(w http.ResponseWriter, filename string, statusCode int, headers http.Header, contentLength int64) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	return streamFile(w, file, statusCode, headers, contentLength)
}

func isLocalhost(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	return host == "127.0.0.1" || host == "::1" || host == "localhost"
}

// acquireCacheLock returns a lock entry for a specific cache key and tracks active users.
func acquireCacheLock(cacheKey string) *cacheLockEntry {
	cacheLocksMutex.Lock()
	defer cacheLocksMutex.Unlock()

	entry, exists := cacheLocks[cacheKey]
	if !exists {
		entry = &cacheLockEntry{}
		cacheLocks[cacheKey] = entry
	}
	entry.refs++

	return entry
}

func releaseCacheLock(cacheKey string, entry *cacheLockEntry) {
	if entry == nil {
		return
	}

	cacheLocksMutex.Lock()
	defer cacheLocksMutex.Unlock()

	current, exists := cacheLocks[cacheKey]
	if !exists || current != entry {
		return
	}

	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs == 0 {
		delete(cacheLocks, cacheKey)
	}
}

// safeJoin joins base directory and the target path, ensuring it remains within the base directory
func safeJoin(baseDir, target string) (string, error) {
	joinedPath := filepath.Join(baseDir, target)

	absPath, err := filepath.Abs(joinedPath)
	if err != nil {
		return "", err
	}

	absBaseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return "", err
	}

	relPath, err := filepath.Rel(absBaseDir, absPath)
	if err != nil {
		return "", err
	}

	if relPath == ".." || strings.HasPrefix(relPath, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path is outside of base directory")
	}

	return absPath, nil
}

// startShortCacheCleanup runs a background task that periodically cleans up expired cache files
func startShortCacheCleanup(ctx context.Context) {

	log.Println("Running initial shortCache cleanup...")
	cleanupShortCache()

	ticker := time.NewTicker(shortCacheCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanupShortCache()
		}
	}
}

// startShortCacheCleanup runs a background task that periodically cleans up expired cache files
func startLongCacheCleanup(ctx context.Context) {

	log.Println("Running initial longCache cleanup...")
	cleanupLongCache()

	ticker := time.NewTicker(longCacheCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanupLongCache()
		}
	}
}

func startSavedBytesFlusher(ctx context.Context) {
	ticker := time.NewTicker(savedBytesFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			writeSavedFile()
			return
		case <-ticker.C:
			writeSavedFile()
		}
	}
}

// cleanupShortCache scans the cache directory and removes expired files
func cleanupShortCache() {
	writeSavedFile()

	processed := make(map[string]struct{})
	filepath.Walk(shortCacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("Error accessing shortCache file:", err)
			return nil
		}
		if info == nil || info.IsDir() {
			return nil
		}

		lockKey := cacheLockKey(info.Name())
		if _, done := processed[lockKey]; done {
			return nil
		}

		// Check if the file is older than the cache duration
		if time.Since(info.ModTime()) > shortCacheDuration {
			log.Println("Deleting expired shortCache file:", path)

			//Lock this so we can't delete it while it is in-use
			lockEntry := acquireCacheLock(lockKey)
			lockEntry.mu.Lock()
			processed[lockKey] = struct{}{}
			err := removeCacheArtifacts(filepath.Join(shortCacheDir, lockKey))
			lockEntry.mu.Unlock()
			releaseCacheLock(lockKey, lockEntry)
			if err != nil {
				log.Println("Failed to delete shortCache file:", err)
				return err
			}
		}
		return nil
	})
}

// cleanupLongCache scans the cache directory and removes expired files
func cleanupLongCache() {
	processed := make(map[string]struct{})

	filepath.Walk(longCacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("Error accessing longCache file:", err)
			return nil
		}
		if info == nil || info.IsDir() {
			return nil
		}

		lockKey := cacheLockKey(info.Name())
		if _, done := processed[lockKey]; done {
			return nil
		}

		// Check if the file is older than the cache duration
		if time.Since(info.ModTime()) > longCacheDuration {
			log.Println("Deleting expired longCache file:", path)

			//Lock this so we can't delete it while it is in-use
			lockEntry := acquireCacheLock(lockKey)
			lockEntry.mu.Lock()
			processed[lockKey] = struct{}{}
			err := removeCacheArtifacts(filepath.Join(longCacheDir, lockKey))
			lockEntry.mu.Unlock()
			releaseCacheLock(lockKey, lockEntry)
			if err != nil {
				log.Println("Failed to delete longCache file:", err)
				return err
			}
		}
		return nil
	})
}

func isAllowedURL(rawURL string) (bool, bool, int) {

	cleanURL := strings.TrimPrefix(rawURL, "https://")
	cleanURL = strings.TrimPrefix(cleanURL, "http://")
	cleanURL = strings.TrimPrefix(cleanURL, "www.")

	for _, safe := range safeURLs {
		if strings.HasPrefix(cleanURL, safe.URL) {
			return true, safe.longTermCache, safe.MinValidSize
		}
	}
	log.Printf("Not in path list: %v : %v\n", cleanURL, safeURLs)

	return false, false, 0
}

func writeSavedFile() {
	savedBytes, dirty := snapshotSavedBytes()
	if dirty {
		if err := writeFile(bytesSavedFilename, []byte(strconv.FormatUint(savedBytes, 10)), 0644); err != nil {
			log.Printf("Failed to write saved bytes file %s: %v", bytesSavedFilename, err)
			return
		}
		markSavedBytesFlushed(savedBytes)
	}
}

func removeCacheArtifacts(bodyFilename string) error {
	for _, name := range []string{bodyFilename, cacheMetadataFilename(bodyFilename)} {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func setSavedBytes(value uint64) {
	setSavedBytesState(value, false)
}

func setSavedBytesState(value uint64, dirty bool) {
	bytesSavedMutex.Lock()
	defer bytesSavedMutex.Unlock()
	bytesBandwidthSaved = value
	bytesBandwidthSavedDirty = dirty
}

func addSavedBytes(delta uint64) {
	if delta == 0 {
		return
	}
	bytesSavedMutex.Lock()
	defer bytesSavedMutex.Unlock()
	bytesBandwidthSaved += delta
	bytesBandwidthSavedDirty = true
}

func snapshotSavedBytes() (uint64, bool) {
	bytesSavedMutex.Lock()
	defer bytesSavedMutex.Unlock()
	return bytesBandwidthSaved, bytesBandwidthSavedDirty
}

func markSavedBytesFlushed(savedBytes uint64) {
	bytesSavedMutex.Lock()
	defer bytesSavedMutex.Unlock()
	if bytesBandwidthSaved == savedBytes {
		bytesBandwidthSavedDirty = false
	}
}
