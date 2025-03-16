package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func removeCacheLock(cacheKey string) {
	cacheLocksMutex.Lock()
	delete(cacheLocks, cacheKey)
	cacheLocksMutex.Unlock()
}

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

func checkCache(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func cacheData(filename string, data []byte) error {
	return os.WriteFile(filename, data, 0644)
}

func isLocalhost(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	return host == "127.0.0.1" || host == "::1" || host == "localhost"
}

// getCacheLock returns a mutex lock for a specific cache key to prevent concurrent downloads
func getCacheLock(cacheKey string) *sync.Mutex {
	cacheLocksMutex.Lock()
	defer cacheLocksMutex.Unlock()

	if _, exists := cacheLocks[cacheKey]; !exists {
		cacheLocks[cacheKey] = &sync.Mutex{}
	}

	return cacheLocks[cacheKey]
}

// safeJoin joins base directory and the target path, ensuring it remains within the base directory
func safeJoin(baseDir, target string) (string, error) {
	// Join the base directory and target filename
	joinedPath := filepath.Join(baseDir, target)

	// Get the absolute path
	absPath, err := filepath.Abs(joinedPath)
	if err != nil {
		return "", err
	}

	// Get the absolute base directory path
	absBaseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return "", err
	}

	// Ensure that the final path starts with the base directory
	if !strings.HasPrefix(absPath, absBaseDir) {
		return "", fmt.Errorf("path is outside of base directory")
	}

	return absPath, nil
}

// startShortCacheCleanup runs a background task that periodically cleans up expired cache files
func startShortCacheCleanup() {

	log.Println("Running initial shortCache cleanup...")
	cleanupShortCache()

	// Start the ticker for periodic cleanup
	ticker := time.NewTicker(shortCacheCleanupInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		//log.Println("Running periodic shortCache cleanup...")

		cleanupShortCache()
	}
}

// startShortCacheCleanup runs a background task that periodically cleans up expired cache files
func startLongCacheCleanup() {

	log.Println("Running initial longCache cleanup...")
	cleanupLongCache()

	// Start the ticker for periodic cleanup
	ticker := time.NewTicker(longCacheCleanupInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		//log.Println("Running periodic longCache cleanup...")

		cleanupLongCache()
	}
}

// cleanupShortCache scans the cache directory and removes expired files
func cleanupShortCache() {
	writeSavedFile()

	filepath.Walk(shortCacheDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if err != nil {
			log.Println("Error accessing shortCache file:", err)
			return nil
		}

		// Check if the file is older than the cache duration
		if time.Since(info.ModTime()) > shortCacheDuration {
			log.Println("Deleting expired shortCache file:", path)

			//Lock this so we can't delete it while it is in-use
			urlCacheLock := getCacheLock(info.Name())
			urlCacheLock.Lock()
			defer removeCacheLock(info.Name())

			//Lock entire locks map, then delete the file
			//If we were able to delete the file, remove it from the locks map
			cacheLocksMutex.Lock()
			defer cacheLocksMutex.Unlock()
			if err := os.Remove(path); err != nil {
				log.Println("Failed to delete shortCache file:", err)
				return err
			}
		}
		return nil
	})
}

// cleanupLongCache scans the cache directory and removes expired files
func cleanupLongCache() {

	filepath.Walk(longCacheDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if err != nil {
			log.Println("Error accessing longCache file:", err)
			return nil
		}

		// Check if the file is older than the cache duration
		if time.Since(info.ModTime()) > longCacheDuration {
			log.Println("Deleting expired longCache file:", path)

			//Lock this so we can't delete it while it is in-use
			urlCacheLock := getCacheLock(info.Name())
			urlCacheLock.Lock()
			defer removeCacheLock(info.Name())

			cacheLocksMutex.Lock()
			defer cacheLocksMutex.Unlock()
			if err := os.Remove(path); err != nil {
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
	//Write saved.txt if needed
	if bytesBandwidthSavedDirty {
		os.WriteFile(bytesSavedFilename, []byte(strconv.FormatUint(bytesBandwidthSaved, 10)), 0644)
		bytesBandwidthSavedDirty = false
	}
}
