package main

import (
	"net/http"
	"sync"
	"time"
)

const (
	cacheSuffix     = ".cache"
	cacheMetaSuffix = ".meta.json"

	defaultShortCacheDir      = "./shortCache/"
	defaultLongCacheDir       = "./longCache/"
	defaultBytesSavedFilename = "saved.txt"
	defaultListenAddr         = "127.0.0.1:55555"
	defaultUpstreamRate       = time.Second
	defaultUpstreamBurst      = 1
	defaultFetchTimeout       = 2 * time.Minute
	defaultMetricsInterval    = time.Minute
	cacheFormatVersion        = 1

	shortCacheDuration = time.Minute * 10
	longCacheDuration  = time.Hour * 24 * 30

	//Prevent case of probable corrupted data
	minCacheSize              = 100
	shortCacheCleanupInterval = time.Minute
	longCacheCleanupInterval  = time.Hour
)

type safeInfo struct {
	URL           string
	longTermCache bool
	MinValidSize  int
}

type cacheMetadata struct {
	Version       int         `json:"version"`
	Status        int         `json:"status"`
	Headers       http.Header `json:"headers,omitempty"`
	ContentLength int64       `json:"content_length,omitempty"`
}

var (
	shortCacheDir      = defaultShortCacheDir
	longCacheDir       = defaultLongCacheDir
	bytesSavedFilename = defaultBytesSavedFilename
	listenAddr         = defaultListenAddr
	upstreamRate       = defaultUpstreamRate
	upstreamBurst      = defaultUpstreamBurst
	fetchTimeout       = defaultFetchTimeout
	metricsInterval    = defaultMetricsInterval

	cacheLocks      = make(map[string]*sync.Mutex)
	cacheLocksMutex sync.Mutex

	bytesSavedMutex          sync.Mutex
	bytesBandwidthSaved      uint64
	bytesBandwidthSavedDirty bool

	safeURLs []safeInfo = []safeInfo{
		//Factorio download
		{URL: "factorio.com/get-download", MinValidSize: 1024 * 1024 * 50},
		{URL: "factorio.com/download/sha256sums", MinValidSize: 1024 * 100},

		//Mod download
		{URL: "mods.factorio.com/download", longTermCache: true, MinValidSize: 1024 * 5},

		//JSON data
		{URL: "factorio.com/api/latest-releases", MinValidSize: 100},
		{URL: "mods.factorio.com/api/mods", MinValidSize: 500},
	}
)
