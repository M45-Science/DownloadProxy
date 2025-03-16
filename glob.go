package main

import (
	"sync"
	"time"
)

const (
	shortCacheDir      = "./shortCache/"
	longCacheDir       = "./longCache/"
	port               = ":55555"
	bytesSavedFilename = "saved.txt"
	cacheSuffix        = ".cache"

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

var (
	cacheLocks      = make(map[string]*sync.Mutex)
	cacheLocksMutex sync.Mutex

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
