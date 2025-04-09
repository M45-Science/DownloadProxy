package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

const version = "v004-04092025-0606"

var limiter = rate.NewLimiter(4, 1000) // 10 req/sec with burst of 1000

func main() {
	log.Println("Starting " + version)

	os.MkdirAll(shortCacheDir, 0755)
	os.MkdirAll(longCacheDir, 0755)

	//Read in bandwidthSaved
	data, err := os.ReadFile(bytesSavedFilename)
	if err == nil {
		val, err := strconv.ParseUint(string(data), 10, 64)
		if err == nil {
			bytesBandwidthSaved = val
			log.Println("Read " + bytesSavedFilename)
		}
	}

	go startShortCacheCleanup()
	go startLongCacheCleanup()
	time.Sleep(time.Second)

	http.HandleFunc("/", rateLimitedHandler)
	log.Println("Starting server on " + port + "...")
	log.Fatal(http.ListenAndServe(port, nil))
}

func rateLimitedHandler(w http.ResponseWriter, r *http.Request) {
	if !limiter.Allow() {
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}
	handler(w, r)
}
