package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const version = "v004-03162025-0337p"

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

	http.HandleFunc("/", handler)
	log.Println("Starting server on " + port + "...")
	log.Fatal(http.ListenAndServe(port, nil))
}
