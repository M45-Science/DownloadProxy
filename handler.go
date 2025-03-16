package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func handler(w http.ResponseWriter, r *http.Request) {
	if !isLocalhost(r.RemoteAddr) {
		log.Println("Forbidden: ", r.RemoteAddr)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Parse the requested path and make it into a URL
	urlPath, _ := url.PathUnescape(r.URL.Path)
	urlPath = strings.TrimPrefix(urlPath, "/")

	good, long, minURLSize := isAllowedURL(urlPath)
	if !good {
		log.Println("Forbidden: ", redactQuery(urlPath))
		http.Error(w, "Forbidden path", http.StatusForbidden)
		return
	}

	cleanedURLPath := redactQuery(urlPath)
	cacheKey := generateCacheKey(urlPath)

	//For specific paths, cache for a longer time
	rootDir := shortCacheDir
	if long {
		rootDir = longCacheDir
	}

	// Construct the safe cache file path
	cacheFile, err := safeJoin(rootDir, cacheKey)
	if err != nil {
		emsg := "Invalid cache path"
		log.Println(emsg)
		http.Error(w, emsg, http.StatusInternalServerError)
		return
	}

	// Acquire a lock for this cache key to prevent multiple downloads
	urlCacheLock := getCacheLock(cacheKey)
	urlCacheLock.Lock()
	defer removeCacheLock(cacheKey)

	// Check if the URL is cached
	if data, err := checkCache(cacheFile); err == nil {

		//Track bandwidth saved
		bytesBandwidthSaved = bytesBandwidthSaved + uint64(len(data))
		bytesBandwidthSavedDirty = true

		// Serve from cache
		log.Println("From Cache: ", cleanedURLPath)
		w.Write(data)
		return
	}
	log.Println("Downloading: ", cleanedURLPath)

	// Fetch the data from the URL
	resp, err := http.Get(urlPath)
	if err != nil {
		emsg := cleanedURLPath + " : failed to fetch URL"
		log.Println(emsg)
		http.Error(w, emsg, http.StatusGatewayTimeout)
		return
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		emsg := cleanedURLPath + " : failed to read response"
		log.Println(emsg)
		http.Error(w, emsg, http.StatusBadGateway)
		return
	}

	// Do not cache responses with errors
	if resp.StatusCode != 200 {
		emsg := fmt.Sprintf("%v: status code error: %v", cleanedURLPath, resp.StatusCode)
		log.Println(emsg)
		http.Error(w, emsg, http.StatusBadGateway)
		return
	}

	// Do not cache responses that are incomplete
	bodyLen := len(body)
	if resp.ContentLength >= 0 {
		if bodyLen != int(resp.ContentLength) {
			emsg := cleanedURLPath + " : data ended early"
			log.Println(emsg)
			http.Error(w, emsg, http.StatusBadGateway)
			return
		}
	}

	// Cache the response data if it is larger than the minimum
	// for that allowed path and the global minimum size
	// This helps prevent caching invalid responses
	if bodyLen >= minCacheSize {
		if good && bodyLen > minURLSize {
			if err := cacheData(cacheFile, body); err != nil {
				log.Println("Failed to cache data:", err)
			}
		} else {
			log.Println("Not caching (min url size): ", len(body), "bytes: ", cleanedURLPath)
		}
	} else {
		log.Println("Not caching (minCacheSize): ", len(body), "bytes: ", cleanedURLPath)
	}

	//Include content length
	w.Header().Set("Content-Length", strconv.Itoa(bodyLen))

	// Serve the response
	w.Write(body)
}
