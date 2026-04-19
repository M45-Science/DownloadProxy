package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

const version = "v005-04182026-1046p"

var (
	limiter = rate.NewLimiter(4, 1000) // inbound requests

	// Limit upstream cache-miss fetches so we do not stampede origin servers.
	upstreamLimiter *rate.Limiter
	upstreamClient  *http.Client
)

func main() {
	flag.StringVar(&listenAddr, "listen-addr", defaultListenAddr, "listen address for the proxy")
	flag.DurationVar(&upstreamRate, "upstream-rate", defaultUpstreamRate, "time per upstream cache-miss fetch token")
	flag.IntVar(&upstreamBurst, "upstream-burst", defaultUpstreamBurst, "burst size for upstream cache-miss fetches")
	flag.DurationVar(&fetchTimeout, "fetch-timeout", defaultFetchTimeout, "hard timeout for each upstream fetch")
	flag.DurationVar(&metricsInterval, "metrics-interval", defaultMetricsInterval, "periodic metrics log interval")
	flag.Parse()

	validateRuntimeConfig()
	upstreamLimiter = rate.NewLimiter(rate.Every(upstreamRate), upstreamBurst)
	upstreamClient = newUpstreamClient()

	log.Println("Starting " + version)

	if err := os.MkdirAll(shortCacheDir, 0755); err != nil {
		log.Fatalf("create short cache dir %s: %v", shortCacheDir, err)
	}
	if err := os.MkdirAll(longCacheDir, 0755); err != nil {
		log.Fatalf("create long cache dir %s: %v", longCacheDir, err)
	}

	//Read in bandwidthSaved
	data, err := os.ReadFile(bytesSavedFilename)
	if err == nil {
		val, err := strconv.ParseUint(string(data), 10, 64)
		if err == nil {
			setSavedBytes(val)
			log.Println("Read " + bytesSavedFilename)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go startShortCacheCleanup(ctx)
	go startLongCacheCleanup(ctx)
	go startMetricsReporter(ctx)
	go startSavedBytesFlusher(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/", rateLimitedHandler)
	server := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	log.Println("Starting server on " + listenAddr + "...")
	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP shutdown error: %v", err)
		}
	}()

	err = server.ListenAndServe()
	writeSavedFile()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func rateLimitedHandler(w http.ResponseWriter, r *http.Request) {
	if !limiter.Allow() {
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}
	handler(w, r)
}

func waitForUpstreamSlot(ctx context.Context) error {
	if err := upstreamLimiter.Wait(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return context.DeadlineExceeded
	}
	return nil
}

func newUpstreamClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}
}

func validateRuntimeConfig() {
	if listenAddr == "" {
		log.Fatal("listen-addr must not be empty")
	}
	if upstreamRate <= 0 {
		log.Fatal("upstream-rate must be greater than 0")
	}
	if upstreamBurst < 1 {
		log.Fatal("upstream-burst must be at least 1")
	}
	if fetchTimeout <= 0 {
		log.Fatal("fetch-timeout must be greater than 0")
	}
	if metricsInterval <= 0 {
		log.Fatal("metrics-interval must be greater than 0")
	}
}
