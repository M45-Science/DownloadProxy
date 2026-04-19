# DownloadProxy

`DownloadProxy` is a small local HTTP caching proxy for Factorio server environments.

It is designed to sit on the same machine as a Factorio server or related automation and reduce repeated upstream downloads by caching allowed Factorio assets locally. The allowlist is intentionally narrow and focused on Factorio endpoints such as game downloads, mod downloads, and selected API responses.

## Purpose

This project exists to help Factorio servers avoid repeatedly fetching the same upstream files:

- Factorio release downloads
- Factorio checksum files
- Factorio mod downloads
- Selected Factorio API responses

The proxy is intended specifically for Factorio server use. It is not a general-purpose open proxy.

## How It Works

- Only requests from localhost are accepted.
- Only explicitly allowed upstream Factorio URLs are fetched.
- Cache hits are served from disk.
- Cache misses are throttled before contacting the upstream server.
- Successful responses are stored with a body file and metadata sidecar.
- Useful upstream headers such as `Content-Type`, `Content-Disposition`, `ETag`, and `Last-Modified` are preserved.

## Running

Build:

```bash
go build ./...
```

Run with defaults:

```bash
./goHTTPCacher
```

The default bind address is:

```text
127.0.0.1:55555
```

Useful launch flags:

```bash
./goHTTPCacher \
  -listen-addr=127.0.0.1:55555 \
  -upstream-rate=1s \
  -upstream-burst=1 \
  -fetch-timeout=2m \
  -metrics-interval=1m
```

## Cache Behavior

- Short-lived cache entries are stored in `./shortCache/`
- Long-lived cache entries are stored in `./longCache/`
- Cached bodies use the `.cache` suffix
- Cached metadata uses the `.cache.meta.json` suffix

Older body-only cache entries are still readable.

## Notes

- This service is intentionally restrictive.
- It should be run locally on the same host as the Factorio server or supporting automation.
- If you need additional upstream endpoints, they should be added deliberately to the allowlist in the source rather than opening the proxy broadly.
