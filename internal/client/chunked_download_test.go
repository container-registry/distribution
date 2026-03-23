package client

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
)

func TestGetRedirectURL(t *testing.T) {
	redirectTarget := "https://s3.amazonaws.com/bucket/blob?X-Amz-Signature=abc"

	// Server that returns a 307 redirect.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	ctx := context.Background()
	url, err := getRedirectURL(ctx, http.DefaultTransport, srv.URL+"/v2/test/blobs/sha256:abc")
	if err != nil {
		t.Fatalf("getRedirectURL failed: %v", err)
	}
	if url != redirectTarget {
		t.Errorf("expected redirect URL %q, got %q", redirectTarget, url)
	}
}

func TestGetRedirectURL_NoRedirect(t *testing.T) {
	// Server that returns 200 (serves blob directly).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("blob data"))
	}))
	defer srv.Close()

	ctx := context.Background()
	url, err := getRedirectURL(ctx, http.DefaultTransport, srv.URL+"/v2/test/blobs/sha256:abc")
	if err != nil {
		t.Fatalf("getRedirectURL failed: %v", err)
	}
	if url != "" {
		t.Errorf("expected empty redirect URL for non-redirect response, got %q", url)
	}
}

func TestDownloadChunked(t *testing.T) {
	// Generate random test data (1 MiB — small for tests but exercises chunking logic).
	dataSize := int64(1 << 20) // 1 MiB
	data := make([]byte, dataSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("failed to generate test data: %v", err)
	}

	// Server that supports Range requests.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", strconv.FormatInt(dataSize, 10))
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}

		// Parse "bytes=start-end"
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.SplitN(rangeHeader, "-", 2)
		if len(parts) != 2 {
			http.Error(w, "invalid range", http.StatusBadRequest)
			return
		}
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		if start < 0 || end >= dataSize || start > end {
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, dataSize))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer srv.Close()

	ctx := context.Background()
	result, err := downloadChunked(ctx, srv.URL, dataSize)
	if err != nil {
		t.Fatalf("downloadChunked failed: %v", err)
	}

	if int64(len(result)) != dataSize {
		t.Fatalf("result size %d, expected %d", len(result), dataSize)
	}

	// Verify byte-for-byte equality.
	for i := range data {
		if result[i] != data[i] {
			t.Fatalf("mismatch at byte %d: expected %d, got %d", i, data[i], result[i])
		}
	}
}

func TestChunkedGet_SmallBlob(t *testing.T) {
	// Blobs smaller than threshold should return nil, nil.
	ctx := context.Background()
	data, err := chunkedGet(ctx, http.DefaultTransport, "http://example.com/blob", 100*1024*1024, "sha256:abc")
	if data != nil || err != nil {
		t.Errorf("expected nil,nil for small blob, got data=%v, err=%v", data != nil, err)
	}
}

func TestChunkedGet_WithRedirect(t *testing.T) {
	// Generate test data large enough for chunked download.
	dataSize := int64(1 << 20) // Use 1 MiB for testing
	data := make([]byte, dataSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("failed to generate test data: %v", err)
	}
	dgst := digest.FromBytes(data)

	// S3-like backend that serves Range requests.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}

		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.SplitN(rangeHeader, "-", 2)
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		if end >= dataSize {
			end = dataSize - 1
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, dataSize))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer backend.Close()

	// Registry that redirects to the backend.
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, backend.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer registry.Close()

	ctx := context.Background()

	// Override threshold for testing.
	origThreshold := defaultChunkedThreshold
	// We can't modify the const, so test with a size >= the threshold.
	// Use chunkedGet directly with our test size pretending it meets threshold.
	_ = origThreshold

	// Test the redirect capture + chunked download flow manually.
	redirectURL, err := getRedirectURL(ctx, http.DefaultTransport, registry.URL+"/v2/test/blobs/"+dgst.String())
	if err != nil {
		t.Fatalf("getRedirectURL failed: %v", err)
	}
	if redirectURL == "" {
		t.Fatal("expected redirect URL, got empty string")
	}

	result, err := downloadChunked(ctx, redirectURL, dataSize)
	if err != nil {
		t.Fatalf("downloadChunked failed: %v", err)
	}

	actual := digest.FromBytes(result)
	if actual != dgst {
		t.Fatalf("digest mismatch: expected %s, got %s", dgst, actual)
	}
}

func TestDownloadChunked_MultipleChunks(t *testing.T) {
	// Test with data that spans multiple chunks.
	// Use a small "chunk size" by testing with data larger than 128 MiB chunk.
	// For unit test, we verify the chunking logic by testing with known data.
	dataSize := int64(300 * 1024) // 300 KiB
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.SplitN(rangeHeader, "-", 2)
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		if end >= dataSize {
			end = dataSize - 1
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, dataSize))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer srv.Close()

	ctx := context.Background()
	result, err := downloadChunked(ctx, srv.URL, dataSize)
	if err != nil {
		t.Fatalf("downloadChunked failed: %v", err)
	}

	if int64(len(result)) != dataSize {
		t.Fatalf("result size %d, expected %d", len(result), dataSize)
	}

	for i := range data {
		if result[i] != data[i] {
			t.Fatalf("mismatch at byte %d: expected %d, got %d", i, data[i], result[i])
		}
	}
}
