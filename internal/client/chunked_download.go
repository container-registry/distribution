package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/opencontainers/go-digest"
)

const (
	// defaultChunkSize is the size of each chunk for parallel downloads (128 MiB).
	defaultChunkSize int64 = 128 << 20

	// defaultChunkedThreshold is the minimum blob size to trigger chunked downloads (512 MiB).
	defaultChunkedThreshold int64 = 512 << 20

	// maxChunkConcurrency is the maximum number of parallel chunk downloads.
	maxChunkConcurrency = 16
)

// getRedirectURL performs a GET request using the authenticated client but
// does not follow redirects. If the server returns a redirect (307/302),
// the target URL is returned. This is used to capture S3 pre-signed URLs
// that can then be reused with Range headers for parallel downloads.
func getRedirectURL(ctx context.Context, transport http.RoundTripper, blobURL string) (string, error) {
	noFollowClient := &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blobURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := noFollowClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	// Drain body to allow connection reuse.
	io.Copy(io.Discard, resp.Body)

	switch resp.StatusCode {
	case http.StatusTemporaryRedirect, http.StatusFound, http.StatusMovedPermanently:
		loc := resp.Header.Get("Location")
		if loc == "" {
			return "", fmt.Errorf("redirect response with no Location header")
		}
		return loc, nil
	default:
		return "", nil
	}
}

// downloadChunked downloads a blob by issuing parallel HTTP Range requests
// to the given URL (typically an S3 pre-signed URL). It returns the
// reassembled content.
//
// A plain http.Client is used for the Range requests because the URL
// already contains authentication (pre-signed). Adding Bearer tokens from
// the registry transport would confuse S3.
func downloadChunked(ctx context.Context, url string, size int64) ([]byte, error) {
	buf := make([]byte, size)

	chunkSize := defaultChunkSize
	numChunks := (size + chunkSize - 1) / chunkSize

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	sem := make(chan struct{}, maxChunkConcurrency)

	plainClient := &http.Client{}

	for i := int64(0); i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize - 1
		if end >= size {
			end = size - 1
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(start, end int64) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := downloadRange(ctx, plainClient, url, buf, start, end); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("chunk %d-%d: %w", start, end, err))
				mu.Unlock()
			}
		}(start, end)
	}

	wg.Wait()

	if len(errs) > 0 {
		return nil, fmt.Errorf("chunked download failed: %w", errs[0])
	}

	return buf, nil
}

// downloadRange downloads a single byte range into the target buffer slice.
func downloadRange(ctx context.Context, client *http.Client, url string, buf []byte, start, end int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("expected HTTP 206, got %d", resp.StatusCode)
	}

	expected := end - start + 1
	n, err := io.ReadFull(resp.Body, buf[start:start+expected])
	if err != nil {
		return fmt.Errorf("read error after %d bytes: %w", n, err)
	}

	return nil
}

// chunkedGet attempts a parallel chunked download of the blob at blobURL.
// It uses the authenticated transport to obtain the redirect URL from the
// registry, then downloads chunks in parallel directly from the storage
// backend. The returned data is verified against the expected digest.
//
// Returns nil, nil if chunked download is not applicable (blob too small
// or server does not redirect).
func chunkedGet(ctx context.Context, transport http.RoundTripper, blobURL string, size int64, dgst digest.Digest) ([]byte, error) {
	if size < defaultChunkedThreshold {
		return nil, nil
	}

	redirectURL, err := getRedirectURL(ctx, transport, blobURL)
	if err != nil {
		return nil, nil // fall back to normal download
	}
	if redirectURL == "" {
		return nil, nil // server doesn't redirect; fall back
	}

	data, err := downloadChunked(ctx, redirectURL, size)
	if err != nil {
		return nil, err
	}

	// Verify digest integrity.
	actual := digest.FromBytes(data)
	if actual != dgst {
		return nil, fmt.Errorf("digest mismatch: expected %s, got %s", dgst, actual)
	}

	return data, nil
}

// readSeekNopCloser wraps a bytes.Reader with a no-op Close method to
// satisfy io.ReadSeekCloser.
type readSeekNopCloser struct {
	*bytes.Reader
}

func (r readSeekNopCloser) Close() error {
	return nil
}
