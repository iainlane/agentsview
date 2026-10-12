package apiclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/doordash-oss/oapi-codegen-dd/v3/pkg/runtime"

	"go.kenn.io/agentsview/internal/httputil"
)

// NewHTTPClient connects the generated API to the selected daemon transport.
func NewHTTPClient(baseURL, token string, client *http.Client) (*Client, error) {
	origin, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	origin.Path, origin.RawPath, origin.RawQuery, origin.Fragment = "", "", "", ""
	return NewDefaultClient(strings.TrimSuffix(baseURL, "/"), runtime.WithHTTPClient(httpTransport{client}), runtime.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if req.Method != http.MethodGet {
			req.Header.Set("Origin", origin.String())
		}
		return nil
	}))
}

type httpTransport struct{ *http.Client }

func (c httpTransport) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	return Do(ctx, c.Client, req)
}

const (
	ArchiveRebuildingHeader = "X-Agentsview-Archive-Rebuilding"
	ArchiveStaleHeader      = "X-Agentsview-Archive-Stale"
	ArchiveStaleWarning     = "Archive results may be stale."
)

type archiveFreshnessKey struct{}

type archiveFreshness struct {
	stale   atomic.Bool
	onStale func()
}

// TrackArchiveFreshness collects stale responses and calls onStale once per operation.
func TrackArchiveFreshness(ctx context.Context, onStale func()) (context.Context, func() bool) {
	freshness := &archiveFreshness{onStale: onStale}
	return context.WithValue(ctx, archiveFreshnessKey{}, freshness), freshness.stale.Load
}

// ObserveArchiveFreshness records freshness for successful raw HTTP responses.
func ObserveArchiveFreshness(ctx context.Context, response *http.Response) {
	if response == nil || response.StatusCode < 200 || response.StatusCode >= 300 || response.Header.Get(ArchiveStaleHeader) != "true" {
		return
	}
	if freshness, ok := ctx.Value(archiveFreshnessKey{}).(*archiveFreshness); ok && freshness.stale.CompareAndSwap(false, true) && freshness.onStale != nil {
		freshness.onStale()
	}
}

// Do retries writes refused before dispatch during a startup archive rebuild.
func Do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	req = req.WithContext(ctx)
	waiting := false
	for {
		response, err := client.Do(req)
		if err != nil || response.StatusCode != http.StatusServiceUnavailable || response.Header.Get(ArchiveRebuildingHeader) != "true" || (req.Body != nil && req.Body != http.NoBody && req.GetBody == nil) {
			ObserveArchiveFreshness(ctx, response)
			return response, err
		}
		delay := httputil.ParseRetryAfter(response.Header.Get("Retry-After"))
		if delay <= 0 {
			delay = time.Second
		}
		response.Body.Close()
		if !waiting {
			fmt.Fprintln(os.Stderr, "waiting for archive rebuild")
			waiting = true
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if req.GetBody != nil {
			req.Body, err = req.GetBody()
			if err != nil {
				return nil, err
			}
		}
	}
}
