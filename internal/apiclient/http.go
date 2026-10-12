package apiclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doordash-oss/oapi-codegen-dd/v3/pkg/runtime"

	"go.kenn.io/agentsview/internal/httputil"
)

// NewHTTPClient connects the generated API to the selected daemon transport.
func NewHTTPClient(baseURL, token string, client *http.Client, options ...ArchiveOption) (*Client, error) {
	origin, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	origin.Path, origin.RawPath, origin.RawQuery, origin.Fragment = "", "", "", ""
	return NewDefaultClient(strings.TrimSuffix(baseURL, "/"), runtime.WithHTTPClient(httpTransport{NewArchiveHTTPClient(client, options...)}), runtime.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
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
	return c.Client.Do(req.WithContext(ctx))
}

const (
	ArchiveStaleHeader  = "X-Agentsview-Archive-Stale"
	ArchiveStaleWarning = "Archive results may be stale."
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

// ArchiveOption enables archive notices for an HTTP client.
type ArchiveOption func(*archiveTransport)

// WithArchiveNotices prints each notice once across clients sharing this option.
func WithArchiveNotices(writer io.Writer) ArchiveOption {
	var waiting, stale sync.Once
	return func(transport *archiveTransport) {
		transport.onWait = func() { waiting.Do(func() { fmt.Fprintln(writer, "waiting for archive rebuild") }) }
		transport.onStale = func() { stale.Do(func() { fmt.Fprintln(writer, "Warning: "+ArchiveStaleWarning) }) }
	}
}

// NewArchiveHTTPClient copies the client and installs archive retry handling.
func NewArchiveHTTPClient(client *http.Client, options ...ArchiveOption) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	base := copy.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	if _, installed := base.(*archiveTransport); installed && len(options) == 0 {
		return &copy
	}
	transport := &archiveTransport{base: base}
	for _, option := range options {
		option(transport)
	}
	copy.Transport = transport
	return &copy
}

type archiveTransport struct {
	base            http.RoundTripper
	onWait, onStale func()
}

func (t *archiveTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for {
		response, err := t.base.RoundTrip(req)
		if err != nil {
			return response, err
		}
		ObserveArchiveFreshness(req.Context(), response)
		if response.StatusCode >= 200 && response.StatusCode < 300 && response.Header.Get(ArchiveStaleHeader) == "true" && t.onStale != nil {
			t.onStale()
		}
		delay := httputil.ParseRetryAfter(response.Header.Get("Retry-After"))
		if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get(ArchiveStaleHeader) != "true" || delay <= 0 || req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
			return response, nil
		}
		response.Body.Close()
		if t.onWait != nil {
			t.onWait()
		}
		timer := time.NewTimer(delay)
		select {
		case <-req.Context().Done():
			timer.Stop()
			return nil, req.Context().Err()
		case <-timer.C:
		}
		req = req.Clone(req.Context())
		req.Header.Set(ArchiveStaleHeader, "true")
		if req.GetBody != nil {
			req.Body, err = req.GetBody()
			if err != nil {
				return nil, err
			}
		}
	}
}
