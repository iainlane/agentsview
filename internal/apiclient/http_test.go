package apiclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retryTransport func(*http.Request) (*http.Response, error)

func (f retryTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestArchiveRebuildRetry(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		header, retryAfter, replay, cancel, notice bool
		wantCalls                                  int
	}{
		{"replays body after two rebuild responses", true, true, true, false, true, 3},
		{"library retry without notice", true, true, true, false, false, 3},
		{"missing retry hint", true, false, true, false, true, 1},
		{"ordinary 503", false, true, true, false, true, 1},
		{"body cannot be replayed", true, true, false, false, true, 1},
		{"cancel while waiting", true, true, true, true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				var notices bytes.Buffer
				var options []ArchiveOption
				if tc.notice {
					options = append(options, WithArchiveNotices(&notices))
				}
				defer cancel()
				calls := 0
				start := time.Now()
				client := &http.Client{Timeout: 100 * time.Millisecond, Transport: retryTransport(func(req *http.Request) (*http.Response, error) {
					calls++
					deadline, ok := req.Context().Deadline()
					require.True(t, ok)
					assert.Equal(t, 100*time.Millisecond, time.Until(deadline))
					assert.Empty(t, req.Header.Get(ArchiveStaleHeader))
					assert.Equal(t, http.MethodPost, req.Method)
					assert.Equal(t, "/api/v1/sync", req.URL.Path)
					body, err := io.ReadAll(req.Body)
					require.NoError(t, err)
					assert.Equal(t, `{"full":true}`, string(body))
					require.NoError(t, req.Body.Close())
					status := http.StatusServiceUnavailable
					if calls == 3 {
						status = http.StatusOK
					}
					header := http.Header{}
					if tc.retryAfter {
						header.Set("Retry-After", "2")
					}
					if tc.header {
						header.Set(ArchiveStaleHeader, "true")
					}
					if tc.cancel {
						go func() { time.Sleep(time.Second); cancel() }()
					}
					return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader("result"))}, nil
				})}
				client = NewArchiveHTTPClient(client)
				client.Timeout = 100 * time.Millisecond
				req, err := http.NewRequest(http.MethodPost, "http://example.test/api/v1/sync", strings.NewReader(`{"full":true}`))
				require.NoError(t, err)
				if !tc.replay {
					req.GetBody = nil
				}
				response, err := NewArchiveHTTPClient(client, options...).Do(req.WithContext(ctx))
				assert.Equal(t, tc.wantCalls, calls)
				wantNotices := 0
				if tc.header && tc.retryAfter && tc.replay && tc.notice {
					wantNotices = 1
				}
				assert.Equal(t, wantNotices, strings.Count(notices.String(), "waiting for archive rebuild"))
				if tc.cancel {
					require.ErrorIs(t, err, context.Canceled)
					assert.Nil(t, response)
					return
				}
				require.NoError(t, err)
				defer response.Body.Close()
				wantStatus := http.StatusServiceUnavailable
				if tc.wantCalls == 3 {
					wantStatus = http.StatusOK
					assert.Equal(t, 4*time.Second, time.Since(start))
				}
				assert.Equal(t, wantStatus, response.StatusCode)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Equal(t, "result", string(body))
			})
		})
	}
	for _, bodyTimeout := range []bool{false, true} {
		t.Run(fmt.Sprintf("attempt timeout body=%v", bodyTimeout), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if bodyTimeout {
						w.WriteHeader(http.StatusOK)
						require.NoError(t, http.NewResponseController(w).Flush())
					}
					<-r.Context().Done()
				}))
				client := *srv.Client()
				client.Timeout = 100 * time.Millisecond
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.com", nil)
				require.NoError(t, err)
				response, err := NewArchiveHTTPClient(&client).Do(req)
				if bodyTimeout {
					require.NoError(t, err)
					defer response.Body.Close()
					_, err = io.ReadAll(response.Body)
				}
				require.ErrorIs(t, err, context.DeadlineExceeded)
				assert.NoError(t, t.Context().Err())
			})
		})
	}
}

func TestArchiveNotices(t *testing.T) {
	for _, notice := range []bool{false, true} {
		t.Run(fmt.Sprint(notice), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var output bytes.Buffer
				var options []ArchiveOption
				if notice {
					options = append(options, WithArchiveNotices(&output))
				}
				calls := 0
				httpClient := &http.Client{Transport: retryTransport(func(*http.Request) (*http.Response, error) {
					calls++
					status := http.StatusOK
					if calls%2 == 1 {
						status = http.StatusServiceUnavailable
					}
					return &http.Response{StatusCode: status, Header: http.Header{ArchiveStaleHeader: {"true"}, "Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader(`{"sessions":[],"machine_labels":{}}`))}, nil
				})}
				for range 2 {
					api, err := NewHTTPClient("http://example.test", "", httpClient, options...)
					require.NoError(t, err)
					_, err = api.GetAPIV1SessionsWithResponse(t.Context(), &GetAPIV1SessionsRequestOptions{})
					require.NoError(t, err)
				}
				assert.Equal(t, 4, calls)
				if notice {
					assert.Equal(t, "waiting for archive rebuild\nWarning: Archive results may be stale.\n", output.String())
				} else {
					assert.Empty(t, output.String())
				}
			})
		})
	}
}
