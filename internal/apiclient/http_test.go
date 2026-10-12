package apiclient

import (
	"context"
	"io"
	"net/http"
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
		name                   string
		header, replay, cancel bool
		wantCalls              int
	}{
		{"replays body after two rebuild responses", true, true, false, 3},
		{"ordinary 503", false, true, false, 1},
		{"body cannot be replayed", true, false, false, 1},
		{"cancel while waiting", true, true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				calls := 0
				start := time.Now()
				client := &http.Client{Transport: retryTransport(func(req *http.Request) (*http.Response, error) {
					calls++
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
					header := http.Header{"Retry-After": []string{"2"}}
					if tc.header {
						header.Set(ArchiveRebuildingHeader, "true")
					}
					if tc.cancel {
						go func() { time.Sleep(time.Second); cancel() }()
					}
					return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader("result"))}, nil
				})}
				req, err := http.NewRequest(http.MethodPost, "http://example.test/api/v1/sync", strings.NewReader(`{"full":true}`))
				require.NoError(t, err)
				if !tc.replay {
					req.GetBody = nil
				}
				response, err := Do(ctx, client, req)
				assert.Equal(t, tc.wantCalls, calls)
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
}
