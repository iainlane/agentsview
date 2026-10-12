package apiclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawRequestTracksFreshnessWithoutReadingBodies(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/v1/remote-sync/archive", r.URL.Path)
				if attempts.Add(1) == 1 {
					w.Header().Set(ArchiveRebuildingHeader, "true")
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "application/x-tar")
				w.Header().Set(ArchiveStaleHeader, "true")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "first")
				w.(http.Flusher).Flush()
				<-release
				_, _ = io.WriteString(w, "last")
			}))
			defer server.Close()
			defer finish()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx, stale := TrackArchiveFreshness(ctx, nil)
			response, err := RawRequest(server.URL, server.Client(), func(client *Client) error {
				_, err := client.PostAPIV1RemoteSyncArchiveWithResponse(ctx, &PostAPIV1RemoteSyncArchiveRequestOptions{})
				return err
			})
			require.NoError(t, err)
			require.NotNil(t, response)
			assert.EqualValues(t, 2, attempts.Load())
			defer response.Body.Close()
			assert.Equal(t, status, response.StatusCode)
			assert.Equal(t, status == http.StatusOK, stale())
			first := make([]byte, 5)
			_, err = io.ReadFull(response.Body, first)
			require.NoError(t, err)
			assert.Equal(t, "first", string(first))
			finish()
			rest, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			assert.Equal(t, "last", string(rest))
		})
	}
}
