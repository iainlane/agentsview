package db

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

func TestArchiveOnlyPreservesPendingRebuild(t *testing.T) {
	for _, tc := range []struct {
		name        string
		version     int
		archiveOnly bool
		wantRaw     int
		stale       bool
	}{
		{"pending archive", dataVersion - 1, true, (1 << 20) + dataVersion - 1, true},
		{"current archive", dataVersion, true, (1 << 20) + dataVersion, false},
		{"ordinary archive", dataVersion, false, dataVersion, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := testDB(t)
			ctx := t.Context()
			require.NoError(t, d.Update(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", tc.version))
				return err
			}))
			if tc.archiveOnly {
				require.NoError(t, d.EnableArchiveOnly(ctx))
				require.NoError(t, d.EnableArchiveOnly(ctx))
			}
			path := d.Path()
			require.NoError(t, d.Close())
			reopened, err := OpenIsolatedContext(ctx, path)
			require.NoError(t, err)
			defer reopened.Close()
			assert.Equal(t, tc.stale, reopened.NeedsResync())
			var raw int
			require.NoError(t, reopened.Reader().QueryRow(ctx, "PRAGMA user_version").Scan(&raw))
			assert.Equal(t, tc.wantRaw, raw)
			if tc.archiveOnly {
				require.ErrorIs(t, reopened.RequireSourceSync(ctx), ErrArchiveOnly)
				assert.Greater(t, raw, dataVersion, "readers without archive-only support reject the encoded version")
			} else {
				assert.NoError(t, reopened.RequireSourceSync(ctx))
			}
		})
	}
}

func TestArchiveOnlySurvivesRebuildMetadataCopy(t *testing.T) {
	source := testDB(t)
	ctx := t.Context()
	require.NoError(t, source.EnableArchiveOnly(ctx))
	replacement := testDB(t)
	require.NoError(t, replacement.CopySessionMetadataFrom(source.Path()))
	require.NoError(t, replacement.setDataVersion(ctx))
	path := replacement.Path()
	require.NoError(t, replacement.Close())
	reopened, err := OpenReadOnly(ctx, path)
	require.NoError(t, err)
	defer reopened.Close()
	assert.False(t, reopened.NeedsResync())
	require.ErrorIs(t, reopened.RequireSourceSync(ctx), ErrArchiveOnly)
	var raw int
	require.NoError(t, reopened.Reader().QueryRow(ctx, "PRAGMA user_version").Scan(&raw))
	assert.Equal(t, (1<<20)+dataVersion, raw)
}

func TestArchiveResyncState(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		version                                   int
		pending, repair, archiveOnly, needs, full bool
	}{
		{name: "current", version: dataVersion},
		{name: "data stale", version: 129, needs: true, full: true},
		{name: "scoped version", version: dataVersion - 1, needs: true},
		{name: "scoped pending", version: dataVersion, pending: true, needs: true},
		{name: "schema repair", version: dataVersion, repair: true, needs: true},
		{name: "archive only", version: dataVersion - 1, archiveOnly: true, needs: true, full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(InstallDataVersionScopesForTest(map[int][]parser.AgentType{dataVersion: {parser.AgentClaude}}))
			d := testDB(t)
			require.NoError(t, d.Update(t.Context(), func(tx *sql.Tx) error {
				_, err := tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", tc.version))
				if err != nil {
					return err
				}
				if tc.pending {
					_, err = tx.Exec("INSERT INTO stats(key,value) VALUES ('scoped_upgrade_pending',1)")
				}
				if tc.repair {
					_, err = tx.Exec("ALTER TABLE tool_calls DROP COLUMN result_content_length")
				}
				return err
			}))
			if tc.archiveOnly {
				require.NoError(t, d.EnableArchiveOnly(t.Context()))
			}
			path := d.Path()
			require.NoError(t, d.Close())
			needs, full, err := ArchiveResyncState(t.Context(), path)
			require.NoError(t, err)
			assert.Equal(t, tc.needs, needs)
			assert.Equal(t, tc.full, full)
		})
	}
}
