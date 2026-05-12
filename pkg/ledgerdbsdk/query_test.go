package ledgerdbsdk

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/infra/sqliteindex"
)

func TestQueryCursorRoundTrip(t *testing.T) {
	token := encodeQueryCursor(42)
	if token == "" {
		t.Fatal("expected non-empty token")
	}
	decoded, err := decodeQueryCursor(token)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Offset != 42 || decoded.Version != QueryCursorVersion {
		t.Fatalf("round-trip mismatch: %+v", decoded)
	}
}

func TestDecodeQueryCursorRejectsGarbage(t *testing.T) {
	if _, err := decodeQueryCursor("!!!not-base64!!!"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestHasOrderBy(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"SELECT * FROM t ORDER BY id", true},
		{"select * from t order by id", true},
		{"SELECT * FROM t", false},
		{"SELECT * FROM t WHERE x = 1", false},
	}
	for _, c := range cases {
		if got := hasOrderBy(c.query); got != c.want {
			t.Errorf("hasOrderBy(%q) = %v, want %v", c.query, got, c.want)
		}
	}
}

func TestQueryPaginatedAgainstSQLite(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	store, err := sqliteindex.Open(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer store.Close()

	db := store.DB()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= 7; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO items (id, name) VALUES (?, ?)`, i, "item"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Build a client with the index already opened.
	client := &Client{cfg: Config{RepoPath: dir, Index: IndexConfig{DBPath: dbPath}}}
	client.indexStore = store
	client.db = db

	// Page 1: 3 rows, expect next cursor.
	rows, next, err := client.QueryPaginated(ctx, `SELECT id, name FROM items ORDER BY id`, nil, "", 3)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("page1: expected 3 rows, got %d", len(rows))
	}
	if got := rows[0]["id"]; got != int64(1) {
		t.Fatalf("page1[0]: expected id=1, got %v", got)
	}
	if next == "" {
		t.Fatal("page1: expected next cursor")
	}

	// Page 2.
	rows2, next2, err := client.QueryPaginated(ctx, `SELECT id, name FROM items ORDER BY id`, nil, next, 3)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(rows2) != 3 {
		t.Fatalf("page2: expected 3 rows, got %d", len(rows2))
	}
	if got := rows2[0]["id"]; got != int64(4) {
		t.Fatalf("page2[0]: expected id=4, got %v", got)
	}
	if next2 == "" {
		t.Fatal("page2: expected next cursor")
	}

	// Page 3: last page (1 row), no next cursor.
	rows3, next3, err := client.QueryPaginated(ctx, `SELECT id, name FROM items ORDER BY id`, nil, next2, 3)
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	if len(rows3) != 1 {
		t.Fatalf("page3: expected 1 row, got %d", len(rows3))
	}
	if got := rows3[0]["id"]; got != int64(7) {
		t.Fatalf("page3[0]: expected id=7, got %v", got)
	}
	if next3 != "" {
		t.Fatalf("page3: expected empty cursor, got %q", next3)
	}
}

func TestQueryPaginatedRejectsMissingOrderBy(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := sqliteindex.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	client := &Client{cfg: Config{RepoPath: dir, Index: IndexConfig{DBPath: dbPath}}}
	client.indexStore = store
	client.db = store.DB()

	_, _, err = client.QueryPaginated(context.Background(), `SELECT 1`, nil, "", 10)
	if !errors.Is(err, ErrMissingOrderBy) {
		t.Fatalf("expected ErrMissingOrderBy, got %v", err)
	}
}

func TestQueryPaginatedRejectsInvalidCursor(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := sqliteindex.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	client := &Client{cfg: Config{RepoPath: dir, Index: IndexConfig{DBPath: dbPath}}}
	client.indexStore = store
	client.db = store.DB()

	_, _, err = client.QueryPaginated(context.Background(), `SELECT 1 ORDER BY 1`, nil, "!!!garbage!!!", 10)
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}
