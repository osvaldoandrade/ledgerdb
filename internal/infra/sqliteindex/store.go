package sqliteindex

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	indexapp "github.com/codecompany/ledgerdb/internal/app/index"
	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

type OpenOptions struct {
	Fast bool
}

func Open(path string) (*Store, error) {
	return OpenWithOptions(path, OpenOptions{})
}

func OpenWithOptions(path string, opts OpenOptions) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("sqlite path required")
	}

	if shouldCreateDir(path) {
		dir := filepath.Dir(path)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create sqlite dir: %w", err)
			}
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &Store{db: db}
	if err := store.applyPragmas(context.Background(), opts); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.initSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) GetState(ctx context.Context) (indexapp.State, error) {
	var lastCommit string
	var lastStateTree string
	err := s.db.QueryRowContext(ctx, "SELECT last_commit, last_state_tree FROM ledger_index_state WHERE id = 1").Scan(&lastCommit, &lastStateTree)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return indexapp.State{}, nil
		}
		return indexapp.State{}, fmt.Errorf("read index state: %w", err)
	}
	return indexapp.State{LastCommit: lastCommit, LastStateTree: lastStateTree}, nil
}

func (s *Store) Begin(ctx context.Context) (indexapp.StoreTx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin index transaction: %w", err)
	}
	return &storeTx{tx: tx, tableCache: make(map[string]string)}, nil
}

func (s *Store) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin reset transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.QueryContext(ctx, "SELECT table_name FROM collection_registry")
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}
	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan collection table: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close collection rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate collection rows: %w", err)
	}

	for _, tableName := range tables {
		stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(tableName))
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("drop table %s: %w", tableName, err)
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM collection_registry"); err != nil {
		return fmt.Errorf("clear collection registry: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE ledger_index_state SET last_commit = '', last_state_tree = '' WHERE id = 1"); err != nil {
		return fmt.Errorf("reset index state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reset: %w", err)
	}
	return nil
}

func (s *Store) initSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign keys: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS ledger_index_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			last_commit TEXT NOT NULL DEFAULT '',
			last_state_tree TEXT NOT NULL DEFAULT ''
		)
	`); err != nil {
		return fmt.Errorf("create state table: %w", err)
	}
	if err := s.ensureStateColumns(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS collection_registry (
			collection TEXT PRIMARY KEY,
			table_name TEXT NOT NULL UNIQUE
		)
	`); err != nil {
		return fmt.Errorf("create collection registry: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO ledger_index_state (id, last_commit, last_state_tree) VALUES (1, '', '')
	`); err != nil {
		return fmt.Errorf("seed state table: %w", err)
	}
	return nil
}

func (s *Store) ensureStateColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(ledger_index_state)")
	if err != nil {
		return fmt.Errorf("read state table info: %w", err)
	}
	defer rows.Close()

	hasStateTree := false
	for rows.Next() {
		var cid int
		var name string
		var colType string
		var notNull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			return fmt.Errorf("scan state table info: %w", err)
		}
		if name == "last_state_tree" {
			hasStateTree = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate state table info: %w", err)
	}
	if hasStateTree {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, "ALTER TABLE ledger_index_state ADD COLUMN last_state_tree TEXT NOT NULL DEFAULT ''"); err != nil {
		return fmt.Errorf("add state tree column: %w", err)
	}
	return nil
}

func (s *Store) applyPragmas(ctx context.Context, opts OpenOptions) error {
	if !opts.Fast {
		return nil
	}
	var mode string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		return fmt.Errorf("set journal_mode: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA synchronous = NORMAL"); err != nil {
		return fmt.Errorf("set synchronous: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA temp_store = MEMORY"); err != nil {
		return fmt.Errorf("set temp_store: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA cache_size = -20000"); err != nil {
		return fmt.Errorf("set cache_size: %w", err)
	}
	return nil
}

type storeTx struct {
	tx         *sql.Tx
	tableCache map[string]string
}

func (s *storeTx) EnsureCollection(ctx context.Context, collection string) (string, error) {
	tableName, err := s.lookupCollection(ctx, collection)
	if err == nil {
		s.tableCache[collection] = tableName
		return tableName, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("lookup collection: %w", err)
	}

	tableName = tableNameForCollection(collection)
	if err := s.createCollectionTable(ctx, tableName); err != nil {
		return "", err
	}
	if _, err := s.tx.ExecContext(ctx, `
		INSERT INTO collection_registry (collection, table_name) VALUES (?, ?)
	`, collection, tableName); err != nil {
		return "", fmt.Errorf("register collection: %w", err)
	}
	s.tableCache[collection] = tableName
	return tableName, nil
}

func (s *storeTx) GetDoc(ctx context.Context, collection, docID string) (indexapp.DocRecord, bool, error) {
	tableName, err := s.lookupCollection(ctx, collection)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return indexapp.DocRecord{}, false, nil
		}
		return indexapp.DocRecord{}, false, fmt.Errorf("lookup collection: %w", err)
	}

	query := fmt.Sprintf(`
		SELECT doc_id, payload, tx_hash, tx_id, op, schema_version, updated_at, deleted
		FROM %s WHERE doc_id = ?
	`, quoteIdent(tableName))
	var record indexapp.DocRecord
	var deleted int
	if err := s.tx.QueryRowContext(ctx, query, docID).Scan(
		&record.DocID,
		&record.Payload,
		&record.TxHash,
		&record.TxID,
		&record.Op,
		&record.SchemaVersion,
		&record.UpdatedAt,
		&deleted,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return indexapp.DocRecord{}, false, nil
		}
		return indexapp.DocRecord{}, false, fmt.Errorf("read doc: %w", err)
	}
	record.Deleted = deleted != 0
	return record, true, nil
}

func (s *storeTx) UpsertDoc(ctx context.Context, collection string, record indexapp.DocRecord) error {
	tableName, err := s.lookupCollection(ctx, collection)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("collection not initialized: %s", collection)
		}
		return fmt.Errorf("lookup collection: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (doc_id, payload, tx_hash, tx_id, op, schema_version, updated_at, deleted)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(doc_id) DO UPDATE SET
			payload = excluded.payload,
			tx_hash = excluded.tx_hash,
			tx_id = excluded.tx_id,
			op = excluded.op,
			schema_version = excluded.schema_version,
			updated_at = excluded.updated_at,
			deleted = excluded.deleted
	`, quoteIdent(tableName))

	deleted := 0
	if record.Deleted {
		deleted = 1
	}

	if _, err := s.tx.ExecContext(ctx, query,
		record.DocID,
		record.Payload,
		record.TxHash,
		record.TxID,
		record.Op,
		record.SchemaVersion,
		record.UpdatedAt,
		deleted,
	); err != nil {
		return fmt.Errorf("upsert doc: %w", err)
	}
	return nil
}

func (s *storeTx) SetState(ctx context.Context, state indexapp.State) error {
	if _, err := s.tx.ExecContext(ctx, `
		INSERT INTO ledger_index_state (id, last_commit, last_state_tree) VALUES (1, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			last_commit = excluded.last_commit,
			last_state_tree = excluded.last_state_tree
	`, state.LastCommit, state.LastStateTree); err != nil {
		return fmt.Errorf("update index state: %w", err)
	}
	return nil
}

func (s *storeTx) Commit() error {
	return s.tx.Commit()
}

func (s *storeTx) Rollback() error {
	return s.tx.Rollback()
}

func (s *storeTx) lookupCollection(ctx context.Context, collection string) (string, error) {
	if tableName, ok := s.tableCache[collection]; ok {
		return tableName, nil
	}
	var tableName string
	err := s.tx.QueryRowContext(ctx, `
		SELECT table_name FROM collection_registry WHERE collection = ?
	`, collection).Scan(&tableName)
	if err != nil {
		return "", err
	}
	return tableName, nil
}

func (s *storeTx) createCollectionTable(ctx context.Context, tableName string) error {
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			doc_id TEXT PRIMARY KEY,
			payload BLOB,
			tx_hash TEXT NOT NULL,
			tx_id TEXT NOT NULL,
			op TEXT NOT NULL,
			schema_version TEXT,
			updated_at INTEGER NOT NULL,
			deleted INTEGER NOT NULL CHECK (deleted IN (0, 1))
		)
	`, quoteIdent(tableName))
	if _, err := s.tx.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create collection table: %w", err)
	}
	return nil
}

func tableNameForCollection(collection string) string {
	return "collection_" + collection
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func shouldCreateDir(path string) bool {
	if path == ":memory:" {
		return false
	}
	if strings.HasPrefix(path, "file:") {
		return false
	}
	return true
}
