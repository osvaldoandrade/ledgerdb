package collection

import (
	"context"
	"errors"
	"testing"
)

type fakeSchemaSource struct {
	data []byte
	err  error
}

func (f *fakeSchemaSource) ReadSchema(ctx context.Context, path string) ([]byte, error) {
	return f.data, f.err
}

type fakeCollectionStore struct {
	collection string
	schema     []byte
	indexes    []string
	err        error
}

func (f *fakeCollectionStore) WriteSchema(ctx context.Context, repoPath, collection string, schema []byte, indexes []string) error {
	f.collection = collection
	f.schema = schema
	f.indexes = indexes
	return f.err
}

type fakeSchemaValidator struct {
	err error
}

func (f fakeSchemaValidator) Validate(ctx context.Context, schema []byte) error {
	return f.err
}

func TestServiceRequiresName(t *testing.T) {
	service := NewService(&fakeCollectionStore{}, &fakeSchemaSource{}, fakeSchemaValidator{})
	err := service.Apply(context.Background(), "repo", " ", "schema.json", nil)
	if !errors.Is(err, ErrCollectionRequired) {
		t.Fatalf("expected ErrCollectionRequired, got %v", err)
	}
}

func TestServiceRejectsInvalidName(t *testing.T) {
	service := NewService(&fakeCollectionStore{}, &fakeSchemaSource{}, fakeSchemaValidator{})
	err := service.Apply(context.Background(), "repo", "users/../etc", "schema.json", nil)
	if !errors.Is(err, ErrInvalidCollectionName) {
		t.Fatalf("expected ErrInvalidCollectionName, got %v", err)
	}
}

func TestServiceRequiresSchemaPath(t *testing.T) {
	service := NewService(&fakeCollectionStore{}, &fakeSchemaSource{}, fakeSchemaValidator{})
	err := service.Apply(context.Background(), "repo", "users", " ", nil)
	if !errors.Is(err, ErrSchemaPathRequired) {
		t.Fatalf("expected ErrSchemaPathRequired, got %v", err)
	}
}

func TestServiceValidatesJSON(t *testing.T) {
	service := NewService(&fakeCollectionStore{}, &fakeSchemaSource{data: []byte("{")}, fakeSchemaValidator{})
	err := service.Apply(context.Background(), "repo", "users", "schema.json", nil)
	if !errors.Is(err, ErrSchemaInvalidJSON) {
		t.Fatalf("expected ErrSchemaInvalidJSON, got %v", err)
	}
}

func TestServiceNormalizesIndexes(t *testing.T) {
	store := &fakeCollectionStore{}
	service := NewService(store, &fakeSchemaSource{data: []byte(`{"type":"object"}`)}, fakeSchemaValidator{})
	err := service.Apply(context.Background(), "repo", "users", "schema.json", []string{" email", "", "role", "email"})
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	expected := []string{"email", "role"}
	if len(store.indexes) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, store.indexes)
	}
	for i, value := range expected {
		if store.indexes[i] != value {
			t.Fatalf("expected %v, got %v", expected, store.indexes)
		}
	}
}

func TestServiceRunsSchemaValidator(t *testing.T) {
	validatorErr := errors.New("invalid schema")
	service := NewService(&fakeCollectionStore{}, &fakeSchemaSource{data: []byte(`{"type":"object"}`)}, fakeSchemaValidator{err: validatorErr})

	err := service.Apply(context.Background(), "repo", "users", "schema.json", nil)
	if !errors.Is(err, validatorErr) {
		t.Fatalf("expected validator error, got %v", err)
	}
}
