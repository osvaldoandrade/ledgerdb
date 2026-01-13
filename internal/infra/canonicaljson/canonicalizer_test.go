package canonicaljson

import (
	"context"
	"testing"
)

func TestCanonicalizeSortsKeys(t *testing.T) {
	input := []byte(`{"b":1,"a":2}`)
	out, err := (Canonicalizer{}).Canonicalize(context.Background(), input)
	if err != nil {
		t.Fatalf("Canonicalize returned error: %v", err)
	}

	expected := `{"a":2,"b":1}`
	if string(out) != expected {
		t.Fatalf("expected %s, got %s", expected, string(out))
	}
}
