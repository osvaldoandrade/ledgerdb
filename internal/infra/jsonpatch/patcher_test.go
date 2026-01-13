package jsonpatch

import (
	"context"
	"testing"
)

func TestApplyPatch(t *testing.T) {
	doc := []byte(`{"name":"Ada"}`)
	patch := []byte(`[{"op":"replace","path":"/name","value":"Grace"}]`)

	out, err := (Patcher{}).Apply(context.Background(), doc, patch)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if string(out) != `{"name":"Grace"}` {
		t.Fatalf("unexpected output: %s", string(out))
	}
}
