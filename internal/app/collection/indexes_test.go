package collection

import (
	"reflect"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

func TestParseIndexSpecsEmpty(t *testing.T) {
	specs, err := ParseIndexSpecs("  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if specs != nil {
		t.Fatalf("expected nil, got %v", specs)
	}
}

func TestParseIndexSpecsCSV(t *testing.T) {
	specs, err := ParseIndexSpecs("email, role ,")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []domain.IndexSpec{
		{Name: "email", Fields: []string{"email"}},
		{Name: "role", Fields: []string{"role"}},
	}
	if !reflect.DeepEqual(specs, expected) {
		t.Fatalf("expected %v, got %v", expected, specs)
	}
}

func TestParseIndexSpecsJSONStrings(t *testing.T) {
	specs, err := ParseIndexSpecs(`["email","role"]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []domain.IndexSpec{
		{Name: "email", Fields: []string{"email"}},
		{Name: "role", Fields: []string{"role"}},
	}
	if !reflect.DeepEqual(specs, expected) {
		t.Fatalf("expected %v, got %v", expected, specs)
	}
}

func TestParseIndexSpecsJSONObjects(t *testing.T) {
	specs, err := ParseIndexSpecs(`[
		{"name":"by_email","fields":["email"],"unique":true},
		{"name":"by_org_status","fields":["org_id","status"]}
	]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []domain.IndexSpec{
		{Name: "by_email", Fields: []string{"email"}, Unique: true},
		{Name: "by_org_status", Fields: []string{"org_id", "status"}},
	}
	if !reflect.DeepEqual(specs, expected) {
		t.Fatalf("expected %v, got %v", expected, specs)
	}
}

func TestParseIndexSpecsInvalidJSON(t *testing.T) {
	if _, err := ParseIndexSpecs(`[`); err == nil {
		t.Fatalf("expected error for malformed JSON")
	}
}

func TestNormalizeIndexSpecsDerivesNameAndDedups(t *testing.T) {
	specs, err := NormalizeIndexSpecs([]domain.IndexSpec{
		{Fields: []string{"org_id", "status"}},
		{Name: "by_email", Fields: []string{" email "}, Unique: true},
		{Name: "by_email", Fields: []string{"email"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []domain.IndexSpec{
		{Name: "by_email", Fields: []string{"email"}, Unique: true},
		{Name: "org_id_status", Fields: []string{"org_id", "status"}},
	}
	if !reflect.DeepEqual(specs, expected) {
		t.Fatalf("expected %v, got %v", expected, specs)
	}
}

func TestNormalizeIndexSpecsRejectsEmptyFields(t *testing.T) {
	if _, err := NormalizeIndexSpecs([]domain.IndexSpec{{Name: "bad", Fields: []string{" "}}}); err == nil {
		t.Fatalf("expected error")
	}
}
