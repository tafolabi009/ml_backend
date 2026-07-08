package handlers

import (
	"strings"
	"testing"
)

func TestLuhnOK(t *testing.T) {
	if !luhnOK("4539148803436467") { // valid Visa test number
		t.Error("valid card rejected")
	}
	if luhnOK("1234567890123456") {
		t.Error("invalid card accepted")
	}
	if luhnOK("4539-1488") { // non-digit
		t.Error("non-digit accepted")
	}
}

func TestInferColumnType(t *testing.T) {
	cases := []struct {
		vals []string
		want string
	}{
		{[]string{"1", "2", "3"}, "integer"},
		{[]string{"1.5", "2", "3.25"}, "float"},
		{[]string{"true", "false", "true"}, "boolean"},
		{[]string{"2026-01-02", "2026-03-04", "2026-05-06"}, "datetime"},
		{[]string{"alpha", "beta", "42x"}, "string"},
		{[]string{"", "NA", "null"}, "string"},
	}
	for _, c := range cases {
		if got := inferColumnType(c.vals); got != c.want {
			t.Errorf("inferColumnType(%v) = %q, want %q", c.vals, got, c.want)
		}
	}
}

func TestDeriveRowFindings(t *testing.T) {
	p := &parsedDataset{
		columns: []string{"email", "score"},
		rows:    make([][]string, 0, 60),
	}
	for i := 0; i < 60; i++ {
		p.rows = append(p.rows, []string{"user@example.com", "1.0"})
	}
	p.rows[10] = []string{"", "1.0"}            // missing value in >95% filled column
	p.rows[20] = []string{"123-45-6789", "1.0"} // SSN
	findings := deriveRowFindings(p)

	var sawMissing, sawSSN bool
	for _, f := range findings {
		switch f["issue"] {
		case "missing_value":
			if f["row_index"] == 10 {
				sawMissing = true
			}
		case "pii_ssn":
			if f["severity"] != "critical" {
				t.Errorf("ssn severity = %v, want critical", f["severity"])
			}
			sawSSN = true
		}
	}
	if !sawMissing {
		t.Error("missing_value finding not produced")
	}
	if !sawSSN {
		t.Error("pii_ssn finding not produced")
	}
}

func TestCanonicalJSONDeterministic(t *testing.T) {
	b1, err := canonicalJSON(map[string]interface{}{"b": 2, "a": "x & y"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b1) != `{"a":"x & y","b":2}` {
		t.Errorf("canonical form = %s", b1)
	}
	if strings.Contains(string(b1), `\u0026`) {
		t.Error("HTML escaping must be disabled for cross-language verification")
	}
}

func TestPipelineStageKeys(t *testing.T) {
	want := []string{"queued", "sampling", "proxy_training", "extrapolation", "report"}
	if len(pipelineStages) != len(want) {
		t.Fatalf("stage count = %d, want %d", len(pipelineStages), len(want))
	}
	for i, s := range pipelineStages {
		if s.key != want[i] {
			t.Errorf("stage[%d].key = %q, want %q", i, s.key, want[i])
		}
	}
}
