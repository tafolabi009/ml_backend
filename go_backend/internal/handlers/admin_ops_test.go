package handlers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func TestValidateRenameName(t *testing.T) {
	cases := []struct {
		in    string
		want  string
		valid bool
	}{
		{"My validation", "My validation", true},
		{"  trimmed  ", "trimmed", true},
		{"", "", false},
		{"   ", "", false},
		{string(make([]byte, 121)), "", false}, // 121 chars
		{"x", "x", true},
	}
	for _, c := range cases {
		got, ok := ValidateRenameName(c.in)
		if ok != c.valid {
			t.Errorf("ValidateRenameName(%q) valid=%v, want %v", c.in, ok, c.valid)
		}
		if ok && got != c.want {
			t.Errorf("ValidateRenameName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func signPaddle(ts int64, body, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(fmt.Sprintf("%d", ts)))
	mac.Write([]byte(":"))
	mac.Write([]byte(body))
	return fmt.Sprintf("ts=%d;h1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
}

func TestVerifyPaddleSignature(t *testing.T) {
	secret := "whsec_test"
	body := `{"event_type":"transaction.completed"}`
	now := time.Unix(1_700_000_000, 0)

	valid := signPaddle(now.Unix(), body, secret)
	if !VerifyPaddleSignature(valid, []byte(body), secret, now) {
		t.Error("valid signature rejected")
	}
	// Wrong secret.
	if VerifyPaddleSignature(signPaddle(now.Unix(), body, "wrong"), []byte(body), secret, now) {
		t.Error("signature with wrong secret accepted")
	}
	// Tampered body.
	if VerifyPaddleSignature(valid, []byte(body+"x"), secret, now) {
		t.Error("tampered body accepted")
	}
	// Stale timestamp (>5 min).
	stale := signPaddle(now.Unix()-600, body, secret)
	if VerifyPaddleSignature(stale, []byte(body), secret, now) {
		t.Error("stale timestamp accepted")
	}
	// Missing/blank inputs.
	if VerifyPaddleSignature("", []byte(body), secret, now) {
		t.Error("empty header accepted")
	}
	if VerifyPaddleSignature(valid, []byte(body), "", now) {
		t.Error("empty secret accepted")
	}
}

func TestMetricsGranularityTrunc(t *testing.T) {
	for in, want := range map[string]string{"": "day", "day": "day", "week": "week", "month": "month"} {
		got, ok := MetricsGranularityTrunc(in)
		if !ok || got != want {
			t.Errorf("MetricsGranularityTrunc(%q) = %q,%v want %q,true", in, got, ok, want)
		}
	}
	if _, ok := MetricsGranularityTrunc("hour"); ok {
		t.Error("invalid granularity accepted")
	}
}
