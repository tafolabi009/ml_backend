package middleware

import "testing"

func TestImpersonationWriteBlocked(t *testing.T) {
	cases := []struct {
		method, path string
		blocked      bool
	}{
		{"GET", "/api/v1/validations", false},
		{"HEAD", "/api/v1/datasets", false},
		{"OPTIONS", "/api/v1/anything", false},
		{"POST", "/api/v1/validations/create", true},
		{"DELETE", "/api/v1/datasets/ds_1", true},
		{"PATCH", "/api/v1/validations/val_1", true},
		{"POST", "/api/v1/credits/purchase", true},
		{"POST", "/api/v1/auth/logout", false}, // allowed so admin can end session
	}
	for _, c := range cases {
		if got := ImpersonationWriteBlocked(c.method, c.path); got != c.blocked {
			t.Errorf("ImpersonationWriteBlocked(%s %s) = %v, want %v", c.method, c.path, got, c.blocked)
		}
	}
}
