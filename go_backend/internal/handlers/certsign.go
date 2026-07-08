package handlers

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Cryptographically signed certificates: a third party (auditor, buyer of a
// dataset) can verify that a Synthos certificate is genuine and unaltered
// using the published Ed25519 public key — offline, without trusting the
// bearer. Canonical form is compact JSON with sorted keys (Go json.Marshal of
// a map; reproducible in Python via json.dumps(obj, sort_keys=True,
// separators=(',',':'))). Certificate payloads contain only strings, ints and
// booleans so canonicalization is deterministic across languages.

var (
	signingKeyMu   sync.Mutex
	signingPriv    ed25519.PrivateKey
	signingPub     ed25519.PublicKey
	signingKeyID   string
	signingLoadErr error
	signingLoaded  bool
)

// loadSigningKey resolves the signing keypair: env seed → DB row → generate
// and persist (race-safe across instances via ON CONFLICT DO NOTHING).
func loadSigningKey(ctx context.Context) (ed25519.PrivateKey, ed25519.PublicKey, string, error) {
	signingKeyMu.Lock()
	defer signingKeyMu.Unlock()
	if signingLoaded {
		return signingPriv, signingPub, signingKeyID, signingLoadErr
	}

	finish := func(seed []byte) {
		priv := ed25519.NewKeyFromSeed(seed)
		pub := priv.Public().(ed25519.PublicKey)
		sum := sha256.Sum256(pub)
		signingPriv, signingPub, signingKeyID = priv, pub, hex.EncodeToString(sum[:4])
		signingLoaded = true
	}

	// 1) Explicit env seed wins (base64, 32 bytes).
	if envSeed := os.Getenv("CERT_SIGNING_SEED"); envSeed != "" {
		seed, err := base64.StdEncoding.DecodeString(envSeed)
		if err != nil || len(seed) != ed25519.SeedSize {
			signingLoadErr = fmt.Errorf("CERT_SIGNING_SEED must be base64 of exactly %d bytes", ed25519.SeedSize)
			signingLoaded = true
			return nil, nil, "", signingLoadErr
		}
		finish(seed)
		return signingPriv, signingPub, signingKeyID, nil
	}

	db := database.GetDB()

	// 2) Existing DB key.
	var privB64 string
	err := db.QueryRow(ctx, `SELECT private_key FROM signing_keys WHERE id = 'default'`).Scan(&privB64)
	if err != nil {
		// 3) Generate and persist; if another instance wins the race, use theirs.
		seed := make([]byte, ed25519.SeedSize)
		if _, rerr := rand.Read(seed); rerr != nil {
			signingLoadErr = fmt.Errorf("failed to generate signing seed: %w", rerr)
			signingLoaded = true
			return nil, nil, "", signingLoadErr
		}
		priv := ed25519.NewKeyFromSeed(seed)
		pub := priv.Public().(ed25519.PublicKey)
		_, _ = db.Exec(ctx,
			`INSERT INTO signing_keys (id, algorithm, public_key, private_key)
			 VALUES ('default', 'Ed25519', $1, $2)
			 ON CONFLICT (id) DO NOTHING`,
			base64.StdEncoding.EncodeToString(pub), base64.StdEncoding.EncodeToString(seed))
		if serr := db.QueryRow(ctx, `SELECT private_key FROM signing_keys WHERE id = 'default'`).Scan(&privB64); serr != nil {
			signingLoadErr = fmt.Errorf("failed to load signing key: %w", serr)
			signingLoaded = true
			return nil, nil, "", signingLoadErr
		}
	}

	seed, derr := base64.StdEncoding.DecodeString(privB64)
	if derr != nil || len(seed) != ed25519.SeedSize {
		signingLoadErr = fmt.Errorf("stored signing key is invalid")
		signingLoaded = true
		return nil, nil, "", signingLoadErr
	}
	finish(seed)
	return signingPriv, signingPub, signingKeyID, nil
}

// canonicalJSON renders compact JSON with sorted keys and no HTML escaping —
// byte-identical to Python's json.dumps(obj, sort_keys=True, separators=(',',':')).
func canonicalJSON(v interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// GetSignedCertificateFiber returns a signed, machine-verifiable certificate
// for a completed validation. GET /validations/:id/certificate.json
func GetSignedCertificateFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	validationRepo := repository.NewValidationRepository(db)
	validation, err := validationRepo.GetByID(ctx, validationID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Validation not found"},
		})
	}
	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this validation"},
		})
	}
	if validation.Status != "completed" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_COMPLETED", "message": "Certificates are only issued for completed validations"},
		})
	}

	priv, pub, keyID, err := loadSigningKey(ctx)
	if err != nil {
		log.Printf("signing key unavailable: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "SIGNING_UNAVAILABLE", "message": "Certificate signing is not available"},
		})
	}

	datasetName := ""
	datasetRepo := repository.NewDatasetRepository(db)
	if ds, derr := datasetRepo.GetByID(ctx, validation.DatasetID); derr == nil {
		datasetName = ds.Filename
	}

	rs := 0
	if validation.RiskScore != nil {
		rs = *validation.RiskScore
	}
	rl := "unknown"
	if validation.RiskLevel != nil {
		rl = *validation.RiskLevel
	}
	we := false
	if validation.WarrantyEligible != nil {
		we = *validation.WarrantyEligible
	}
	completedAt := ""
	if validation.CompletedAt != nil {
		completedAt = validation.CompletedAt.UTC().Format(time.RFC3339)
	}

	// Only strings/ints/bools: keeps canonical JSON deterministic everywhere.
	displayName := validationDisplayName(ctx, validationID)

	cert := map[string]interface{}{
		"version":           "1.0",
		"certificate_id":    "cert_" + validationID,
		"issuer":            "Synthos",
		"issued_at":         time.Now().UTC().Format(time.RFC3339),
		"validation_id":     validationID,
		"dataset_id":        validation.DatasetID,
		"dataset_name":      datasetName,
		"validation_name":   displayName,
		"completed_at":      completedAt,
		"risk_score":        rs,
		"risk_level":        rl,
		"warranty_eligible": we,
	}

	payload, err := canonicalJSON(cert)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "SIGNING_ERROR", "message": "Failed to canonicalize certificate"},
		})
	}
	sig := ed25519.Sign(priv, payload)
	sum := sha256.Sum256(payload)

	return c.JSON(fiber.Map{
		"certificate":    cert,
		"signature":      base64.StdEncoding.EncodeToString(sig),
		"algorithm":      "Ed25519",
		"key_id":         keyID,
		"public_key":     base64.StdEncoding.EncodeToString(pub),
		"payload_sha256": hex.EncodeToString(sum[:]),
		"verify": fiber.Map{
			"endpoint": "/api/v1/certificates/verify",
			"offline":  "canonicalize the certificate object as compact JSON with sorted keys (Python: json.dumps(cert, sort_keys=True, separators=(',',':'))) and verify the Ed25519 signature against the public key",
		},
	})
}

// GetCertificatePublicKeyFiber publishes the verification key. PUBLIC.
// GET /certificates/public-key
func GetCertificatePublicKeyFiber(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, pub, keyID, err := loadSigningKey(ctx)
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": fiber.Map{"code": "SIGNING_UNAVAILABLE", "message": "Certificate signing is not configured"},
		})
	}
	return c.JSON(fiber.Map{
		"algorithm":  "Ed25519",
		"key_id":     keyID,
		"public_key": base64.StdEncoding.EncodeToString(pub),
	})
}

// VerifyCertificateFiber verifies a certificate + signature pair. PUBLIC.
// POST /certificates/verify {"certificate": {...}, "signature": "base64"}
func VerifyCertificateFiber(c *fiber.Ctx) error {
	var req struct {
		Certificate map[string]interface{} `json:"certificate"`
		Signature   string                 `json:"signature"`
	}
	if err := c.BodyParser(&req); err != nil || req.Certificate == nil || req.Signature == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "certificate object and signature are required"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, pub, keyID, err := loadSigningKey(ctx)
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": fiber.Map{"code": "SIGNING_UNAVAILABLE", "message": "Certificate signing is not configured"},
		})
	}

	sig, err := base64.StdEncoding.DecodeString(req.Signature)
	if err != nil {
		return c.JSON(fiber.Map{"valid": false, "reason": "signature is not valid base64", "key_id": keyID})
	}
	payload, err := canonicalJSON(req.Certificate)
	if err != nil {
		return c.JSON(fiber.Map{"valid": false, "reason": "certificate cannot be canonicalized", "key_id": keyID})
	}

	valid := ed25519.Verify(pub, payload, sig)
	resp := fiber.Map{"valid": valid, "key_id": keyID}
	if !valid {
		resp["reason"] = "signature does not match this certificate under the current Synthos signing key"
	}
	return c.JSON(resp)
}
