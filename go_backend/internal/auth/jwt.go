package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/pquerna/otp"
	"github.com/pquerna/otp/totp"
	"golang.org/x/crypto/bcrypt"
)

var jwtSecret []byte
var totpIssuer = "SynthOS"

// InitJWT initializes the JWT secret
func InitJWT(secret string) {
	jwtSecret = []byte(secret)
}

// SetTOTPIssuer sets the issuer name for TOTP (shown in authenticator apps)
func SetTOTPIssuer(issuer string) {
	totpIssuer = issuer
}

// Token types distinguish short-lived access tokens from long-lived refresh
// tokens so that a refresh token cannot be replayed as a bearer access token
// (and vice-versa). Tokens issued before this field existed have an empty
// TokenType and are accepted on both paths for backward compatibility.
const (
	TokenTypeAccess  = "access"
	TokenTypeRefresh = "refresh"
)

// Claims represents JWT claims with extended user information
type Claims struct {
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	Username  string `json:"username,omitempty"`
	CompanyID string `json:"company_id"`
	Role      string `json:"role,omitempty"`
	SessionID string `json:"session_id,omitempty"` // For session tracking
	TokenType string `json:"token_type,omitempty"` // "access" or "refresh"
	jwt.RegisteredClaims
}

// GenerateToken creates a new JWT token with extended claims
func GenerateToken(userID, email, companyID string, expiresIn time.Duration) (string, error) {
	return GenerateTokenWithClaims(userID, email, "", companyID, "", "", expiresIn)
}

// GenerateTokenWithClaims creates an access JWT with full claims
func GenerateTokenWithClaims(userID, email, username, companyID, role, sessionID string, expiresIn time.Duration) (string, error) {
	return generateToken(userID, email, username, companyID, role, sessionID, TokenTypeAccess, expiresIn)
}

// GenerateRefreshToken creates a refresh JWT. Refresh tokens are rejected by the
// access-token middleware, so a leaked refresh token cannot be used as a bearer.
func GenerateRefreshToken(userID, email, username, companyID, role, sessionID string, expiresIn time.Duration) (string, error) {
	return generateToken(userID, email, username, companyID, role, sessionID, TokenTypeRefresh, expiresIn)
}

func generateToken(userID, email, username, companyID, role, sessionID, tokenType string, expiresIn time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		UserID:    userID,
		Email:     email,
		Username:  username,
		CompanyID: companyID,
		Role:      role,
		SessionID: sessionID,
		TokenType: tokenType,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(expiresIn)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    totpIssuer,
			Subject:   userID,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}

// TokenPair represents access and refresh tokens
type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

// GenerateTokenPair creates both access and refresh tokens
func GenerateTokenPair(userID, email string) (*TokenPair, error) {
	accessToken, err := GenerateToken(userID, email, "", 15*time.Minute)
	if err != nil {
		return nil, err
	}

	refreshToken, err := GenerateRefreshToken(userID, email, "", "", "", "", 7*24*time.Hour)
	if err != nil {
		return nil, err
	}

	return &TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// ValidateToken validates a JWT token and returns claims
func ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}

	return nil, errors.New("invalid token")
}

// HashPassword hashes a password using bcrypt with a secure cost factor
func HashPassword(password string) (string, error) {
	// Use cost of 12 for production (balance between security and performance)
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	return string(bytes), err
}

// CheckPasswordHash compares password with hash
func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// GenerateSecureToken generates a cryptographically secure random token
func GenerateSecureToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// HashToken creates a SHA256 hash of a token for storage
func HashToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

// GenerateAPIKey generates a new API key with prefix
func GenerateAPIKey() (fullKey string, prefix string, hash string, err error) {
	// Generate 32 bytes of random data
	bytes := make([]byte, 32)
	if _, err = rand.Read(bytes); err != nil {
		return "", "", "", err
	}

	// Create key with prefix for identification
	fullKey = "sk_" + hex.EncodeToString(bytes)
	prefix = fullKey[:12] // "sk_" + first 8 hex chars
	hash = HashToken(fullKey)

	return fullKey, prefix, hash, nil
}

// ValidateAPIKeyHash validates an API key against its hash
func ValidateAPIKeyHash(key, hash string) bool {
	return HashToken(key) == hash
}

// GenerateTOTPSecret generates a new TOTP secret for 2FA setup
func GenerateTOTPSecret(email string) (secret string, url string, err error) {
	key, err := totp.Generate(totp.GenerateOpts{
		Issuer:      totpIssuer,
		AccountName: email,
		Period:      30,
		SecretSize:  32,
		Digits:      otp.DigitsSix,
		Algorithm:   otp.AlgorithmSHA1,
	})
	if err != nil {
		return "", "", err
	}

	return key.Secret(), key.URL(), nil
}

// ValidateTOTPCode validates a TOTP code against a secret
func ValidateTOTPCode(code, secret string) bool {
	return totp.Validate(code, secret)
}

// GenerateBackupCodes generates recovery codes for 2FA
func GenerateBackupCodes(count int) ([]string, error) {
	codes := make([]string, count)
	for i := 0; i < count; i++ {
		bytes := make([]byte, 5)
		if _, err := rand.Read(bytes); err != nil {
			return nil, err
		}
		// Format: XXXXX-XXXXX (base32, no padding, lowercase)
		encoded := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(bytes))
		codes[i] = fmt.Sprintf("%s-%s", encoded[:5], encoded[5:])
	}
	return codes, nil
}

// HashBackupCodes hashes backup codes for storage
func HashBackupCodes(codes []string) []string {
	hashed := make([]string, len(codes))
	for i, code := range codes {
		hashed[i] = HashToken(code)
	}
	return hashed
}

// ValidateBackupCode validates a backup code against stored hashes
func ValidateBackupCode(code string, hashedCodes []string) (int, bool) {
	codeHash := HashToken(code)
	for i, hash := range hashedCodes {
		if hash == codeHash {
			return i, true
		}
	}
	return -1, false
}

// PasswordMeetsRequirements checks if password meets security requirements
func PasswordMeetsRequirements(password string) error {
	if len(password) < 8 {
		return errors.New("password must be at least 8 characters")
	}

	var (
		hasUpper   bool
		hasLower   bool
		hasNumber  bool
		hasSpecial bool
	)

	for _, char := range password {
		switch {
		case 'A' <= char && char <= 'Z':
			hasUpper = true
		case 'a' <= char && char <= 'z':
			hasLower = true
		case '0' <= char && char <= '9':
			hasNumber = true
		case strings.ContainsRune("!@#$%^&*()_+-=[]{}|;':\",./<>?", char):
			hasSpecial = true
		}
	}

	if !hasUpper {
		return errors.New("password must contain at least one uppercase letter")
	}
	if !hasLower {
		return errors.New("password must contain at least one lowercase letter")
	}
	if !hasNumber {
		return errors.New("password must contain at least one number")
	}
	if !hasSpecial {
		return errors.New("password must contain at least one special character")
	}

	return nil
}
