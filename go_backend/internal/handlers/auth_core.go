package handlers

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/email"
	"github.com/tafolabi009/backend/go_backend/pkg/sanitize"
)

const (
	maxFailedAttempts = 5
	lockoutDuration   = 15 * time.Minute
	accessTokenTTL    = 15 * time.Minute
	refreshTokenTTL   = 30 * 24 * time.Hour
	resetTokenTTL     = 1 * time.Hour
)

// Helper to safely dereference string pointer
func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Helper to create string pointer
func strPtr(s string) *string {
	return &s
}

// RegisterFiber handles user registration with full validation
// POST /api/v1/auth/register
func RegisterFiber(c *fiber.Ctx) error {
	var req models.RegisterRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": "Invalid request body",
			},
		})
	}

	// Validate required fields
	if req.Email == "" || req.Password == "" || req.FullName == "" || req.CompanyName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "VALIDATION_ERROR",
				"message": "Email, password, full_name, and company_name are required",
			},
		})
	}

	// Validate email format
	if !strings.Contains(req.Email, "@") || !strings.Contains(req.Email, ".") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "VALIDATION_ERROR",
				"message": "Invalid email format",
			},
		})
	}

	// Sanitize user inputs
	req.FullName = sanitize.String(req.FullName)
	req.CompanyName = sanitize.String(req.CompanyName)

	// Validate password strength
	if err := auth.PasswordMeetsRequirements(req.Password); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "WEAK_PASSWORD",
				"message": err.Error(),
			},
		})
	}

	// Generate username from email if not provided
	username := req.Username
	if username == "" {
		username = strings.Split(req.Email, "@")[0]
	}

	// Hash password with bcrypt (cost 12)
	passwordHash, err := auth.HashPassword(req.Password)
	if err != nil {
		log.Printf("Failed to hash password: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to process password",
			},
		})
	}

	// Check for invite token
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()
	role := "user"
	autoVerified := false

	if req.InviteToken != "" {
		var inviteID, inviteEmail, inviteRole, inviteStatus string
		var inviteExpiresAt time.Time
		err := db.QueryRow(ctx,
			`SELECT id, email, role, status, expires_at FROM invites WHERE token = $1`,
			req.InviteToken,
		).Scan(&inviteID, &inviteEmail, &inviteRole, &inviteStatus, &inviteExpiresAt)
		if err == nil && inviteStatus == "pending" && time.Now().Before(inviteExpiresAt) {
			role = inviteRole
			autoVerified = true
			// Mark invite as accepted
			_, _ = db.Exec(ctx, `UPDATE invites SET status = 'accepted' WHERE id = $1`, inviteID)
		}
	}

	// Create user
	now := time.Now().UTC()
	user := models.User{
		ID:               "usr_" + uuid.New().String()[:8],
		Email:            strings.ToLower(strings.TrimSpace(req.Email)),
		Username:         strPtr(strings.ToLower(strings.TrimSpace(username))),
		PasswordHash:     passwordHash,
		FullName:         strPtr(strings.TrimSpace(req.FullName)),
		CompanyID:        strPtr("cmp_" + uuid.New().String()[:8]),
		CompanyName:      strPtr(strings.TrimSpace(req.CompanyName)),
		Role:             strPtr(role),
		TwoFactorEnabled: false,
		EmailVerified:    autoVerified,
		IsActive:         true,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	// Save user to database
	userRepo := repository.NewUserRepository(database.GetDB())
	if err := userRepo.Create(ctx, &user); err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "EMAIL_EXISTS",
					"message": "An account with this email already exists",
				},
			})
		}

		log.Printf("Failed to create user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to create user account",
			},
		})
	}

	// Log security event
	logSecurityEvent(ctx, user.ID, "user_registered", true, c.IP(), c.Get("User-Agent"), nil)

	// If not auto-verified via invite, generate OTP for email verification
	requiresVerification := false
	if !autoVerified {
		otp, err := generateOTP()
		if err == nil {
			otpHash, err := bcrypt.GenerateFromPassword([]byte(otp), bcrypt.DefaultCost)
			if err == nil {
				verID := "ver_" + uuid.New().String()[:8]
				_, dbErr := db.Exec(ctx,
					`INSERT INTO email_verifications (id, user_id, email, otp_hash, attempts, expires_at, created_at)
					 VALUES ($1, $2, $3, $4, 0, $5, NOW())`,
					verID, user.ID, user.Email, string(otpHash), time.Now().Add(10*time.Minute),
				)
				if dbErr == nil {
					requiresVerification = true
					// Send verification email (best-effort)
					go func() {
						emailClient := email.GetClient()
						if emailClient.IsConfigured() {
							name := strings.TrimSpace(req.FullName)
							subject, html := email.VerificationOTPEmail(name, otp)
							if err := emailClient.Send(user.Email, subject, html); err != nil {
								log.Printf("Failed to send verification email to %s: %v", user.Email, err)
							}
						} else {
							log.Printf("Email client not configured, skipping verification email for %s", user.Email)
						}
					}()
				}
			}
		}
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"user_id":               user.ID,
		"email":                 user.Email,
		"username":              user.Username,
		"company_id":            user.CompanyID,
		"created_at":            user.CreatedAt.Format(time.RFC3339),
		"requires_verification": requiresVerification,
	})
}

// LoginFiber handles user authentication with 2FA support
// POST /api/v1/auth/login
func LoginFiber(c *fiber.Ctx) error {
	var req models.LoginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": "Invalid request body",
			},
		})
	}

	if req.Email == "" || req.Password == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "VALIDATION_ERROR",
				"message": "Email and password are required",
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())
	user, err := userRepo.GetByEmail(ctx, strings.ToLower(strings.TrimSpace(req.Email)))
	if err != nil {
		log.Printf("Login failed for %s: %v", req.Email, err) // Debug logging
		logSecurityEvent(ctx, "", "login_failed", false, c.IP(), c.Get("User-Agent"), map[string]string{"email": req.Email, "reason": "user_not_found"})
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_CREDENTIALS",
				"message": "Invalid email or password",
			},
		})
	}

	// Check if account is locked
	if user.LockedUntil != nil && time.Now().Before(*user.LockedUntil) {
		logSecurityEvent(ctx, user.ID, "login_failed", false, c.IP(), c.Get("User-Agent"), map[string]string{"reason": "account_locked"})
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "ACCOUNT_LOCKED",
				"message": "Account is temporarily locked due to too many failed login attempts",
			},
		})
	}

	// Check if account is active
	if !user.IsActive {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "ACCOUNT_DISABLED",
				"message": "This account has been disabled",
			},
		})
	}

	// Check email verification status before proceeding
	if !user.EmailVerified {
		// Verify password first so we don't leak verification status to wrong credentials
		if auth.CheckPasswordHash(req.Password, user.PasswordHash) {
			// Auto-resend OTP so the user can verify from the redirect page
			go func() {
				otpCtx, otpCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer otpCancel()
				db := database.GetDB()
				var recentCount int
				db.QueryRow(otpCtx, `SELECT COUNT(*) FROM email_verifications WHERE user_id = $1 AND created_at > NOW() - INTERVAL '1 hour'`, user.ID).Scan(&recentCount)
				if recentCount < 3 {
					db.Exec(otpCtx, `DELETE FROM email_verifications WHERE user_id = $1`, user.ID)
					otp, _ := generateOTP()
					otpHash, _ := bcrypt.GenerateFromPassword([]byte(otp), bcrypt.DefaultCost)
					verID := "ver_" + uuid.New().String()[:8]
					db.Exec(otpCtx, `INSERT INTO email_verifications (id, user_id, email, otp_hash, expires_at) VALUES ($1, $2, $3, $4, NOW() + INTERVAL '10 minutes')`, verID, user.ID, user.Email, string(otpHash))
					emailClient := email.GetClient()
					if emailClient.IsConfigured() {
						userName := ""
						if user.FullName != nil {
							userName = *user.FullName
						}
						subject, body := email.VerificationOTPEmail(userName, otp)
						if err := emailClient.Send(user.Email, subject, body); err != nil {
							log.Printf("Failed to resend OTP to %s: %v", user.Email, err)
						}
					}
				}
			}()
			return c.Status(fiber.StatusOK).JSON(fiber.Map{
				"requires_verification": true,
				"email":                 user.Email,
			})
		}
	}

	// Verify password
	if !auth.CheckPasswordHash(req.Password, user.PasswordHash) {
		// Increment failed attempts
		user.FailedLoginAttempts++
		if user.FailedLoginAttempts >= maxFailedAttempts {
			lockUntil := time.Now().Add(lockoutDuration)
			user.LockedUntil = &lockUntil
		}
		_ = userRepo.UpdateLoginAttempts(ctx, user.ID, user.FailedLoginAttempts, user.LockedUntil)

		logSecurityEvent(ctx, user.ID, "login_failed", false, c.IP(), c.Get("User-Agent"), map[string]string{"reason": "invalid_password", "attempts": strconv.Itoa(user.FailedLoginAttempts)})
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_CREDENTIALS",
				"message": "Invalid email or password",
			},
		})
	}

	// Check 2FA if enabled
	if user.TwoFactorEnabled {
		if req.TOTPCode == "" {
			// Return indication that 2FA is required
			return c.Status(fiber.StatusOK).JSON(fiber.Map{
				"requires_two_factor": true,
				"message":             "Two-factor authentication code required",
			})
		}

		// Validate TOTP code
		if !auth.ValidateTOTPCode(req.TOTPCode, strVal(user.TwoFactorSecret)) {
			// Check backup codes
			if idx, valid := auth.ValidateBackupCode(req.TOTPCode, user.TwoFactorBackupCodes); valid {
				// Remove used backup code
				user.TwoFactorBackupCodes = append(user.TwoFactorBackupCodes[:idx], user.TwoFactorBackupCodes[idx+1:]...)
				_ = userRepo.UpdateBackupCodes(ctx, user.ID, user.TwoFactorBackupCodes)
			} else {
				logSecurityEvent(ctx, user.ID, "2fa_failed", false, c.IP(), c.Get("User-Agent"), nil)
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
					"error": fiber.Map{
						"code":    "INVALID_2FA_CODE",
						"message": "Invalid two-factor authentication code",
					},
				})
			}
		}
	}

	// Reset failed attempts on successful login
	if user.FailedLoginAttempts > 0 {
		_ = userRepo.UpdateLoginAttempts(ctx, user.ID, 0, nil)
	}

	// Generate session ID for tracking
	sessionID := "ses_" + uuid.New().String()[:8]

	// Generate tokens with full claims
	accessToken, err := auth.GenerateTokenWithClaims(
		user.ID, user.Email, strVal(user.Username), strVal(user.CompanyID), strVal(user.Role), sessionID, accessTokenTTL,
	)
	if err != nil {
		log.Printf("Failed to generate access token: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate access token",
			},
		})
	}

	refreshToken, err := auth.GenerateRefreshToken(
		user.ID, user.Email, strVal(user.Username), strVal(user.CompanyID), strVal(user.Role), sessionID, refreshTokenTTL,
	)
	if err != nil {
		log.Printf("Failed to generate refresh token: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate refresh token",
			},
		})
	}

	// Create session record
	session := models.Session{
		ID:               sessionID,
		UserID:           user.ID,
		RefreshTokenHash: auth.HashToken(refreshToken),
		UserAgent:        c.Get("User-Agent"),
		IPAddress:        c.IP(),
		IsValid:          true,
		CreatedAt:        time.Now().UTC(),
		ExpiresAt:        time.Now().Add(refreshTokenTTL),
		LastUsedAt:       time.Now().UTC(),
	}
	_ = userRepo.CreateSession(ctx, &session)

	// Update last login
	now := time.Now().UTC()
	user.LastLoginAt = &now
	_ = userRepo.UpdateLastLogin(ctx, user.ID, now)

	logSecurityEvent(ctx, user.ID, "login_success", true, c.IP(), c.Get("User-Agent"), nil)

	return c.JSON(models.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    "Bearer",
		ExpiresIn:    int(accessTokenTTL.Seconds()),
		User:         user.ToProfile(),
	})
}

// LogoutFiber handles user logout and session invalidation
// POST /api/v1/auth/logout
func LogoutFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id")
	if userID == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "UNAUTHORIZED",
				"message": "Authentication required",
			},
		})
	}

	// Get the current token to blacklist it
	authHeader := c.Get("Authorization")
	if authHeader != "" {
		parts := strings.Split(authHeader, " ")
		if len(parts) == 2 {
			token := parts[1]
			claims, _ := auth.ValidateToken(token)
			if claims != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				userRepo := repository.NewUserRepository(database.GetDB())

				// Invalidate session if session ID is in token
				if claims.SessionID != "" {
					_ = userRepo.InvalidateSession(ctx, claims.SessionID)
				}

				// Blacklist the token
				_ = userRepo.BlacklistToken(ctx, auth.HashToken(token), userID.(string), claims.ExpiresAt.Time)

				logSecurityEvent(ctx, userID.(string), "logout", true, c.IP(), c.Get("User-Agent"), nil)
			}
		}
	}

	return c.JSON(fiber.Map{
		"message": "Successfully logged out",
	})
}

// RefreshTokenFiber handles token refresh with rotation
// POST /api/v1/auth/refresh
func RefreshTokenFiber(c *fiber.Ctx) error {
	var req models.RefreshTokenRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": "Invalid request body",
			},
		})
	}

	if req.RefreshToken == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "VALIDATION_ERROR",
				"message": "Refresh token is required",
			},
		})
	}

	// Validate refresh token
	claims, err := auth.ValidateToken(req.RefreshToken)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_TOKEN",
				"message": "Invalid or expired refresh token",
			},
		})
	}

	// Only refresh tokens may be exchanged here; reject access tokens.
	// (Empty type = legacy token issued before typing, still accepted.)
	if claims.TokenType == auth.TokenTypeAccess {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_TOKEN",
				"message": "An access token cannot be used to refresh",
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())

	// Check if token is blacklisted. Fail closed on a lookup error.
	blacklisted, blErr := userRepo.IsTokenBlacklisted(ctx, auth.HashToken(req.RefreshToken))
	if blErr != nil || blacklisted {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "TOKEN_REVOKED",
				"message": "Token has been revoked",
			},
		})
	}

	// Verify session is still valid
	if claims.SessionID != "" {
		session, err := userRepo.GetSession(ctx, claims.SessionID)
		if err != nil || !session.IsValid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "SESSION_INVALID",
					"message": "Session has been invalidated",
				},
			})
		}
	}

	// Get user to ensure account is still active
	user, err := userRepo.GetByID(ctx, claims.UserID)
	if err != nil || !user.IsActive {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "ACCOUNT_INVALID",
				"message": "Account not found or disabled",
			},
		})
	}

	// Generate new session ID for token rotation
	newSessionID := "ses_" + uuid.New().String()[:8]

	// Generate new access token
	accessToken, err := auth.GenerateTokenWithClaims(
		claims.UserID, claims.Email, strVal(user.Username), claims.CompanyID, strVal(user.Role), newSessionID, accessTokenTTL,
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate access token",
			},
		})
	}

	// Token rotation: generate new refresh token and blacklist the old one
	newRefreshToken, err := auth.GenerateRefreshToken(
		claims.UserID, claims.Email, strVal(user.Username), claims.CompanyID, strVal(user.Role), newSessionID, refreshTokenTTL,
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate refresh token",
			},
		})
	}

	// Blacklist old refresh token
	_ = userRepo.BlacklistToken(ctx, auth.HashToken(req.RefreshToken), claims.UserID, claims.ExpiresAt.Time)

	// Create new session
	session := models.Session{
		ID:               newSessionID,
		UserID:           claims.UserID,
		RefreshTokenHash: auth.HashToken(newRefreshToken),
		UserAgent:        c.Get("User-Agent"),
		IPAddress:        c.IP(),
		IsValid:          true,
		CreatedAt:        time.Now().UTC(),
		ExpiresAt:        time.Now().Add(refreshTokenTTL),
		LastUsedAt:       time.Now().UTC(),
	}
	_ = userRepo.CreateSession(ctx, &session)

	// Invalidate old session
	if claims.SessionID != "" {
		_ = userRepo.InvalidateSession(ctx, claims.SessionID)
	}

	return c.JSON(models.RefreshTokenResponse{
		AccessToken:  accessToken,
		RefreshToken: newRefreshToken,
		ExpiresIn:    int(accessTokenTTL.Seconds()),
	})
}

// GetMeFiber returns the current authenticated user's complete profile
// GET /api/v1/auth/me
func GetMeFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id")
	if userID == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "UNAUTHORIZED",
				"message": "Authentication required",
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())
	user, err := userRepo.GetByID(ctx, userID.(string))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "User not found",
			},
		})
	}

	// Return safe user profile (no sensitive fields)
	return c.JSON(user.ToProfile())
}

// UpdateProfileFiber updates the current user's profile
// PUT /api/v1/auth/me
func UpdateProfileFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id")
	if userID == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "UNAUTHORIZED",
				"message": "Authentication required",
			},
		})
	}

	var req models.UpdateProfileRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": "Invalid request body",
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())

	// Get existing user
	user, err := userRepo.GetByID(ctx, userID.(string))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "User not found",
			},
		})
	}

	// Sanitize user inputs
	req.FullName = sanitize.String(req.FullName)
	req.CompanyName = sanitize.String(req.CompanyName)
	req.JobTitle = sanitize.String(req.JobTitle)

	// Update fields if provided
	if req.FullName != "" {
		user.FullName = &req.FullName
	}
	if req.Username != "" {
		user.Username = &req.Username
	}
	if req.CompanyName != "" {
		user.CompanyName = &req.CompanyName
	}
	if req.JobTitle != "" {
		user.JobTitle = &req.JobTitle
	}
	if req.Phone != "" {
		user.Phone = &req.Phone
	}

	// Save updates
	if err := userRepo.Update(ctx, user); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "UPDATE_FAILED",
				"message": "Failed to update profile",
			},
		})
	}

	// Return updated profile directly (consistent with GetMeFiber)
	return c.JSON(user.ToProfile())
}

// ChangePasswordFiber handles password change for authenticated users
// POST /api/v1/auth/change-password
func ChangePasswordFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id")
	if userID == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "UNAUTHORIZED",
				"message": "Authentication required",
			},
		})
	}

	var req models.ChangePasswordRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": "Invalid request body",
			},
		})
	}

	if req.CurrentPassword == "" || req.NewPassword == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "VALIDATION_ERROR",
				"message": "Current password and new password are required",
			},
		})
	}

	// Validate new password strength
	if err := auth.PasswordMeetsRequirements(req.NewPassword); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "WEAK_PASSWORD",
				"message": err.Error(),
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())
	user, err := userRepo.GetByID(ctx, userID.(string))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "User not found",
			},
		})
	}

	// Verify current password
	if !auth.CheckPasswordHash(req.CurrentPassword, user.PasswordHash) {
		logSecurityEvent(ctx, userID.(string), "password_change_failed", false, c.IP(), c.Get("User-Agent"), map[string]string{"reason": "invalid_current_password"})
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_PASSWORD",
				"message": "Current password is incorrect",
			},
		})
	}

	// Hash new password
	newHash, err := auth.HashPassword(req.NewPassword)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to process new password",
			},
		})
	}

	// Update password
	now := time.Now().UTC()
	if err := userRepo.UpdatePassword(ctx, userID.(string), newHash, now); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to update password",
			},
		})
	}

	// Invalidate all existing sessions except current
	_ = userRepo.InvalidateAllUserSessions(ctx, userID.(string))

	logSecurityEvent(ctx, userID.(string), "password_changed", true, c.IP(), c.Get("User-Agent"), nil)

	// Create notification
	_ = userRepo.CreateNotification(ctx, &models.Notification{
		ID:        "ntf_" + uuid.New().String()[:8],
		UserID:    userID.(string),
		Type:      "security",
		Title:     "Password Changed",
		Message:   "Your password was successfully changed. If you didn't make this change, please contact support immediately.",
		CreatedAt: now,
	})

	return c.JSON(fiber.Map{
		"message": "Password changed successfully. Please log in again with your new password.",
	})
}

// ForgotPasswordFiber initiates password reset flow
// POST /api/v1/auth/forgot-password
func ForgotPasswordFiber(c *fiber.Ctx) error {
	var req models.ForgotPasswordRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": "Invalid request body",
			},
		})
	}

	if req.Email == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "VALIDATION_ERROR",
				"message": "Email is required",
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())

	// Always return success to prevent email enumeration
	// But only actually create token if user exists
	user, err := userRepo.GetByEmail(ctx, strings.ToLower(strings.TrimSpace(req.Email)))
	if err == nil && user.IsActive {
		// Generate reset token
		token, err := auth.GenerateSecureToken(32)
		if err == nil {
			resetToken := models.PasswordResetToken{
				ID:        "rst_" + uuid.New().String()[:8],
				UserID:    user.ID,
				TokenHash: auth.HashToken(token),
				ExpiresAt: time.Now().Add(resetTokenTTL),
				CreatedAt: time.Now().UTC(),
			}

			if err := userRepo.CreatePasswordResetToken(ctx, &resetToken); err == nil {
				// Send password reset email (best-effort)
				resetLink := fmt.Sprintf("https://synthos.dev/reset-password?token=%s", token)
				go func() {
					emailClient := email.GetClient()
					if emailClient.IsConfigured() {
						name := ""
						if user.FullName != nil {
							name = *user.FullName
						}
						subject, html := email.PasswordResetEmail(name, resetLink)
						if err := emailClient.Send(user.Email, subject, html); err != nil {
							log.Printf("Failed to send password reset email to %s: %v", user.Email, err)
						}
					} else {
						log.Printf("Email client not configured, skipping password reset email for %s", user.Email)
					}
				}()

				logSecurityEvent(ctx, user.ID, "password_reset_requested", true, c.IP(), c.Get("User-Agent"), nil)
			}
		}
	}

	return c.JSON(fiber.Map{
		"message": "If an account with that email exists, a password reset link has been sent.",
	})
}

// ResetPasswordFiber completes password reset with token
// POST /api/v1/auth/reset-password
func ResetPasswordFiber(c *fiber.Ctx) error {
	var req models.ResetPasswordRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": "Invalid request body",
			},
		})
	}

	if req.Token == "" || req.NewPassword == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "VALIDATION_ERROR",
				"message": "Token and new password are required",
			},
		})
	}

	// Validate password strength
	if err := auth.PasswordMeetsRequirements(req.NewPassword); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "WEAK_PASSWORD",
				"message": err.Error(),
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())

	// Find valid reset token
	tokenHash := auth.HashToken(req.Token)
	resetToken, err := userRepo.GetPasswordResetToken(ctx, tokenHash)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_TOKEN",
				"message": "Invalid or expired reset token",
			},
		})
	}

	// Check if token is expired
	if time.Now().After(resetToken.ExpiresAt) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "TOKEN_EXPIRED",
				"message": "Reset token has expired. Please request a new one.",
			},
		})
	}

	// Check if token was already used
	if resetToken.UsedAt != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "TOKEN_USED",
				"message": "Reset token has already been used",
			},
		})
	}

	// Hash new password
	newHash, err := auth.HashPassword(req.NewPassword)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to process new password",
			},
		})
	}

	// Update password
	now := time.Now().UTC()
	if err := userRepo.UpdatePassword(ctx, resetToken.UserID, newHash, now); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to update password",
			},
		})
	}

	// Mark token as used
	_ = userRepo.MarkResetTokenUsed(ctx, resetToken.ID, now)

	// Invalidate all sessions
	_ = userRepo.InvalidateAllUserSessions(ctx, resetToken.UserID)

	// Reset failed login attempts
	_ = userRepo.UpdateLoginAttempts(ctx, resetToken.UserID, 0, nil)

	logSecurityEvent(ctx, resetToken.UserID, "password_reset_completed", true, c.IP(), c.Get("User-Agent"), nil)

	return c.JSON(fiber.Map{
		"message": "Password has been reset successfully. Please log in with your new password.",
	})
}

// Helper function to log security events
func logSecurityEvent(ctx context.Context, userID, eventType string, success bool, ipAddress, userAgent string, details map[string]string) {
	userRepo := repository.NewUserRepository(database.GetDB())
	var detailsJSON *string
	if details != nil {
		if b, err := json.Marshal(details); err == nil {
			s := string(b)
			detailsJSON = &s
		}
	}
	_ = userRepo.LogSecurityEvent(ctx, &models.SecurityEvent{
		UserID:    userID,
		EventType: eventType,
		Success:   success,
		IPAddress: ipAddress,
		UserAgent: userAgent,
		Details:   detailsJSON,
		CreatedAt: time.Now().UTC(),
	})
}

// generateOTP generates a cryptographically secure 6-digit OTP
func generateOTP() (string, error) {
	max := big.NewInt(1000000)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return "", fmt.Errorf("failed to generate OTP: %w", err)
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}

// VerifyEmailFiber handles email verification with OTP
// POST /api/v1/auth/verify-email
func VerifyEmailFiber(c *fiber.Ctx) error {
	var req struct {
		Email string `json:"email"`
		OTP   string `json:"otp"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}

	if req.Email == "" || req.OTP == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "VALIDATION_ERROR", "message": "Email and OTP are required"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	// Find latest non-expired verification for this email
	var verID, userID, otpHash string
	var attempts int
	var expiresAt time.Time
	err := db.QueryRow(ctx,
		`SELECT ev.id, ev.user_id, ev.otp_hash, ev.attempts, ev.expires_at
		 FROM email_verifications ev
		 JOIN users u ON ev.user_id = u.id
		 WHERE ev.email = $1 AND ev.expires_at > NOW()
		 ORDER BY ev.created_at DESC LIMIT 1`,
		strings.ToLower(strings.TrimSpace(req.Email)),
	).Scan(&verID, &userID, &otpHash, &attempts, &expiresAt)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_OTP", "message": "No pending verification found. Please request a new OTP."},
		})
	}

	// Check max attempts
	if attempts >= 5 {
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"error": fiber.Map{"code": "MAX_ATTEMPTS", "message": "Too many failed attempts. Please request a new OTP."},
		})
	}

	// Increment attempts
	_, _ = db.Exec(ctx, `UPDATE email_verifications SET attempts = attempts + 1 WHERE id = $1`, verID)

	// Compare OTP hash with bcrypt
	if err := bcrypt.CompareHashAndPassword([]byte(otpHash), []byte(req.OTP)); err != nil {
		remaining := 4 - attempts // attempts was before increment
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":               "INVALID_OTP",
				"message":            "Invalid verification code",
				"remaining_attempts": remaining,
			},
		})
	}

	// OTP is valid - mark email as verified
	_, _ = db.Exec(ctx, `UPDATE users SET email_verified = true, updated_at = NOW() WHERE id = $1`, userID)

	// Delete all verifications for this user
	_, _ = db.Exec(ctx, `DELETE FROM email_verifications WHERE user_id = $1`, userID)

	logSecurityEvent(ctx, userID, "email_verified", true, c.IP(), c.Get("User-Agent"), nil)

	// Get full user data to generate JWT and return auto-login
	userRepo := repository.NewUserRepository(db)
	user, err := userRepo.GetByID(ctx, userID)
	if err != nil {
		return c.JSON(fiber.Map{"message": "Email verified successfully", "email_verified": true})
	}

	// Generate session and JWT for auto-login
	sessionID := "ses_" + uuid.New().String()[:8]
	accessToken, err := auth.GenerateTokenWithClaims(
		userID, user.Email, strVal(user.Username), strVal(user.CompanyID), strVal(user.Role), sessionID, accessTokenTTL,
	)
	if err != nil {
		return c.JSON(fiber.Map{"message": "Email verified successfully", "email_verified": true})
	}
	refreshToken, err := auth.GenerateRefreshToken(
		userID, user.Email, strVal(user.Username), strVal(user.CompanyID), strVal(user.Role), sessionID, refreshTokenTTL,
	)
	if err != nil {
		return c.JSON(fiber.Map{"message": "Email verified successfully", "email_verified": true})
	}

	// Create session record
	refreshHash := auth.HashToken(refreshToken)
	_, _ = db.Exec(ctx,
		`INSERT INTO sessions (id, user_id, refresh_token_hash, user_agent, ip_address, expires_at) VALUES ($1, $2, $3, $4, $5, NOW() + INTERVAL '30 days')`,
		sessionID, userID, refreshHash, c.Get("User-Agent"), c.IP())

	// Update last login
	_, _ = db.Exec(ctx, `UPDATE users SET last_login_at = NOW() WHERE id = $1`, userID)

	// Send welcome email (best-effort) to the authoritative, normalized address.
	go func() {
		emailClient := email.GetClient()
		if emailClient.IsConfigured() {
			var fullName, userEmail string
			database.GetDB().QueryRow(context.Background(),
				`SELECT COALESCE(full_name, ''), email FROM users WHERE id = $1`, userID,
			).Scan(&fullName, &userEmail)
			if userEmail == "" {
				userEmail = req.Email
			}
			subject, html := email.WelcomeEmail(fullName)
			if err := emailClient.Send(userEmail, subject, html); err != nil {
				log.Printf("Failed to send welcome email to %s: %v", userEmail, err)
			}
		}
	}()

	// Return JWT so frontend can auto-login
	profile := user.ToProfile()
	return c.JSON(fiber.Map{
		"message":        "Email verified successfully",
		"email_verified": true,
		"access_token":   accessToken,
		"refresh_token":  refreshToken,
		"token_type":     "Bearer",
		"expires_in":     900,
		"user":           profile,
	})
}

// ResendOTPFiber sends a new OTP verification code
// POST /api/v1/auth/resend-otp
func ResendOTPFiber(c *fiber.Ctx) error {
	var req struct {
		Email string `json:"email"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}

	if req.Email == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "VALIDATION_ERROR", "message": "Email is required"},
		})
	}

	// Always return success to prevent email enumeration
	successResponse := fiber.Map{
		"message": "If an account with that email exists and is unverified, a new verification code has been sent.",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()
	emailAddr := strings.ToLower(strings.TrimSpace(req.Email))

	// Look up user
	var userID, fullName string
	var emailVerified bool
	err := db.QueryRow(ctx,
		`SELECT id, COALESCE(full_name, ''), email_verified FROM users WHERE email = $1 AND is_active = true`,
		emailAddr,
	).Scan(&userID, &fullName, &emailVerified)
	if err != nil || emailVerified {
		// User not found or already verified - return success anyway
		return c.JSON(successResponse)
	}

	// Rate limit: max 3 OTPs per email per hour
	var recentCount int
	db.QueryRow(ctx,
		`SELECT COUNT(*) FROM email_verifications WHERE user_id = $1 AND created_at > NOW() - INTERVAL '1 hour'`,
		userID,
	).Scan(&recentCount)
	if recentCount >= 3 {
		return c.JSON(successResponse) // Don't reveal rate limit
	}

	// Generate new OTP
	otp, err := generateOTP()
	if err != nil {
		log.Printf("Failed to generate OTP: %v", err)
		return c.JSON(successResponse)
	}

	otpHash, err := bcrypt.GenerateFromPassword([]byte(otp), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("Failed to hash OTP: %v", err)
		return c.JSON(successResponse)
	}

	// Delete old verifications for this user
	_, _ = db.Exec(ctx, `DELETE FROM email_verifications WHERE user_id = $1`, userID)

	// Insert new verification (10-min expiry)
	verID := "ver_" + uuid.New().String()[:8]
	_, err = db.Exec(ctx,
		`INSERT INTO email_verifications (id, user_id, email, otp_hash, attempts, expires_at, created_at)
		 VALUES ($1, $2, $3, $4, 0, $5, NOW())`,
		verID, userID, emailAddr, string(otpHash), time.Now().Add(10*time.Minute),
	)
	if err != nil {
		log.Printf("Failed to create verification record: %v", err)
		return c.JSON(successResponse)
	}

	// Send OTP email (best-effort)
	go func() {
		emailClient := email.GetClient()
		if emailClient.IsConfigured() {
			subject, html := email.VerificationOTPEmail(fullName, otp)
			if err := emailClient.Send(emailAddr, subject, html); err != nil {
				log.Printf("Failed to send OTP email to %s: %v", emailAddr, err)
			}
		} else {
			log.Printf("Email client not configured, skipping OTP email for %s", emailAddr)
		}
	}()

	return c.JSON(successResponse)
}

// ListSessionsFiber returns all active sessions for the authenticated user
// GET /api/v1/auth/sessions
func ListSessionsFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id")
	if userID == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{"code": "UNAUTHORIZED", "message": "Authentication required"},
		})
	}

	// Get current session ID from JWT
	currentSessionID := ""
	if sid := c.Locals("session_id"); sid != nil {
		currentSessionID = sid.(string)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()
	rows, err := db.Query(ctx,
		`SELECT id, user_agent, ip_address, created_at, last_used_at
		 FROM sessions WHERE user_id = $1 AND is_valid = true ORDER BY last_used_at DESC`,
		userID.(string),
	)
	if err != nil {
		log.Printf("Failed to list sessions: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to retrieve sessions"},
		})
	}
	defer rows.Close()

	sessions := []fiber.Map{}
	for rows.Next() {
		var id, userAgent, ipAddress string
		var createdAt, lastUsedAt time.Time
		if err := rows.Scan(&id, &userAgent, &ipAddress, &createdAt, &lastUsedAt); err != nil {
			continue
		}
		sessions = append(sessions, fiber.Map{
			"id":           id,
			"user_agent":   userAgent,
			"ip_address":   ipAddress,
			"created_at":   createdAt.Format(time.RFC3339),
			"last_used_at": lastUsedAt.Format(time.RFC3339),
			"is_current":   id == currentSessionID,
		})
	}

	return c.JSON(fiber.Map{"sessions": sessions})
}

// RevokeSessionFiber revokes a specific session
// DELETE /api/v1/auth/sessions/:id
func RevokeSessionFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id")
	if userID == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{"code": "UNAUTHORIZED", "message": "Authentication required"},
		})
	}

	sessionID := c.Params("id")

	// Prevent revoking current session (must use logout instead)
	currentSessionID := ""
	if sid := c.Locals("session_id"); sid != nil {
		currentSessionID = sid.(string)
	}
	if sessionID == currentSessionID {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "CANNOT_REVOKE_CURRENT", "message": "Cannot revoke your current session. Use logout instead."},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	// Verify session belongs to user
	var ownerID string
	err := db.QueryRow(ctx, `SELECT user_id FROM sessions WHERE id = $1 AND is_valid = true`, sessionID).Scan(&ownerID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Session not found"},
		})
	}
	if ownerID != userID.(string) {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this session"},
		})
	}

	// Revoke the session
	_, err = db.Exec(ctx,
		`UPDATE sessions SET is_valid = false, revoked_at = NOW() WHERE id = $1`, sessionID)
	if err != nil {
		log.Printf("Failed to revoke session: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to revoke session"},
		})
	}

	logSecurityEvent(ctx, userID.(string), "session_revoked", true, c.IP(), c.Get("User-Agent"), map[string]string{"revoked_session": sessionID})

	return c.JSON(fiber.Map{"message": "Session revoked successfully"})
}

// GetNotificationPreferencesFiber returns the user's notification preferences
// GET /api/v1/auth/notification-preferences
func GetNotificationPreferencesFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id")
	if userID == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{"code": "UNAUTHORIZED", "message": "Authentication required"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	var emailNotif, validationComplete, warrantyExpiring, weeklyDigest, ticketUpdates bool
	err := db.QueryRow(ctx,
		`SELECT email_notifications, validation_complete, warranty_expiring, weekly_digest, ticket_updates
		 FROM notification_preferences WHERE user_id = $1`, userID.(string),
	).Scan(&emailNotif, &validationComplete, &warrantyExpiring, &weeklyDigest, &ticketUpdates)
	if err != nil {
		// Return defaults if no row exists
		return c.JSON(fiber.Map{
			"email_notifications": true,
			"validation_complete": true,
			"warranty_expiring":   true,
			"weekly_digest":       false,
			"ticket_updates":      true,
		})
	}

	return c.JSON(fiber.Map{
		"email_notifications": emailNotif,
		"validation_complete": validationComplete,
		"warranty_expiring":   warrantyExpiring,
		"weekly_digest":       weeklyDigest,
		"ticket_updates":      ticketUpdates,
	})
}

// UpdateNotificationPreferencesFiber updates the user's notification preferences
// PUT /api/v1/auth/notification-preferences
func UpdateNotificationPreferencesFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id")
	if userID == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{"code": "UNAUTHORIZED", "message": "Authentication required"},
		})
	}

	var req struct {
		EmailNotifications *bool `json:"email_notifications"`
		ValidationComplete *bool `json:"validation_complete"`
		WarrantyExpiring   *bool `json:"warranty_expiring"`
		WeeklyDigest       *bool `json:"weekly_digest"`
		TicketUpdates      *bool `json:"ticket_updates"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}

	// Use defaults for nil values
	emailNotif := true
	if req.EmailNotifications != nil {
		emailNotif = *req.EmailNotifications
	}
	validationComplete := true
	if req.ValidationComplete != nil {
		validationComplete = *req.ValidationComplete
	}
	warrantyExpiring := true
	if req.WarrantyExpiring != nil {
		warrantyExpiring = *req.WarrantyExpiring
	}
	weeklyDigest := false
	if req.WeeklyDigest != nil {
		weeklyDigest = *req.WeeklyDigest
	}
	ticketUpdates := true
	if req.TicketUpdates != nil {
		ticketUpdates = *req.TicketUpdates
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	_, err := db.Exec(ctx,
		`INSERT INTO notification_preferences (user_id, email_notifications, validation_complete, warranty_expiring, weekly_digest, ticket_updates, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW())
		 ON CONFLICT (user_id) DO UPDATE SET
		   email_notifications = EXCLUDED.email_notifications,
		   validation_complete = EXCLUDED.validation_complete,
		   warranty_expiring = EXCLUDED.warranty_expiring,
		   weekly_digest = EXCLUDED.weekly_digest,
		   ticket_updates = EXCLUDED.ticket_updates,
		   updated_at = NOW()`,
		userID.(string), emailNotif, validationComplete, warrantyExpiring, weeklyDigest, ticketUpdates,
	)
	if err != nil {
		log.Printf("Failed to update notification preferences: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to update notification preferences"},
		})
	}

	return c.JSON(fiber.Map{
		"email_notifications": emailNotif,
		"validation_complete": validationComplete,
		"warranty_expiring":   warrantyExpiring,
		"weekly_digest":       weeklyDigest,
		"ticket_updates":      ticketUpdates,
	})
}
