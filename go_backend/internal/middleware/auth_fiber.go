package middleware

import (
	"context"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// AuthRequiredFiber is Fiber middleware for JWT authentication
func AuthRequiredFiber() fiber.Handler {
	return func(c *fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "UNAUTHORIZED",
					"message": "Authorization header required",
				},
			})
		}

		// Extract token from "Bearer <token>"
		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "INVALID_TOKEN",
					"message": "Invalid authorization header format",
				},
			})
		}

		token := parts[1]

		// Check if it's an API key (starts with "sk_")
		if strings.HasPrefix(token, "sk_") {
			return authenticateWithAPIKey(c, token)
		}

		// Validate JWT token
		claims, err := auth.ValidateToken(token)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "INVALID_TOKEN",
					"message": "Invalid or expired token",
				},
			})
		}

		// A refresh token must never be accepted as a bearer/access token.
		// (Tokens issued before token typing have an empty type and still pass.)
		if claims.TokenType == auth.TokenTypeRefresh {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "INVALID_TOKEN",
					"message": "Refresh token cannot be used for authentication",
				},
			})
		}

		// Check if token is blacklisted
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		userRepo := repository.NewUserRepository(database.GetDB())
		blacklisted, blErr := userRepo.IsTokenBlacklisted(ctx, auth.HashToken(token))
		if blErr != nil || blacklisted {
			// Fail closed: if we cannot verify the token is not revoked, deny.
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "TOKEN_REVOKED",
					"message": "Token has been revoked",
				},
			})
		}

		// Verify session is still valid (if session ID is in claims)
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

		// Check if user is still active
		var isActive bool
		err = database.GetDB().QueryRow(ctx, `SELECT is_active FROM users WHERE id = $1`, claims.UserID).Scan(&isActive)
		if err != nil || !isActive {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": fiber.Map{"code": "ACCOUNT_SUSPENDED", "message": "Your account has been suspended"},
			})
		}

		// Store user info in context for downstream handlers
		c.Locals("user_id", claims.UserID)
		c.Locals("email", claims.Email)
		c.Locals("company_id", claims.CompanyID)
		c.Locals("role", claims.Role)
		c.Locals("session_id", claims.SessionID)
		c.Locals("auth_type", "jwt")

		return c.Next()
	}
}

// authenticateWithAPIKey validates API key authentication
func authenticateWithAPIKey(c *fiber.Ctx, apiKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())
	keyHash := auth.HashToken(apiKey)

	key, err := userRepo.GetAPIKeyByHash(ctx, keyHash)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_API_KEY",
				"message": "Invalid or expired API key",
			},
		})
	}

	// Update last used timestamp (async, don't block)
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = userRepo.UpdateAPIKeyLastUsed(bgCtx, key.ID)
	}()

	// Store auth info in context
	c.Locals("user_id", key.UserID)
	c.Locals("api_key_id", key.ID)
	c.Locals("api_key_scopes", key.Scopes)
	c.Locals("auth_type", "api_key")

	return c.Next()
}

// RequireScopes middleware checks if the authenticated request has required scopes
func RequireScopes(requiredScopes ...string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		authType := c.Locals("auth_type")

		// JWT auth has all scopes by default
		if authType == "jwt" {
			return c.Next()
		}

		// Check API key scopes
		scopes := c.Locals("api_key_scopes")
		if scopes == nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "INSUFFICIENT_SCOPE",
					"message": "Insufficient permissions",
				},
			})
		}

		keyScopes := scopes.([]string)
		scopeMap := make(map[string]bool)
		for _, s := range keyScopes {
			scopeMap[s] = true
		}

		// Check if user has admin scope (grants all access)
		if scopeMap["admin"] {
			return c.Next()
		}

		// Check required scopes
		for _, required := range requiredScopes {
			if !scopeMap[required] {
				return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
					"error": fiber.Map{
						"code":    "INSUFFICIENT_SCOPE",
						"message": "Missing required scope: " + required,
					},
				})
			}
		}

		return c.Next()
	}
}

// roleLevel defines the hierarchy of roles. Higher level implies access to all lower levels.
var roleLevel = map[string]int{
	"admin":     100,
	"developer": 75,
	"support":   50,
	"user":      25,
}

// RequireRole middleware checks if the authenticated user has a role at or above
// the highest required level. For example, RequireRole("support") admits admin,
// developer, and support users because their levels are >= 50.
func RequireRole(roles ...string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		userRole := c.Locals("role")
		if userRole == nil {
			// For API key auth, fetch user role
			userID := c.Locals("user_id")
			if userID != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				userRepo := repository.NewUserRepository(database.GetDB())
				user, err := userRepo.GetByID(ctx, userID.(string))
				if err == nil && user.Role != nil {
					userRole = *user.Role
				}
			}
		}

		if userRole == nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "FORBIDDEN",
					"message": "Insufficient permissions",
				},
			})
		}

		role := userRole.(string)

		// Determine the highest required level from the roles parameter
		requiredLevel := 0
		for _, r := range roles {
			if lvl, ok := roleLevel[r]; ok && lvl > requiredLevel {
				requiredLevel = lvl
			}
		}

		// Check if the user's role level meets or exceeds the required level
		userLvl, ok := roleLevel[role]
		if !ok {
			userLvl = 0
		}

		if userLvl >= requiredLevel {
			return c.Next()
		}

		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "Insufficient permissions",
			},
		})
	}
}

// RateLimitFiber is Fiber middleware for rate limiting
func RateLimitFiber() fiber.Handler {
	return RateLimitWithConfig(100, time.Minute)
}
