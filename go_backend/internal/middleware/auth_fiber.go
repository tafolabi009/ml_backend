package middleware

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/auth"
)

// AuthRequiredFiber is Fiber middleware for authentication
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

		// Validate token
		claims, err := auth.ValidateToken(token)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "INVALID_TOKEN",
					"message": "Invalid or expired token",
				},
			})
		}

		// Store user info in context
		c.Locals("user_id", claims.UserID)
		c.Locals("email", claims.Email)

		return c.Next()
	}
}

// RateLimitFiber is Fiber middleware for rate limiting
func RateLimitFiber() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// TODO: Implement rate limiting logic
		return c.Next()
	}
}
