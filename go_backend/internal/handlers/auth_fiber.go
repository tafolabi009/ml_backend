package handlers

import (
	"context"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Register handles user registration - Fiber version
func RegisterFiber(c *fiber.Ctx) error {
	var req models.RegisterRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
	}

	// Hash password
	passwordHash, err := auth.HashPassword(req.Password)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to process password",
			},
		})
	}

	// Create user
	user := models.User{
		ID:               "usr_" + uuid.New().String()[:8],
		Email:            req.Email,
		PasswordHash:     passwordHash,
		FullName:         req.FullName,
		CompanyID:        "cmp_" + uuid.New().String()[:8],
		CompanyName:      req.CompanyName,
		SubscriptionTier: "free",
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	// Save user to database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())
	if err := userRepo.Create(ctx, &user); err != nil {
		// Check if it's a duplicate email error
		if err.Error() == "ERROR: duplicate key value violates unique constraint" ||
			err.Error() == "pq: duplicate key value violates unique constraint \"users_email_key\"" {
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

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"user_id":    user.ID,
		"email":      user.Email,
		"company_id": user.CompanyID,
		"created_at": user.CreatedAt.Format(time.RFC3339),
	})
}

// Login handles user authentication - Fiber version
func LoginFiber(c *fiber.Ctx) error {
	var req models.LoginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
	}

	// Fetch user from database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())
	user, err := userRepo.GetByEmail(ctx, req.Email)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_CREDENTIALS",
				"message": "Invalid email or password",
			},
		})
	}

	// Verify password hash
	if !auth.CheckPasswordHash(req.Password, user.PasswordHash) {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_CREDENTIALS",
				"message": "Invalid email or password",
			},
		})
	}

	// Generate tokens
	accessToken, err := auth.GenerateToken(user.ID, user.Email, user.CompanyID, 15*time.Minute)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate access token",
			},
		})
	}

	refreshToken, err := auth.GenerateToken(user.ID, user.Email, user.CompanyID, 30*24*time.Hour)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate refresh token",
			},
		})
	}

	response := models.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    "Bearer",
		ExpiresIn:    900, // 15 minutes in seconds
		User:         *user,
	}

	return c.JSON(response)
}

// RefreshToken handles token refresh - Fiber version
func RefreshTokenFiber(c *fiber.Ctx) error {
	var req models.RefreshTokenRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
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

	// Generate new access token
	accessToken, err := auth.GenerateToken(claims.UserID, claims.Email, claims.CompanyID, 15*time.Minute)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate access token",
			},
		})
	}

	response := models.RefreshTokenResponse{
		AccessToken: accessToken,
		ExpiresIn:   900, // 15 minutes
	}

	return c.JSON(response)
}
