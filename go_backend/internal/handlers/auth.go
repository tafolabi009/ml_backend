package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/models"
)

// Register handles user registration
func Register(c *gin.Context) {
	var req models.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
		return
	}

	// Hash password
	passwordHash, err := auth.HashPassword(req.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to process password",
			},
		})
		return
	}

	// Create user (in production, this would insert into database)
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

	// TODO: Save user to database

	c.JSON(http.StatusCreated, gin.H{
		"user_id":      user.ID,
		"email":        user.Email,
		"company_id":   user.CompanyID,
		"created_at":   user.CreatedAt.Format(time.RFC3339),
	})
}

// Login handles user authentication
func Login(c *gin.Context) {
	var req models.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
		return
	}

	// TODO: Fetch user from database
	// For now, returning mock data
	user := models.User{
		ID:               "usr_abc123",
		Email:            req.Email,
		FullName:         "John Doe",
		CompanyID:        "cmp_xyz789",
		CompanyName:      "Acme AI Labs",
		SubscriptionTier: "professional",
	}

	// TODO: Verify password hash
	// if !auth.CheckPasswordHash(req.Password, user.PasswordHash) {
	// 	c.JSON(http.StatusUnauthorized, gin.H{...})
	// 	return
	// }

	// Generate tokens
	accessToken, err := auth.GenerateToken(user.ID, user.Email, user.CompanyID, 15*time.Minute)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate access token",
			},
		})
		return
	}

	refreshToken, err := auth.GenerateToken(user.ID, user.Email, user.CompanyID, 30*24*time.Hour)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate refresh token",
			},
		})
		return
	}

	response := models.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    "Bearer",
		ExpiresIn:    900, // 15 minutes in seconds
		User:         user,
	}

	c.JSON(http.StatusOK, response)
}

// RefreshToken handles token refresh
func RefreshToken(c *gin.Context) {
	var req models.RefreshTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
		return
	}

	// Validate refresh token
	claims, err := auth.ValidateToken(req.RefreshToken)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": gin.H{
				"code":    "INVALID_TOKEN",
				"message": "Invalid or expired refresh token",
			},
		})
		return
	}

	// Generate new access token
	accessToken, err := auth.GenerateToken(claims.UserID, claims.Email, claims.CompanyID, 15*time.Minute)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to generate access token",
			},
		})
		return
	}

	response := models.RefreshTokenResponse{
		AccessToken: accessToken,
		ExpiresIn:   900, // 15 minutes
	}

	c.JSON(http.StatusOK, response)
}
