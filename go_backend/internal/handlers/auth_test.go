package handlers

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tafolabi009/backend/go_backend/internal/auth"
)

func TestRegisterFiber(t *testing.T) {
	// Initialize JWT for testing
	auth.InitJWT("test-secret")

	tests := []struct {
		name           string
		payload        map[string]interface{}
		expectedStatus int
		checkResponse  func(t *testing.T, body map[string]interface{})
	}{
		{
			name: "Valid Registration",
			payload: map[string]interface{}{
				"email":    "test@example.com",
				"password": "SecurePassword123!",
				"name":     "Test User",
				"company":  "Test Company",
			},
			expectedStatus: fiber.StatusCreated,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "user_id")
				assert.Contains(t, body, "email")
				assert.Equal(t, "test@example.com", body["email"])
			},
		},
		{
			name: "Missing Email",
			payload: map[string]interface{}{
				"password": "SecurePassword123!",
				"name":     "Test User",
			},
			expectedStatus: fiber.StatusBadRequest,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "error")
			},
		},
		{
			name: "Weak Password",
			payload: map[string]interface{}{
				"email":    "test@example.com",
				"password": "weak",
				"name":     "Test User",
			},
			expectedStatus: fiber.StatusBadRequest,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "error")
			},
		},
		{
			name: "Invalid Email Format",
			payload: map[string]interface{}{
				"email":    "invalid-email",
				"password": "SecurePassword123!",
				"name":     "Test User",
			},
			expectedStatus: fiber.StatusBadRequest,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "error")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New()
			app.Post("/register", RegisterFiber)

			body, _ := json.Marshal(tt.payload)
			req := httptest.NewRequest("POST", "/register", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			var responseBody map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&responseBody)
			tt.checkResponse(t, responseBody)
		})
	}
}

func TestLoginFiber(t *testing.T) {
	auth.InitJWT("test-secret")

	tests := []struct {
		name           string
		payload        map[string]interface{}
		expectedStatus int
		checkResponse  func(t *testing.T, body map[string]interface{})
	}{
		{
			name: "Valid Login",
			payload: map[string]interface{}{
				"email":    "test@example.com",
				"password": "SecurePassword123!",
			},
			expectedStatus: fiber.StatusOK,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "access_token")
				assert.Contains(t, body, "refresh_token")
				assert.Contains(t, body, "expires_in")
			},
		},
		{
			name: "Missing Password",
			payload: map[string]interface{}{
				"email": "test@example.com",
			},
			expectedStatus: fiber.StatusBadRequest,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "error")
			},
		},
		{
			name: "Invalid Credentials",
			payload: map[string]interface{}{
				"email":    "test@example.com",
				"password": "WrongPassword",
			},
			expectedStatus: fiber.StatusUnauthorized,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "error")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New()
			app.Post("/login", LoginFiber)

			body, _ := json.Marshal(tt.payload)
			req := httptest.NewRequest("POST", "/login", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			var responseBody map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&responseBody)
			tt.checkResponse(t, responseBody)
		})
	}
}

func TestRefreshTokenFiber(t *testing.T) {
	auth.InitJWT("test-secret")

	// Generate a valid refresh token
	token, _ := auth.GenerateTokenPair("user_123", "test@example.com")

	tests := []struct {
		name           string
		payload        map[string]interface{}
		expectedStatus int
		checkResponse  func(t *testing.T, body map[string]interface{})
	}{
		{
			name: "Valid Refresh Token",
			payload: map[string]interface{}{
				"refresh_token": token.RefreshToken,
			},
			expectedStatus: fiber.StatusOK,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "access_token")
				assert.Contains(t, body, "refresh_token")
			},
		},
		{
			name: "Invalid Refresh Token",
			payload: map[string]interface{}{
				"refresh_token": "invalid.token.here",
			},
			expectedStatus: fiber.StatusUnauthorized,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "error")
			},
		},
		{
			name:           "Missing Refresh Token",
			payload:        map[string]interface{}{},
			expectedStatus: fiber.StatusBadRequest,
			checkResponse: func(t *testing.T, body map[string]interface{}) {
				assert.Contains(t, body, "error")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New()
			app.Post("/refresh", RefreshTokenFiber)

			body, _ := json.Marshal(tt.payload)
			req := httptest.NewRequest("POST", "/refresh", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			var responseBody map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&responseBody)
			tt.checkResponse(t, responseBody)
		})
	}
}
