package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/handlers"
	"github.com/tafolabi009/backend/go_backend/internal/middleware"
	"github.com/tafolabi009/backend/go_backend/pkg/orchestrator"
)

func setupTestApp() *fiber.App {
	auth.InitJWT("test-secret")

	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "INTERNAL_ERROR",
					"message": err.Error(),
				},
			})
		},
	})

	// Public routes
	app.Post("/api/v1/auth/register", handlers.RegisterFiber)
	app.Post("/api/v1/auth/login", handlers.LoginFiber)
	app.Post("/api/v1/auth/refresh", handlers.RefreshTokenFiber)

	// Protected routes
	protected := app.Group("/api/v1", middleware.AuthRequiredFiber())
	protected.Post("/datasets/upload", handlers.InitiateUploadFiber)
	protected.Post("/validations/create", handlers.CreateValidationFiber)
	protected.Get("/validations/:id", handlers.GetValidationFiber)

	return app
}

func TestFullAuthFlow(t *testing.T) {
	app := setupTestApp()

	// Step 1: Register
	registerPayload := map[string]interface{}{
		"email":    "integration@test.com",
		"password": "SecurePassword123!",
		"name":     "Integration Test",
		"company":  "Test Corp",
	}

	body, _ := json.Marshal(registerPayload)
	req := httptest.NewRequest("POST", "/api/v1/auth/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusCreated, resp.StatusCode)

	var registerResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&registerResp)
	assert.Contains(t, registerResp, "user_id")
	assert.Equal(t, "integration@test.com", registerResp["email"])

	// Step 2: Login
	loginPayload := map[string]interface{}{
		"email":    "integration@test.com",
		"password": "SecurePassword123!",
	}

	body, _ = json.Marshal(loginPayload)
	req = httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err = app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)

	var loginResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	assert.Contains(t, loginResp, "access_token")
	assert.Contains(t, loginResp, "refresh_token")

	accessToken := loginResp["access_token"].(string)
	refreshToken := loginResp["refresh_token"].(string)

	// Step 3: Access Protected Route
	req = httptest.NewRequest("POST", "/api/v1/datasets/upload", bytes.NewReader([]byte(`{
		"filename": "test.parquet",
		"size": 1024,
		"format": "parquet"
	}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err = app.Test(req)
	require.NoError(t, err)
	// Should succeed with valid token
	assert.NotEqual(t, fiber.StatusUnauthorized, resp.StatusCode)

	// Step 4: Refresh Token
	refreshPayload := map[string]interface{}{
		"refresh_token": refreshToken,
	}

	body, _ = json.Marshal(refreshPayload)
	req = httptest.NewRequest("POST", "/api/v1/auth/refresh", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err = app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)

	var refreshResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&refreshResp)
	assert.Contains(t, refreshResp, "access_token")
	assert.Contains(t, refreshResp, "refresh_token")
}

func TestValidationPipelineFlow(t *testing.T) {
	t.Skip("Requires running orchestrator service")

	app := setupTestApp()

	// Login first
	loginPayload := map[string]interface{}{
		"email":    "test@example.com",
		"password": "SecurePassword123!",
	}

	body, _ := json.Marshal(loginPayload)
	req := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	var loginResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	accessToken := loginResp["access_token"].(string)

	// Create validation
	validationPayload := map[string]interface{}{
		"dataset_id": "ds_test123",
		"options": map[string]interface{}{
			"priority":                  "express",
			"enable_collapse_detection": false,
			"enable_recommendations":    false,
		},
	}

	body, _ = json.Marshal(validationPayload)
	req = httptest.NewRequest("POST", "/api/v1/validations/create", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err = app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusCreated, resp.StatusCode)

	var validationResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&validationResp)
	assert.Contains(t, validationResp, "validation_id")
	assert.Contains(t, validationResp, "status")

	validationID := validationResp["validation_id"].(string)

	// Poll for validation status
	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Second)

		req = httptest.NewRequest("GET", "/api/v1/validations/"+validationID, nil)
		req.Header.Set("Authorization", "Bearer "+accessToken)

		resp, err = app.Test(req)
		require.NoError(t, err)

		var statusResp map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&statusResp)

		if statusResp["status"] == "completed" || statusResp["status"] == "failed" {
			break
		}
	}
}

func TestUnauthorizedAccess(t *testing.T) {
	app := setupTestApp()

	protectedEndpoints := []string{
		"/api/v1/datasets/upload",
		"/api/v1/validations/create",
		"/api/v1/validations/val_123",
	}

	for _, endpoint := range protectedEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			req := httptest.NewRequest("GET", endpoint, nil)
			resp, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
		})
	}
}

func TestInvalidToken(t *testing.T) {
	app := setupTestApp()

	req := httptest.NewRequest("GET", "/api/v1/validations/val_123", nil)
	req.Header.Set("Authorization", "Bearer invalid.token.here")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
}

func TestOrchestratorClient(t *testing.T) {
	t.Skip("Requires running orchestrator service")

	client, err := orchestrator.NewClient("http://localhost:8080")
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Test resource status
	status, err := client.GetResourceStatus(ctx)
	require.NoError(t, err)
	assert.NotNil(t, status)
	assert.GreaterOrEqual(t, status.TotalWorkers, 0)

	// Test job creation
	jobReq := &orchestrator.CreateJobRequest{
		UserID:   "test_user",
		JobType:  "validation",
		Priority: 5,
		Payload: map[string]string{
			"dataset_path": "/data/test.parquet",
			"data_format":  "parquet",
		},
	}

	jobResp, err := client.CreateJob(ctx, jobReq)
	require.NoError(t, err)
	assert.NotEmpty(t, jobResp.JobID)
	assert.Equal(t, "queued", jobResp.Status)

	// Test job status
	job, err := client.GetJob(ctx, jobResp.JobID)
	require.NoError(t, err)
	assert.Equal(t, jobResp.JobID, job.JobID)
}

func TestConcurrentRequests(t *testing.T) {
	app := setupTestApp()

	// Generate token
	auth.InitJWT("test-secret")
	tokens, _ := auth.GenerateTokenPair("test_user", "test@example.com")

	concurrency := 10
	done := make(chan bool, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer func() { done <- true }()

			req := httptest.NewRequest("GET", fmt.Sprintf("/api/v1/validations/val_%d", id), nil)
			req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)

			resp, err := app.Test(req)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < concurrency; i++ {
		<-done
	}
}
