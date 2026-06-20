package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/template/html/v2"
)

// checkOK returns an error if the HTTP response is not a 2xx status, so callers
// don't silently decode a 4xx/5xx error body (often HTML) into an empty result.
func checkOK(resp *http.Response) error {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("orchestrator returned status %d", resp.StatusCode)
	}
	return nil
}

type OrchestratorClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewOrchestratorClient(baseURL string) *OrchestratorClient {
	return &OrchestratorClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *OrchestratorClient) GetResourceStatus(ctx context.Context) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/api/v1/resources/status", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := checkOK(resp); err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *OrchestratorClient) GetMetrics(ctx context.Context) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/metrics", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := checkOK(resp); err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *OrchestratorClient) ListJobs(ctx context.Context, page, pageSize int) ([]map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v1/jobs?page=%d&page_size=%d", c.baseURL, page, pageSize)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := checkOK(resp); err != nil {
		return nil, err
	}

	var result struct {
		Jobs []map[string]interface{} `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Jobs, nil
}

func main() {
	// Configuration
	orchestratorURL := os.Getenv("ORCHESTRATOR_ADDR")
	if orchestratorURL == "" {
		orchestratorURL = "http://localhost:8080"
	}

	port := os.Getenv("ADMIN_PORT")
	if port == "" {
		port = "3001"
	}

	adminUser := os.Getenv("ADMIN_USER")
	if adminUser == "" {
		adminUser = "admin"
	}

	adminPassword := os.Getenv("ADMIN_PASSWORD")
	if adminPassword == "" {
		// Never ship a guessable default for an admin panel that proxies job and
		// resource control. Generate a strong random password and log it once so
		// the operator can use it; production should set ADMIN_PASSWORD explicitly.
		buf := make([]byte, 24)
		if _, err := rand.Read(buf); err != nil {
			log.Fatalf("ADMIN_PASSWORD not set and failed to generate a random password: %v", err)
		}
		adminPassword = hex.EncodeToString(buf)
		log.Printf("WARNING: ADMIN_PASSWORD not set; generated a random password for this session: %s", adminPassword)
		log.Println("Set ADMIN_PASSWORD to a stable secret for production deployments.")
	}

	// Initialize orchestrator client
	client := NewOrchestratorClient(orchestratorURL)

	// Setup template engine
	engine := html.New("./views", ".html")
	engine.Reload(true)

	// Create Fiber app
	app := fiber.New(fiber.Config{
		Views: engine,
	})

	// Middleware
	app.Use(logger.New())
	app.Use(cors.New())

	// Basic auth for all routes
	app.Use(basicauth.New(basicauth.Config{
		Users: map[string]string{
			adminUser: adminPassword,
		},
		Realm: "Admin Dashboard",
	}))

	// Static files
	app.Static("/static", "./static")

	// Routes
	app.Get("/", func(c *fiber.Ctx) error {
		return c.Render("dashboard", fiber.Map{
			"Title": "Synthos Admin Dashboard",
		})
	})

	// API endpoints for dashboard
	app.Get("/api/status", func(c *fiber.Ctx) error {
		status, err := client.GetResourceStatus(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(status)
	})

	app.Get("/api/metrics", func(c *fiber.Ctx) error {
		metrics, err := client.GetMetrics(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(metrics)
	})

	app.Get("/api/jobs", func(c *fiber.Ctx) error {
		page := c.QueryInt("page", 1)
		pageSize := c.QueryInt("page_size", 20)

		jobs, err := client.ListJobs(c.Context(), page, pageSize)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(fiber.Map{
			"jobs": jobs,
		})
	})

	// Health check
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "healthy",
		})
	})

	log.Printf("Admin Dashboard starting on port %s", port)
	log.Printf("Orchestrator URL: %s", orchestratorURL)
	log.Printf("Login credentials: %s / %s", adminUser, "********")

	if err := app.Listen(":" + port); err != nil {
		log.Fatal(err)
	}
}
