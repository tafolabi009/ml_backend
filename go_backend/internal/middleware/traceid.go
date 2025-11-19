package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// TraceID middleware adds a unique trace ID to each request
func TraceID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		traceID := c.Get("X-Trace-ID")
		if traceID == "" {
			traceID = uuid.New().String()
		}

		c.Locals("trace_id", traceID)
		c.Set("X-Trace-ID", traceID)

		return c.Next()
	}
}
