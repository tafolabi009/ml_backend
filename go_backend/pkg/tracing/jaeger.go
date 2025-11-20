package tracing

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

// InitJaeger initializes Jaeger tracer
func InitJaeger(serviceName, jaegerEndpoint string) (opentracing.Tracer, io.Closer, error) {
	cfg := &config.Configuration{
		ServiceName: serviceName,
		Sampler: &config.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans:            true,
			BufferFlushInterval: 1 * time.Second,
			LocalAgentHostPort:  jaegerEndpoint,
		},
	}

	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize tracer: %w", err)
	}

	opentracing.SetGlobalTracer(tracer)
	return tracer, closer, nil
}

// TracingMiddleware returns a Fiber middleware that creates spans for each request
func TracingMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		tracer := opentracing.GlobalTracer()

		// Extract span context from headers if present
		spanCtx, _ := tracer.Extract(
			opentracing.HTTPHeaders,
			opentracing.HTTPHeadersCarrier(c.GetReqHeaders()),
		)

		// Start a new span
		span := tracer.StartSpan(
			fmt.Sprintf("%s %s", c.Method(), c.Path()),
			opentracing.ChildOf(spanCtx),
		)
		defer span.Finish()

		// Add tags
		span.SetTag("http.method", c.Method())
		span.SetTag("http.url", c.OriginalURL())
		span.SetTag("component", "go-backend")

		// Store span in context
		c.Locals("span", span)

		// Process request
		err := c.Next()

		// Record status
		span.SetTag("http.status_code", c.Response().StatusCode())
		if err != nil {
			span.SetTag("error", true)
			span.LogKV("error.message", err.Error())
		}

		return err
	}
}

// StartSpan starts a new child span from the request context
func StartSpan(c *fiber.Ctx, operationName string) opentracing.Span {
	parentSpan, ok := c.Locals("span").(opentracing.Span)
	if !ok {
		tracer := opentracing.GlobalTracer()
		return tracer.StartSpan(operationName)
	}

	tracer := opentracing.GlobalTracer()
	return tracer.StartSpan(
		operationName,
		opentracing.ChildOf(parentSpan.Context()),
	)
}

// TraceOrchestratorCall traces a call to the orchestrator
func TraceOrchestratorCall(c *fiber.Ctx, operation string, fn func() error) error {
	span := StartSpan(c, fmt.Sprintf("orchestrator.%s", operation))
	defer span.Finish()

	span.SetTag("service", "orchestrator")
	span.SetTag("operation", operation)

	err := fn()
	if err != nil {
		span.SetTag("error", true)
		span.LogKV("error.message", err.Error())
	}

	return err
}

// TraceDBQuery traces a database query
func TraceDBQuery(c *fiber.Ctx, query string, fn func() error) error {
	span := StartSpan(c, "db.query")
	defer span.Finish()

	span.SetTag("db.type", "postgres")
	span.SetTag("db.statement", query)

	err := fn()
	if err != nil {
		span.SetTag("error", true)
		span.LogKV("error.message", err.Error())
	}

	return err
}

// InjectSpanContext injects span context into HTTP headers
func InjectSpanContext(span opentracing.Span, headers map[string]string) error {
	tracer := opentracing.GlobalTracer()

	// Convert map to http.Header
	httpHeaders := make(http.Header)
	for k, v := range headers {
		httpHeaders.Set(k, v)
	}

	carrier := opentracing.HTTPHeadersCarrier(httpHeaders)
	err := tracer.Inject(span.Context(), opentracing.HTTPHeaders, carrier)
	if err != nil {
		return err
	}

	// Copy back to original map
	for key, values := range httpHeaders {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	return nil
}
