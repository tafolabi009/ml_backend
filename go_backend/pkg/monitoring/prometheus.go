package monitoring

import (
	"strconv"
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// registry is a dedicated Prometheus registry for this service. Using a private
// registry instead of the global default — and serving it via promhttp.HandlerFor
// rather than promhttp.Handler — avoids InstrumentMetricHandler mutating the very
// registry it gathers on every scrape (a self-modification-during-gather that
// intermittently makes Gather report a duplicate series and return 500).
var registry = prometheus.NewRegistry()
var factory = promauto.With(registry)

func init() {
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

var (
	// HTTP Metrics
	httpRequestsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	httpRequestsInFlight = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Current number of HTTP requests being processed",
		},
	)

	// Validation Metrics
	validationsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "validations_total",
			Help: "Total number of validations",
		},
		[]string{"status", "user_id"},
	)

	validationDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "validation_duration_seconds",
			Help:    "Validation duration in seconds",
			Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 14400, 28800},
		},
		[]string{"dataset_format"},
	)

	// Dataset Metrics
	datasetsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "datasets_total",
			Help: "Total number of datasets",
		},
		[]string{"format", "status"},
	)

	datasetSizeBytes = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataset_size_bytes",
			Help:    "Dataset size in bytes",
			Buckets: []float64{1e6, 1e7, 1e8, 1e9, 1e10},
		},
		[]string{"format"},
	)

	// Orchestrator Metrics
	orchestratorRequestsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orchestrator_requests_total",
			Help: "Total number of requests to orchestrator",
		},
		[]string{"operation", "status"},
	)

	orchestratorRequestDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orchestrator_request_duration_seconds",
			Help:    "Orchestrator request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// Database Metrics
	dbQueriesTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_queries_total",
			Help: "Total number of database queries",
		},
		[]string{"operation", "status"},
	)

	dbQueryDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Database query duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// Error Metrics
	errorsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "errors_total",
			Help: "Total number of errors",
		},
		[]string{"type", "component"},
	)
)

// PrometheusMiddleware returns a Fiber middleware that records metrics
func PrometheusMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Do not instrument the scrape endpoint or the health-check endpoints.
		// These are by far the highest-frequency paths (the ALB health-checks
		// /health/live continuously), and recording their series while the registry
		// is concurrently gathered for a /metrics scrape makes the collector emit a
		// series inconsistently -> /metrics 500 ("collected metric ... was collected
		// before with the same name and label values"). Excluding health/scrape
		// endpoints from instrumentation is also standard Prometheus practice.
		switch c.Path() {
		case "/metrics", "/health", "/health/live", "/health/ready":
			return c.Next()
		}

		start := time.Now()

		// Increment in-flight requests
		httpRequestsInFlight.Inc()
		defer httpRequestsInFlight.Dec()

		// Process request
		err := c.Next()

		// Record metrics.
		// Use the matched route TEMPLATE (e.g. "/api/v1/validations/:id") rather than
		// c.Path(): the raw path is backed by a Fiber buffer that is reused across
		// requests, so retaining it as a label value corrupts the string (e.g.
		// "/health/livedations") and produces inconsistent/duplicate Prometheus label
		// sets — which makes the collector error out and /metrics return 500. The route
		// template is a stable, low-cardinality string.
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Response().StatusCode())
		method := c.Method()
		path := "unmatched"
		if r := c.Route(); r != nil && r.Path != "" {
			// Copy so the label value never aliases a reused Fiber buffer.
			path = utils.CopyString(r.Path)
		}

		httpRequestsTotal.WithLabelValues(method, path, status).Inc()
		httpRequestDuration.WithLabelValues(method, path).Observe(duration)

		return err
	}
}

// MetricsHandler returns a Fiber handler for the /metrics endpoint.
// HandlerFor(registry, ...) serves the dedicated registry without
// InstrumentMetricHandler, so the scrape does not modify the registry it gathers.
// ErrorHandling: ContinueOnError makes a scrape serve the successfully-gathered
// metrics with HTTP 200 even if a single series is momentarily inconsistent under
// concurrent observe+gather, instead of failing the whole scrape with a 500.
func MetricsHandler() fiber.Handler {
	return adaptor.HTTPHandler(promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
}

// RecordValidation records a validation event
func RecordValidation(userID, status string, duration time.Duration) {
	validationsTotal.WithLabelValues(status, userID).Inc()
}

// RecordValidationDuration records validation duration
func RecordValidationDuration(format string, duration time.Duration) {
	validationDuration.WithLabelValues(format).Observe(duration.Seconds())
}

// RecordDataset records a dataset event
func RecordDataset(format, status string, sizeBytes int64) {
	datasetsTotal.WithLabelValues(format, status).Inc()
	datasetSizeBytes.WithLabelValues(format).Observe(float64(sizeBytes))
}

// RecordOrchestratorRequest records an orchestrator request
func RecordOrchestratorRequest(operation, status string, duration time.Duration) {
	orchestratorRequestsTotal.WithLabelValues(operation, status).Inc()
	orchestratorRequestDuration.WithLabelValues(operation).Observe(duration.Seconds())
}

// RecordDBQuery records a database query
func RecordDBQuery(operation, status string, duration time.Duration) {
	dbQueriesTotal.WithLabelValues(operation, status).Inc()
	dbQueryDuration.WithLabelValues(operation).Observe(duration.Seconds())
}

// RecordError records an error
func RecordError(errorType, component string) {
	errorsTotal.WithLabelValues(errorType, component).Inc()
}
