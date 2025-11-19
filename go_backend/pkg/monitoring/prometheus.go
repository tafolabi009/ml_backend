package monitoring

import (
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// HTTP Metrics
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	httpRequestsInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Current number of HTTP requests being processed",
		},
	)

	// Validation Metrics
	validationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "validations_total",
			Help: "Total number of validations",
		},
		[]string{"status", "user_id"},
	)

	validationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "validation_duration_seconds",
			Help:    "Validation duration in seconds",
			Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 14400, 28800},
		},
		[]string{"dataset_format"},
	)

	// Dataset Metrics
	datasetsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "datasets_total",
			Help: "Total number of datasets",
		},
		[]string{"format", "status"},
	)

	datasetSizeBytes = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataset_size_bytes",
			Help:    "Dataset size in bytes",
			Buckets: []float64{1e6, 1e7, 1e8, 1e9, 1e10},
		},
		[]string{"format"},
	)

	// Orchestrator Metrics
	orchestratorRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orchestrator_requests_total",
			Help: "Total number of requests to orchestrator",
		},
		[]string{"operation", "status"},
	)

	orchestratorRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orchestrator_request_duration_seconds",
			Help:    "Orchestrator request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// Database Metrics
	dbQueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_queries_total",
			Help: "Total number of database queries",
		},
		[]string{"operation", "status"},
	)

	dbQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Database query duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// Error Metrics
	errorsTotal = promauto.NewCounterVec(
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
		start := time.Now()

		// Increment in-flight requests
		httpRequestsInFlight.Inc()
		defer httpRequestsInFlight.Dec()

		// Process request
		err := c.Next()

		// Record metrics
		duration := time.Since(start).Seconds()
		status := c.Response().StatusCode()
		method := c.Method()
		path := c.Path()

		httpRequestsTotal.WithLabelValues(method, path, string(rune(status))).Inc()
		httpRequestDuration.WithLabelValues(method, path).Observe(duration)

		return err
	}
}

// MetricsHandler returns a Fiber handler for the /metrics endpoint
func MetricsHandler() fiber.Handler {
	return adaptor.HTTPHandler(promhttp.Handler())
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
