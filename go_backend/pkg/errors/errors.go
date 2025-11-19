package errors

import (
	"fmt"
	"net/http"

	"github.com/gofiber/fiber/v2"
)

// ErrorCode represents standardized error codes
type ErrorCode string

const (
	ErrInternalServer     ErrorCode = "INTERNAL_SERVER_ERROR"
	ErrBadRequest         ErrorCode = "BAD_REQUEST"
	ErrUnauthorized       ErrorCode = "UNAUTHORIZED"
	ErrForbidden          ErrorCode = "FORBIDDEN"
	ErrNotFound           ErrorCode = "NOT_FOUND"
	ErrConflict           ErrorCode = "CONFLICT"
	ErrValidation         ErrorCode = "VALIDATION_ERROR"
	ErrRateLimit          ErrorCode = "RATE_LIMIT_EXCEEDED"
	ErrServiceUnavailable ErrorCode = "SERVICE_UNAVAILABLE"
	ErrTimeout            ErrorCode = "REQUEST_TIMEOUT"
	ErrDatabaseError      ErrorCode = "DATABASE_ERROR"
	ErrInvalidCredentials ErrorCode = "INVALID_CREDENTIALS"
)

// AppError represents a standardized application error
type AppError struct {
	Code       ErrorCode         `json:"code"`
	Message    string            `json:"message"`
	Details    map[string]string `json:"details,omitempty"`
	StatusCode int               `json:"-"`
	TraceID    string            `json:"trace_id,omitempty"`
}

// Error implements the error interface
func (e *AppError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// NewAppError creates a new application error
func NewAppError(code ErrorCode, message string, statusCode int) *AppError {
	return &AppError{
		Code:       code,
		Message:    message,
		StatusCode: statusCode,
		Details:    make(map[string]string),
	}
}

// WithDetails adds details to the error
func (e *AppError) WithDetails(key, value string) *AppError {
	e.Details[key] = value
	return e
}

// WithTraceID adds trace ID to the error
func (e *AppError) WithTraceID(traceID string) *AppError {
	e.TraceID = traceID
	return e
}

// Standard error constructors
func BadRequest(message string) *AppError {
	return NewAppError(ErrBadRequest, message, http.StatusBadRequest)
}

func Unauthorized(message string) *AppError {
	return NewAppError(ErrUnauthorized, message, http.StatusUnauthorized)
}

func Forbidden(message string) *AppError {
	return NewAppError(ErrForbidden, message, http.StatusForbidden)
}

func NotFound(message string) *AppError {
	return NewAppError(ErrNotFound, message, http.StatusNotFound)
}

func Conflict(message string) *AppError {
	return NewAppError(ErrConflict, message, http.StatusConflict)
}

func InternalServer(message string) *AppError {
	return NewAppError(ErrInternalServer, message, http.StatusInternalServerError)
}

func ValidationError(message string) *AppError {
	return NewAppError(ErrValidation, message, http.StatusBadRequest)
}

func RateLimitExceeded(message string) *AppError {
	return NewAppError(ErrRateLimit, message, http.StatusTooManyRequests)
}

func ServiceUnavailable(message string) *AppError {
	return NewAppError(ErrServiceUnavailable, message, http.StatusServiceUnavailable)
}

func Timeout(message string) *AppError {
	return NewAppError(ErrTimeout, message, http.StatusRequestTimeout)
}

// ErrorResponse formats the error response
type ErrorResponse struct {
	Error AppError `json:"error"`
}

// HandleError handles errors in Fiber context
func HandleError(c *fiber.Ctx, err error) error {
	traceID, _ := c.Locals("trace_id").(string)

	// Convert generic errors to AppError
	appErr, ok := err.(*AppError)
	if !ok {
		appErr = InternalServer("An unexpected error occurred")
		appErr.TraceID = traceID
	} else {
		appErr.TraceID = traceID
	}

	return c.Status(appErr.StatusCode).JSON(ErrorResponse{
		Error: *appErr,
	})
}
