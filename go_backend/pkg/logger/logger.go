package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	once         sync.Once
)

// Logger is a wrapper around zap.Logger
type Logger struct {
	*zap.Logger
}

// Initialize sets up the global logger
func Initialize(level string) error {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}

	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zapLevel)
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}

	logger, err := config.Build()
	if err != nil {
		return err
	}

	globalLogger = logger
	return nil
}

// Get returns the global logger instance
func Get() *Logger {
	once.Do(func() {
		if globalLogger == nil {
			config := zap.NewProductionConfig()
			config.OutputPaths = []string{"stdout"}
			logger, _ := config.Build()
			globalLogger = logger
		}
	})
	return &Logger{globalLogger}
}

// With adds structured context to the logger
func (l *Logger) With(fields ...interface{}) *Logger {
	zapFields := make([]zap.Field, 0, len(fields)/2)
	for i := 0; i < len(fields)-1; i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		zapFields = append(zapFields, zap.Any(key, fields[i+1]))
	}
	return &Logger{l.Logger.With(zapFields...)}
}

// Info logs an info message with key-value pairs
func (l *Logger) Info(msg string, keysAndValues ...interface{}) {
	zapFields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			zapFields = append(zapFields, zap.Any(key, keysAndValues[i+1]))
		}
	}
	l.Logger.Info(msg, zapFields...)
}

// Error logs an error message with key-value pairs
func (l *Logger) Error(msg string, keysAndValues ...interface{}) {
	zapFields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			zapFields = append(zapFields, zap.Any(key, keysAndValues[i+1]))
		}
	}
	l.Logger.Error(msg, zapFields...)
}

// Warn logs a warning message with key-value pairs
func (l *Logger) Warn(msg string, keysAndValues ...interface{}) {
	zapFields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			zapFields = append(zapFields, zap.Any(key, keysAndValues[i+1]))
		}
	}
	l.Logger.Warn(msg, zapFields...)
}

// Debug logs a debug message with key-value pairs
func (l *Logger) Debug(msg string, keysAndValues ...interface{}) {
	zapFields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			zapFields = append(zapFields, zap.Any(key, keysAndValues[i+1]))
		}
	}
	l.Logger.Debug(msg, zapFields...)
}

// Sync flushes any buffered log entries
func Sync() {
	if globalLogger != nil {
		_ = globalLogger.Sync()
	}
}

func init() {
	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		level = "info"
	}
	_ = Initialize(level)
}
