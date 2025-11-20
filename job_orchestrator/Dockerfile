# Build stage
FROM golang:alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git

# Copy proto files to parent directory (for go.mod replace directive)
COPY proto/gen/go /proto/gen/go

# Copy job_orchestrator files
COPY job_orchestrator/go.mod job_orchestrator/go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY job_orchestrator/ .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o job-orchestrator ./main.go

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/job-orchestrator .

# Create necessary directories
RUN mkdir -p /app/logs /etc/synthos/certs

# Expose gRPC port
EXPOSE 50052

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:50052/health || exit 1

# Run the application
CMD ["./job-orchestrator"]
