# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o data-service ./main.go

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata curl

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/data-service .

# Create necessary directories
RUN mkdir -p /app/logs /app/temp /etc/synthos/certs

# Expose gRPC port
EXPOSE 50054

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:50054/health || exit 1

# Run the application
CMD ["./data-service"]
