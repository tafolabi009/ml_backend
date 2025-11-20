# Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy proto files to parent directory (for go.mod replace directive)
COPY proto/gen/go /proto/gen/go

# Copy go_backend files
COPY go_backend/go.mod go_backend/go.sum ./
RUN go mod download

# Copy source code
COPY go_backend/ .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o api cmd/api/main.go

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/api .

# Expose port
EXPOSE 8080

# Set environment
ENV ENVIRONMENT=production
ENV PORT=8080

# Run the application
CMD ["./api"]
