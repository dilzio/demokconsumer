# Step 1: Build the Go app in a builder container
FROM golang:1.23.1-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy the go.mod and go.sum files
COPY go.mod go.sum ./

# Download the Go module dependencies
RUN go mod download

# Copy the rest of the application code
COPY . .

# Build the Go app
RUN go build -o /app/kafka-consumer-server

# Step 2: Create a lightweight container to run the service
FROM alpine:latest

# Install ca-certificates for secure Kafka communication and AWS SDK usage
RUN apk --no-cache add ca-certificates

# Set the working directory
WORKDIR /root/

# Copy the built binary from the builder stage
COPY --from=builder /app/kafka-consumer-server .

# Expose the port (if any communication occurs)
# EXPOSE <port>

# Run the consumer server
CMD ["./kafka-consumer-server"]
