# Stage 1: Build
FROM go:1.26.1-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o main ./cmd/server

# Stage 2: Production
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/main .
COPY --from=builder /app/internal/db/migrations /root/internal/db/migrations
EXPOSE 8080
CMD ["./main"]
