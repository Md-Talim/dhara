# syntax=docker/dockerfile:1

# Single multi-binary build: pass BINARY=server|worker|migrate via build args.
# Each docker compose service builds its own image with its target binary.

# ---- Stage 1: Build ----
FROM golang:1.26.1-alpine AS builder
ARG BINARY=server
WORKDIR /app

# Cache module downloads before copying source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dhara ./cmd/${BINARY}

# ---- Stage 2: Runtime ----
FROM alpine:3.21
RUN adduser -D -u 10001 dhara
USER dhara
COPY --from=builder /out/dhara /usr/local/bin/dhara
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dhara"]
