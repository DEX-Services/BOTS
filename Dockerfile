# Render deploys this via runtime: docker.
# Builds the cmd/bots entrypoint into a small static binary.

# ---- Build stage ----
FROM golang:1.24-alpine AS builder
WORKDIR /src

# Cache module downloads first (separate layer) so rebuilds are fast.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# -tags netgo: static binary, no cgo DNS issues on the slim runtime.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -tags netgo \
    -ldflags '-s -w' \
    -o /out/bots \
    ./cmd/bots

# ---- Runtime stage ----
# Alpine base keeps it tiny; ca-certificates are REQUIRED for TLS to
# Aiven Postgres/Redis.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
RUN addgroup -S app && adduser -S -G app app
COPY --from=builder /out/bots /usr/local/bin/bots
USER app
# Render sets $PORT and routes to it; config.go now reads PORT first, falling
# back to BOTS_PORT, then 8082.
CMD ["bots"]
