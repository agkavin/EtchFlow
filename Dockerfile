# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Download dependencies first (cached layer unless go.mod/go.sum change)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o etchflow ./cmd/server

# ── Stage 2: Minimal Runtime ─────────────────────────────────────────────────
# distroless/static: no shell, no libc, ~2MB image.
# Safe for Phase MVP + 1.5 (pure Go, CGO disabled).
FROM gcr.io/distroless/static-debian12

COPY --from=builder /app/etchflow /etchflow

EXPOSE 8080
ENTRYPOINT ["/etchflow"]
