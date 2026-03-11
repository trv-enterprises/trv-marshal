FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o alert-engine ./cmd/alert-engine/

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/alert-engine /usr/local/bin/alert-engine

ENTRYPOINT ["alert-engine"]
CMD ["-config", "/etc/alert-engine/rules.yaml"]
