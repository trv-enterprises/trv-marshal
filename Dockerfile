FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o marshal ./cmd/marshal/

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/marshal /usr/local/bin/marshal

ENTRYPOINT ["marshal"]
CMD ["-config", "/etc/marshal/rules.yaml"]
