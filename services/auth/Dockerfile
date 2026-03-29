FROM golang:1.25 AS builder

WORKDIR /app

COPY . .

RUN cd services/auth && go mod tidy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /bin/auth ./services/auth/cmd/auth

FROM debian:stable-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/auth /bin/auth

WORKDIR /app

EXPOSE 8081 50051

CMD ["/bin/auth"]