FROM golang:1.25 AS builder

WORKDIR /app

COPY . .

RUN cd services/worker/cmd && go mod tidy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /bin/worker ./services/worker/cmd

FROM debian:stable-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/worker /bin/worker

WORKDIR /app

CMD ["/bin/worker"]
