FROM golang:1.25 AS builder

WORKDIR /app

COPY . .

RUN cd services/tasks && go mod tidy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /bin/tasks ./services/tasks/cmd/tasks

FROM debian:stable-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/tasks /bin/tasks

WORKDIR /app

EXPOSE 8082

CMD ["/bin/tasks"]