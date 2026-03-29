FROM golang:1.25 AS builder

WORKDIR /app

COPY . .

RUN cd services/graphql && go mod tidy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /bin/graphql ./services/graphql/cmd/graphql

FROM debian:stable-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/graphql /bin/graphql

WORKDIR /app

EXPOSE 8090

CMD ["/bin/graphql"]