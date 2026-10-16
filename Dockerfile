FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN CGO_ENABLED=0 go build -trimpath -o /out/wallet-worker ./cmd/wallet-worker  && CGO_ENABLED=0 go build -trimpath -o /out/wallet-migrate ./cmd/wallet-migrate
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl  && rm -rf /var/lib/apt/lists/*  && useradd --uid 10001 --create-home app
COPY --from=build /out/ /usr/local/bin/
USER 10001:10001
EXPOSE 8080
CMD ["wallet-worker"]