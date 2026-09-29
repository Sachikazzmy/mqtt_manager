FROM golang:1.27.1-alpine3.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/broker-setup ./cmd/broker-setup \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/publish ./cmd/publish

FROM alpine:3.23.3
RUN addgroup -g 10001 app && adduser -D -u 10001 -G app app
WORKDIR /app
COPY --from=build /out/ /usr/local/bin/
USER 10001:10001
CMD ["server"]
