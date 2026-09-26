FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /engine ./cmd/engine

FROM alpine:3.22
RUN addgroup -S engine && adduser -S -G engine engine
USER engine
COPY --from=build /engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
