# syntax=docker/dockerfile:1
ARG GO_VERSION=1.24.0
FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/servitor ./cmd/servitor

FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /out/servitor /usr/local/bin/servitor
USER 65532:0
ENTRYPOINT ["/usr/local/bin/servitor"]
