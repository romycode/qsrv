# syntax=docker/dockerfile:1
# Traffic generator for the development stack (deploy/dev). Not published.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/qdevload ./cmd/qdevload

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/qdevload /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/qdevload"]
