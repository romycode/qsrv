# syntax=docker/dockerfile:1

# qsrv with the cgo SQLite driver (mattn/go-sqlite3: native SQLite, ~1.7-1.9x
# less CPU than the WASM build), linked statically against musl: the runtime
# image needs no libc. Cross-compiled with clang through tonistiigi/xx, so
# arm64 builds on an amd64 runner without QEMU. qctl has no SQLite: plain Go.
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.10.0 AS xx

FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
COPY --from=xx / /
RUN apk add --no-cache clang lld
ARG TARGETPLATFORM
RUN xx-apk add --no-cache musl-dev gcc
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build,id=gobuild-$TARGETPLATFORM \
    CGO_ENABLED=1 xx-go build -trimpath \
      -tags sqlite_cgo,sqlite_omit_load_extension,netgo,osusergo \
      -ldflags="-s -w -linkmode external -extldflags '-static' -X main.version=${VERSION}" \
      -o /out/qsrv ./cmd/qsrv \
 && xx-verify --static /out/qsrv \
 && CGO_ENABLED=0 xx-go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/qctl ./cmd/qctl \
 && mkdir -p /out/data

# distroless/static: CA certificates, tzdata and a nonroot user, no shell.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/qsrv /out/qctl /usr/local/bin/
COPY --from=build --chown=nonroot:nonroot /out/data /data

# Configure with the usual flags or env vars. API keys are required, as
# tenant:name:key entries: QSRV_API_KEYS, or QSRV_API_KEYS_FILE pointing at a
# mounted secret (reloaded on SIGHUP: docker kill -s HUP).
ENV QSRV_ADDR=:8080 \
    QSRV_DATA_DIR=/data \
    QSRV_METRICS_ADDR=:9464
VOLUME /data
EXPOSE 8080 9464
USER nonroot:nonroot
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/qsrv", "-healthcheck"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/qsrv"]
