#!/usr/bin/env bash
# Runs qsrv as configured by scripts/setup.sh (.qsrv/config.env). Used by
# the Makefile: make up | down | restart | logs | status | keys | add-key.
set -euo pipefail
cd "$(dirname "$0")/.."

DIR=.qsrv
CONFIG=$DIR/config.env
KEYS=$DIR/keys
PIDFILE=$DIR/qsrv.pid
LOG=$DIR/qsrv.log
CONTAINER=qsrv
COMPOSE=(docker compose -f deploy/monitoring/docker-compose.yml)

die()  { printf '\033[31m%s\033[0m\n' "$*" >&2; exit 1; }
note() { printf '  %s\n' "$*"; }

[ -f "$CONFIG" ] || die "qsrv is not set up yet: run make setup"
# shellcheck disable=SC1090
. "$CONFIG"

scheme=http; [ -n "${TLS_CERT:-}" ] && scheme=https
url="$scheme://localhost:$PORT"

healthy() { curl -skf "$url/healthz" >/dev/null 2>&1; }

wait_healthy() {
  for _ in $(seq 60); do healthy && return 0; sleep 0.5; done
  return 1
}

binary_env() {
  export QSRV_ADDR="$BIND:$PORT" QSRV_DATA_DIR="$DATA" QSRV_API_KEYS_FILE="$PWD/$KEYS" \
    QSRV_METRICS_ADDR="$METRICS" QSRV_ADMIN="$ADMIN" QSRV_MIN_FREE_DISK="$MIN_FREE_DISK" \
    QSRV_BACKUP_DIR="$BACKUP_DIR" QSRV_BACKUP_EVERY="$BACKUP_EVERY" QSRV_BACKUP_KEEP="$BACKUP_KEEP" \
    QSRV_TLS_CERT="$TLS_CERT" QSRV_TLS_KEY="$TLS_KEY"
}

running_pid() {
  [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null && cat "$PIDFILE"
}

# docker run arguments for the configured container.
docker_args() {
  args=(-d --name "$CONTAINER" --restart unless-stopped
    -p "$BIND:$PORT:8080"
    -v "$PWD/$KEYS:/run/secrets/qsrv-keys:ro" -e QSRV_API_KEYS_FILE=/run/secrets/qsrv-keys
    -v "$DATA:/data"
    -e QSRV_ADMIN="$ADMIN" -e QSRV_MIN_FREE_DISK="$MIN_FREE_DISK" -e QSRV_METRICS_ADDR=":9464")
  if [ -n "$METRICS" ]; then args+=(-p "${METRICS%:*}:${METRICS##*:}:9464"); else args+=(-e QSRV_METRICS_ADDR=); fi
  if [ -n "$BACKUP_DIR" ]; then
    args+=(-v "$BACKUP_DIR:/backup" -e QSRV_BACKUP_DIR=/backup -e QSRV_BACKUP_EVERY="$BACKUP_EVERY" -e QSRV_BACKUP_KEEP="$BACKUP_KEEP")
  fi
  if [ -n "$TLS_CERT" ]; then
    args+=(-v "$TLS_CERT:/run/secrets/tls.crt:ro" -v "$TLS_KEY:/run/secrets/tls.key:ro"
      -e QSRV_TLS_CERT=/run/secrets/tls.crt -e QSRV_TLS_KEY=/run/secrets/tls.key)
  fi
  args+=(--stop-timeout 30 "$IMAGE")
}

backup_dir_writable_by_container() {
  [ -z "$BACKUP_DIR" ] && return 0
  mkdir -p "$BACKUP_DIR"
  [ "$(stat -c %u "$BACKUP_DIR" 2>/dev/null || stat -f %u "$BACKUP_DIR")" = 65532 ] && return 0
  note "The snapshot directory must belong to the container's user (uid 65532):"
  if chown 65532:65532 "$BACKUP_DIR" 2>/dev/null || sudo -n chown 65532:65532 "$BACKUP_DIR" 2>/dev/null; then
    note "chown 65532:65532 $BACKUP_DIR done."
  else
    note "run: sudo chown 65532:65532 $BACKUP_DIR   (then make up again)"
    return 1
  fi
}

up() {
  case "$MODE" in
  binary)
    if pid=$(running_pid); then note "qsrv is already running (pid $pid)."; status; return; fi
    [ -x bin/qsrv ] || die "bin/qsrv is missing: make build"
    binary_env
    mkdir -p "$DATA"
    nohup bin/qsrv >> "$LOG" 2>&1 &
    echo $! > "$PIDFILE"
    wait_healthy || { tail -20 "$LOG"; die "qsrv did not start (log: $LOG)"; }
    ;;
  docker)
    backup_dir_writable_by_container || exit 1
    docker volume create "$DATA" >/dev/null
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true # recreate with the current configuration; data is in the volume
    docker_args
    docker run "${args[@]}" >/dev/null
    wait_healthy || { docker logs --tail 20 "$CONTAINER"; die "qsrv did not start"; }
    ;;
  monitoring)
    "${COMPOSE[@]}" up -d
    wait_healthy || { "${COMPOSE[@]}" logs --tail 20 qsrv; die "qsrv did not start"; }
    ;;
  esac
  status
}

down() {
  case "$MODE" in
  binary)
    if pid=$(running_pid); then
      kill -TERM "$pid" # commits pending writes
      for _ in $(seq 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
      rm -f "$PIDFILE"
      note "qsrv stopped."
    else
      note "qsrv is not running."
    fi
    ;;
  docker)
    if docker stop -t 30 "$CONTAINER" >/dev/null 2>&1; then
      docker rm "$CONTAINER" >/dev/null
      note "qsrv stopped."
    else
      note "qsrv is not running."
    fi
    ;;
  monitoring) "${COMPOSE[@]}" down ;;
  esac
}

logs() {
  case "$MODE" in
  binary) tail -n 50 -f "$LOG" ;;
  docker) docker logs -n 50 -f "$CONTAINER" ;;
  monitoring) "${COMPOSE[@]}" logs -n 50 -f ;;
  esac
}

status() {
  if healthy; then printf '\033[32m  qsrv is up\033[0m (%s)\n' "$MODE"; else printf '\033[31m  qsrv is down\033[0m (%s)\n' "$MODE"; fi
  note "API:      $url"
  [ "$ADMIN" = true ] && note "Admin UI: $url/admin/"
  [ -n "$METRICS" ] && note "Metrics:  http://${METRICS/#:/localhost:}/metrics"
  [ "$MODE" = monitoring ] && note "Grafana:  http://localhost:3000 (admin / ${GRAFANA_PASSWORD})" && note "Prometheus http://localhost:9090, Alertmanager http://localhost:9093"
  note "Keys:     make keys"
}

keys() {
  note "tenant:name:key (a key only sees its tenant's queues; * = admin)"
  sed 's/^/  /' "$KEYS"
}

# add-key TENANT NAME: a new key, live (the running qsrv reloads its keys).
add_key() {
  local tenant=${1:-} name=${2:-}
  [[ "$tenant" =~ ^([a-z0-9][a-z0-9_-]{0,39}|\*)$ ]] || die "usage: make add-key TENANT=acme NAME=billing (TENANT='*' for an admin key)"
  [[ "$name" =~ ^[A-Za-z0-9_.-]{1,64}$ ]] || die "NAME: letters, digits, '.', '-' and '_'"
  cut -d: -f1,2 "$KEYS" | grep -qxF "$tenant:$name" && die "$tenant:$name already exists"
  key=$(openssl rand -hex 32)
  echo "$tenant:$name:$key" >> "$KEYS" # appended in place: same file for a running container
  case "$MODE" in
  binary) if pid=$(running_pid); then kill -HUP "$pid"; fi ;;
  docker) docker kill -s HUP "$CONTAINER" >/dev/null 2>&1 || true ;;
  monitoring)
    sed -i.bak "s|^QSRV_API_KEYS=.*|QSRV_API_KEYS=$(paste -sd, "$KEYS")|" deploy/monitoring/.env && rm -f deploy/monitoring/.env.bak
    "${COMPOSE[@]}" up -d qsrv >/dev/null # the key list is an env var there: recreate
    ;;
  esac
  note "Added $tenant:$name. Key:"
  echo "  $key"
}

case "${1:-}" in
up) up ;;
down) down ;;
restart) down; up ;;
logs) logs ;;
status) status ;;
keys) keys ;;
add-key) add_key "${2:-}" "${3:-}" ;;
run) # foreground, binary mode
  [ "$MODE" = binary ] || die "make run is for the binary mode; use make up"
  binary_env
  mkdir -p "$DATA"
  exec bin/qsrv
  ;;
*) die "usage: $0 up|down|restart|logs|status|keys|add-key TENANT NAME|run" ;;
esac
