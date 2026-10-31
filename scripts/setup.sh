#!/usr/bin/env bash
# Asks what qsrv needs to run and writes it to .qsrv/:
#   .qsrv/config.env  how to run it (mode, ports, data, backups, ...)
#   .qsrv/keys        API keys, tenant:name:key per line
# .qsrv/ is private (0700): the keys file inside is 0644 only so the Docker
# container's user can read it through a bind mount.
# Then `make up` starts it. Run again to change anything: existing keys are
# kept unless you say otherwise.
#
#   make setup                          # interactive
#   QSRV_SETUP_DEFAULTS=1 make setup    # accept every default (scripts, CI)
set -euo pipefail
cd "$(dirname "$0")/.."

DIR=.qsrv
CONFIG=$DIR/config.env
KEYS=$DIR/keys
DEFAULTS=${QSRV_SETUP_DEFAULTS:-}

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
note() { printf '  %s\n' "$*"; }
warn() { printf '\033[33m  ! %s\033[0m\n' "$*"; }
die()  { printf '\033[31msetup: %s\033[0m\n' "$*" >&2; exit 1; }

# ask VAR "Question" default: reads an answer (empty = default).
# Its locals have names no caller uses: printf -v writes the caller's variable.
ask() {
  local __var=$1 __q=$2 __def=${3:-} __in=""
  if [ -z "$DEFAULTS" ]; then
    if [ -n "$__def" ]; then printf '%s [%s]: ' "$__q" "$__def"; else printf '%s: ' "$__q"; fi
    read -r __in || true
  fi
  printf -v "$__var" '%s' "${__in:-$__def}"
}

# ask_valid VAR "Question" default REGEX "what is valid": asks until it matches.
ask_valid() {
  local __var=$1 __q=$2 __def=$3 __re=$4 __help=$5
  while :; do
    ask "$__var" "$__q" "$__def"
    [[ "${!__var}" =~ $__re ]] && return
    [ -n "$DEFAULTS" ] && die "invalid default for '$__q': ${!__var}"
    note "$__help"
  done
}

# ask_yn VAR "Question" y|n
ask_yn() {
  local var=$1 q=$2 def=$3 ans
  while :; do
    ask ans "$q (y/n)" "$def"
    case "$ans" in
      y|Y|yes) printf -v "$var" 'yes'; return ;;
      n|N|no) printf -v "$var" 'no'; return ;;
      *) note "Answer y or n." ;;
    esac
  done
}

# choose VAR "Question" default option...  (options as "value|description")
choose() {
  local var=$1 q=$2 def=$3 ans i opt; shift 3
  bold "$q"
  i=1
  for opt in "$@"; do
    printf '  %d) %-11s %s\n' "$i" "${opt%%|*}" "${opt#*|}"
    i=$((i + 1))
  done
  while :; do
    ask ans "Choose 1-$#" "$def"
    if [[ "$ans" =~ ^[0-9]+$ ]] && [ "$ans" -ge 1 ] && [ "$ans" -le $# ]; then
      opt=${*:$ans:1}
      printf -v "$var" '%s' "${opt%%|*}"
      return
    fi
    note "Answer a number from 1 to $#."
  done
}

newkey() {
  if command -v openssl >/dev/null; then openssl rand -hex 32
  else od -An -N32 -tx1 /dev/urandom | tr -d ' \n'; fi
}

valid_tenant() { [[ "$1" =~ ^[a-z0-9][a-z0-9_-]{0,39}$ ]]; }
valid_name() { [[ "$1" =~ ^[A-Za-z0-9_.-]{1,64}$ ]]; }

mkdir -p "$DIR"
chmod 700 "$DIR"
echo
bold "qsrv setup"
note "Answers go to $CONFIG and $KEYS. Press Enter to take the [default]."
if [ -f "$CONFIG" ]; then
  note "There is a configuration already: its values are the defaults now."
  # shellcheck disable=SC1090
  . "$CONFIG"
fi
echo

# --- how to run ---------------------------------------------------------------
case "${MODE:-}" in binary) d=1 ;; docker) d=2 ;; monitoring) d=3 ;; *) d=2 ;; esac
choose MODE "How do you want to run qsrv?" "$d" \
  "binary|built from this repository (needs Go 1.27+), runs on this machine" \
  "docker|one container (needs Docker)" \
  "monitoring|qsrv + Prometheus + Alertmanager + Grafana with Docker Compose"

case "$MODE" in
  binary)
    command -v go >/dev/null || die "Go is not installed: https://go.dev/dl (1.27 or later), or choose docker"
    ;;
  docker|monitoring)
    command -v docker >/dev/null || die "Docker is not installed: https://docs.docker.com/get-docker/, or choose binary"
    docker info >/dev/null 2>&1 || warn "Docker is installed but not running (or needs sudo): start it before make up."
    [ "$MODE" = monitoring ] && { docker compose version >/dev/null 2>&1 || die "docker compose is not available"; }
    case "${IMAGE:-}" in qsrv:local) d=2 ;; *) d=1 ;; esac
    choose IMAGE_CHOICE "Which image?" "$d" \
      "published|ghcr.io/romycode/qsrv:latest" \
      "local|built from this repository (make image)"
    if [ "$IMAGE_CHOICE" = local ]; then IMAGE=qsrv:local; else IMAGE=ghcr.io/romycode/qsrv:latest; fi
    ;;
esac
echo

# --- network -------------------------------------------------------------------
bold "Network"
if [ "$MODE" = monitoring ]; then
  PORT=8080
  note "The API listens on port 8080 (deploy/monitoring/docker-compose.yml)."
else
  ask_valid PORT "API port" "${PORT:-8080}" '^[0-9]{1,5}$' "A port number, e.g. 8080."
fi
case "${BIND:-}" in 127.0.0.1) d=n ;; *) d=y ;; esac
if [ "$MODE" = monitoring ]; then
  BIND=0.0.0.0
else
  ask_yn PUBLIC "Reachable from other machines? (n = only from this one)" "$d"
  if [ "$PUBLIC" = yes ]; then BIND=0.0.0.0; else BIND=127.0.0.1; fi
fi
TLS_CERT=${TLS_CERT:-}; TLS_KEY=${TLS_KEY:-}
if [ "$BIND" = 0.0.0.0 ] && [ "$MODE" != monitoring ]; then
  if [ -n "$TLS_CERT" ]; then d=y; else d=n; fi
  ask_yn USE_TLS "Serve HTTPS with your own certificate? (n = plain HTTP, put TLS in a proxy in front)" "$d"
  if [ "$USE_TLS" = yes ]; then
    ask TLS_CERT "Certificate file (PEM)" "$TLS_CERT"
    ask TLS_KEY "Private key file (PEM)" "$TLS_KEY"
    [ -f "$TLS_CERT" ] && [ -f "$TLS_KEY" ] || die "certificate or key not found"
    TLS_CERT=$(cd "$(dirname "$TLS_CERT")" && pwd)/$(basename "$TLS_CERT")
    TLS_KEY=$(cd "$(dirname "$TLS_KEY")" && pwd)/$(basename "$TLS_KEY")
  else
    TLS_CERT=""; TLS_KEY=""
    warn "Plain HTTP: API keys travel in clear. Use a TLS proxy for anything outside a private network."
  fi
fi
if [ "$MODE" != monitoring ]; then
  ask METRICS "Prometheus metrics address (no auth: keep it private; empty = off)" "${METRICS-127.0.0.1:9464}"
else
  METRICS=":9464"
fi
if [ "${ADMIN:-true}" = true ]; then d=y; else d=n; fi
ask_yn ADMIN_ON "Admin web UI at /admin/ (read-only, sign in with an API key)" "$d"
if [ "$ADMIN_ON" = yes ]; then ADMIN=true; else ADMIN=false; fi
echo

# --- storage ---------------------------------------------------------------------
bold "Storage"
if [ "$MODE" = binary ]; then
  ask DATA "Data directory" "${DATA:-./data}"
  ask_yn BACKUPS "Take snapshots (backups)?" "y"
  if [ "$BACKUPS" = yes ]; then
    ask BACKUP_DIR "Snapshot directory (best on another disk)" "${BACKUP_DIR:-./backups}"
  else
    BACKUP_DIR=""
    warn "No snapshots: losing the disk loses the messages."
  fi
else
  DATA=qsrv-data
  note "Data in the Docker volume qsrv-data."
  if [ "$MODE" = monitoring ]; then
    BACKUP_DIR=volume
    note "Snapshots in the Docker volume qsrv-backup."
  else
    ask_yn BACKUPS "Take snapshots (backups)?" "y"
    if [ "$BACKUPS" = yes ]; then
      ask BACKUP_DIR "Snapshot directory on this machine (best on another disk)" "${BACKUP_DIR:-$PWD/backups}"
      mkdir -p "$BACKUP_DIR"
      BACKUP_DIR=$(cd "$BACKUP_DIR" && pwd)
      note "It must be writable by the container's user (uid 65532); make up fixes that with sudo if needed."
    else
      BACKUP_DIR=""
      warn "No snapshots: losing the disk loses the messages."
    fi
  fi
fi
BACKUP_EVERY=${BACKUP_EVERY:-1h}; BACKUP_KEEP=${BACKUP_KEEP:-24}
if [ -n "$BACKUP_DIR" ]; then
  ask_valid BACKUP_EVERY "Snapshot every (how much you accept to lose)" "$BACKUP_EVERY" '^([0-9]+(h|m|s))+$' "A duration like 1h, 30m or 1h30m."
  ask_valid BACKUP_KEEP "Snapshots to keep" "$BACKUP_KEEP" '^[1-9][0-9]*$' "A number, e.g. 24."
fi
ask_valid MIN_FREE_DISK "Refuse new messages below this free disk space (0 = never)" "${MIN_FREE_DISK:-1GiB}" '^([0-9]+(\.[0-9]+)?(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)?)$' "A size like 500MiB or 2GiB, or 0."
echo

# --- API keys --------------------------------------------------------------------
bold "API keys"
note "Every key belongs to a tenant: it only sees that tenant's queues. An admin key sees all of them."
keep=no
if [ -s "$KEYS" ]; then
  note "$(wc -l < "$KEYS" | tr -d ' ') key(s) in $KEYS: $(cut -d: -f1,2 "$KEYS" | tr '\n' ' ')"
  ask_yn keep "Keep them?" "y"
fi
if [ "$keep" = no ]; then
  : > "$KEYS.new"
  ask TENANTS "Tenants, comma-separated (e.g. your apps or teams)" "${TENANTS:-default}"
  for t in ${TENANTS//,/ }; do
    valid_tenant "$t" || die "invalid tenant name '$t': lowercase letters, digits, - and _, up to 40"
    kname=""
    ask kname "Name of the key for tenant $t" "app"
    valid_name "$kname" || die "invalid key name '$kname'"
    echo "$t:$kname:$(newkey)" >> "$KEYS.new"
  done
  ask_yn WANT_ADMIN "Also an admin key (sees every tenant; for operators, not applications)?" "y"
  [ "$WANT_ADMIN" = yes ] && echo "*:ops:$(newkey)" >> "$KEYS.new"
  cat "$KEYS.new" > "$KEYS" # in place: a running container keeps seeing the same file
  rm "$KEYS.new"
  NEW_KEYS=yes
fi
chmod 644 "$KEYS"
echo

if [ "$MODE" = monitoring ]; then
  bold "Grafana"
  ask GRAFANA_PASSWORD "Password for Grafana's admin user" "${GRAFANA_PASSWORD:-$(newkey | cut -c1-16)}"
  echo
fi

# --- write -------------------------------------------------------------------------
cat > "$CONFIG" <<EOF
# Written by scripts/setup.sh ($(date -u +%Y-%m-%dT%H:%M:%SZ)). Run make setup to change it.
MODE=$MODE
IMAGE=${IMAGE:-}
BIND=$BIND
PORT=$PORT
TLS_CERT=$TLS_CERT
TLS_KEY=$TLS_KEY
METRICS=$METRICS
ADMIN=$ADMIN
DATA=$DATA
BACKUP_DIR=$BACKUP_DIR
BACKUP_EVERY=$BACKUP_EVERY
BACKUP_KEEP=$BACKUP_KEEP
MIN_FREE_DISK=$MIN_FREE_DISK
TENANTS=${TENANTS:-$(cut -d: -f1 "$KEYS" | grep -v '^\*$' | paste -sd, -)}
GRAFANA_PASSWORD=${GRAFANA_PASSWORD:-}
EOF
chmod 600 "$CONFIG"

if [ "$MODE" = monitoring ]; then
  {
    echo "# Written by scripts/setup.sh. Holds the API keys: keep it private."
    echo "QSRV_API_KEYS=$(paste -sd, "$KEYS")"
    echo "QSRV_IMAGE=$IMAGE"
    echo "QSRV_BACKUP_EVERY=$BACKUP_EVERY"
    echo "QSRV_BACKUP_KEEP=$BACKUP_KEEP"
    echo "QSRV_MIN_FREE_DISK=$MIN_FREE_DISK"
    echo "QSRV_ADMIN=$ADMIN"
    echo "GRAFANA_ADMIN_PASSWORD=$GRAFANA_PASSWORD"
  } > deploy/monitoring/.env
  chmod 600 deploy/monitoring/.env
fi

bold "Done"
note "Mode: $MODE$([ -n "${IMAGE:-}" ] && echo " ($IMAGE)")"
scheme=http; [ -n "$TLS_CERT" ] && scheme=https
host=localhost
note "API: $scheme://$host:$PORT  (listening on $BIND)"
[ "$ADMIN" = true ] && note "Admin UI: $scheme://$host:$PORT/admin/"
[ -n "$METRICS" ] && note "Metrics: http://${METRICS/#:/localhost:}/metrics"
[ "$MODE" = monitoring ] && note "Grafana: http://localhost:3000 (admin / the password above)"
note "Snapshots: ${BACKUP_DIR:-off}$([ -n "$BACKUP_DIR" ] && echo ", every $BACKUP_EVERY, $BACKUP_KEEP kept")"
if [ "${NEW_KEYS:-}" = yes ]; then
  echo
  bold "Your API keys (also in $KEYS; give each application its tenant's key):"
  sed 's/^/  /' "$KEYS"
fi
echo
note "Next: make up      (make help lists everything)"
