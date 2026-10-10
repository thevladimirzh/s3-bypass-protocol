#!/usr/bin/env bash
# Upgrades a running fedarisha server to our hardened build.
#
# Why: an existing server — deployed from upstream main by the Android client's
# deploy flow, or by hand — drops a file after `uploadAttempts`. The peer reads
# strictly in sequence and cannot skip a missing object, so one lost file wedges
# the session for minutes. Our build retries until the backend takes the file.
#
# What it does: builds our fork at a pinned tag, refuses to install unless the
# result is provably our build, keeps a backup of the running binary, swaps it,
# restarts, and rolls back if the service does not come back.
#
# What it never touches: the config. Credentials and bucket paths are yours.
#
# Usage:
#   sudo ./deploy/upgrade-server.sh
#   sudo ./deploy/upgrade-server.sh --version v0.1.0-fork.3 --dry-run
#
# Exits 0 on a healthy restart, non-zero otherwise (with the rollback done).

set -euo pipefail

REPO_DEFAULT="https://github.com/thevladimirzh/s3-bypass-protocol"
VERSION_DEFAULT="v0.1.0-fork.3"

REPO="$REPO_DEFAULT"
VERSION="$VERSION_DEFAULT"
DRY_RUN=0
BINARY_OVERRIDE=""
SERVICE_OVERRIDE=""
BUILD_JOBS="$(nproc 2>/dev/null || echo 2)"

log()  { printf '\033[1m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --version)  VERSION="${2:?--version needs a tag}"; shift 2 ;;
    --repo)     REPO="${2:?--repo needs a URL}"; shift 2 ;;
    --jobs)     BUILD_JOBS="${2:?--jobs needs a number}"; shift 2 ;;
    --binary)   BINARY_OVERRIDE="${2:?--binary needs a path}"; shift 2 ;;
    --service)  SERVICE_OVERRIDE="${2:?--service needs a unit name}"; shift 2 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    -h|--help)  sed -n '2,26p' "$0"; exit 0 ;;
    *)          die "unknown argument: $1" ;;
  esac
done

# --- locate the installation --------------------------------------------------
# Two layouts are in the wild: the Android deploy flow (xray-fedarisha under
# /usr/local) and this repo's own kit (s3bypass-protocol under /opt).
BINARY="$BINARY_OVERRIDE"
SERVICE="$SERVICE_OVERRIDE"
if [ -z "$BINARY" ]; then
  for candidate in /usr/local/bin/xray-fedarisha /opt/s3bypass-protocol/xray /usr/local/bin/xray; do
    if [ -x "$candidate" ]; then BINARY="$candidate"; break; fi
  done
fi
[ -n "$BINARY" ] || die "no running core found; pass --binary /path/to/xray"

if [ -z "$SERVICE" ]; then
  for candidate in xray-fedarisha s3bypass-protocol xray; do
    if systemctl list-unit-files --no-legend 2>/dev/null | awk '{print $1}' | grep -q "^${candidate}\.service$"; then
      SERVICE="$candidate"; break
    fi
  done
fi

# Root is only needed for the actual swap: a dry run must stay safe to run as a
# normal user, because it is the check people run before committing to it.
if [ "$DRY_RUN" -eq 0 ]; then
  [ "$(id -u)" -eq 0 ] || die "run as root (it replaces a system binary and restarts a service)"
fi

RUNNING_VERSION="$("$BINARY" version 2>/dev/null | head -1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9A-Za-z.\-]+' || echo unknown)"
log "current binary : $BINARY ($RUNNING_VERSION)"
log "current service: $SERVICE"

command -v go >/dev/null || die "go is required to build; install it first (Go 1.23+)"
log "go: $(go version | awk '{print $3}')"

if [ "$DRY_RUN" -eq 1 ]; then
  log "dry run — would build $REPO at $VERSION and replace $BINARY"
  exit 0
fi

# --- build --------------------------------------------------------------------
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT

log "cloning $REPO at $VERSION"
git clone --quiet --depth 1 --branch "$VERSION" "$REPO" "${BUILD_DIR}/src"

log "building (this takes a minute or two)"
( cd "${BUILD_DIR}/src/main" && go build -o "${BUILD_DIR}/xray" -trimpath -ldflags "-s -w" . )

# The marker matters: installing a build that is not ours would look successful
# and change nothing. "upload retry" only exists in our fork.
MARKERS="$(strings "${BUILD_DIR}/xray" | grep -c 'upload retry' || true)"
[ "${MARKERS:-0}" -gt 0 ] || die "build has no delivery-retry marker — this is not our fork build, aborting before touching the running binary"
log "build verified (delivery-retry marker present)"

# --- swap ---------------------------------------------------------------------
BACKUP="${BINARY}.bak.$(date +%Y%m%d%H%M%S)"
log "backing up current binary to $BACKUP"
cp -a "$BINARY" "$BACKUP"

log "installing $VERSION"
install -m 0755 "${BUILD_DIR}/xray" "$BINARY"

log "restarting ${SERVICE}.service"
if ! systemctl restart "$SERVICE"; then
  warn "restart failed — rolling back"
  cp -a "$BACKUP" "$BINARY"
  systemctl restart "$SERVICE" || true
  die "rolled back; the previous binary is back in place"
fi

sleep 2
if ! systemctl is-active --quiet "$SERVICE"; then
  warn "service is not active — rolling back"
  cp -a "$BACKUP" "$BINARY"
  systemctl restart "$SERVICE" || true
  die "rolled back; check: journalctl -u ${SERVICE} -n 50 --no-pager"
fi

NEW_VERSION="$("$BINARY" version 2>/dev/null | head -1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9A-Za-z.\-]+' || echo unknown)"
log "done: $RUNNING_VERSION → $NEW_VERSION"
cat <<EOF

  Verify under load before calling it good: six parallel 50 MB downloads
  should complete without stalling. If a session does stall, recovery is now
  ~13 s instead of ~4 minutes.

  Rollback at any time:
    cp -a ${BACKUP} ${BINARY} && systemctl restart ${SERVICE}
EOF