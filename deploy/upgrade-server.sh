#!/usr/bin/env bash
# Upgrades a running fedarisha server to a published release of this fork.
#
# Why this exists: the server that ships in the Android client's deploy flow
# drops a file after `uploadAttempts`. The peer reads strictly in sequence and
# cannot skip a missing object, so one lost file wedges the session for
# minutes. The releases this script installs retry until the backend takes the
# file, and fix a further dozen defects found by audit.
#
# What it does: downloads a prebuilt release asset, verifies its published
# SHA-256, proves the binary is the build we think it is, test-runs the LIVE
# config with it, backs up the running binary, swaps atomically, restarts,
# watches the service settle, and rolls back if anything is off.
#
# What it never touches: the config, the credentials, the bucket. Not once.
#
# Usage:
#   sudo ./deploy/upgrade-server.sh
#   sudo ./deploy/upgrade-server.sh --version v0.1.0-fork.4
#   sudo ./deploy/upgrade-server.sh --version v0.1.0-fork.4 --dry-run
#   ./deploy/upgrade-server.sh --status
#
# Exits 0 only when the new binary is installed and the service is healthy.
# On any failure it rolls back first, then exits non-zero.

set -euo pipefail

REPO_DEFAULT="https://github.com/thevladimirzh/s3-bypass-protocol"
VERSION_DEFAULT="v0.1.0-fork.4"

# Markers that must be present in the installed binary. The first proves it is
# our fork at all; the rest are strings this release added and older ones do not
# have, so they distinguish fork.4 from fork.3 rather than merely "a fork".
# These are log messages the server prints at runtime, which is why checking
# them also tells us what to grep for when verifying the upgrade by hand.
REQUIRED_MARKERS=(
  "upload retry"
  "read list failed (retrying)"
  "delete queue full"
)

# Where a live server binary may live, and the unit that runs it.
BINARY_CANDIDATES=(
  /usr/local/bin/xray-fedarisha
  /opt/s3bypass-protocol/xray
  /usr/local/bin/xray
  /usr/bin/xray
)
SERVICE_CANDIDATES=(xray-fedarisha s3bypass-protocol xray)
# Where its config may live. Only read — see the header.
CONFIG_CANDIDATES=(
  /usr/local/etc/xray-fedarisha/config.json
  /etc/s3bypass-protocol/server.json
  /etc/xray/config.json
)

REPO="$REPO_DEFAULT"
VERSION="$VERSION_DEFAULT"
ASSET="Xray-linux-64.zip"
DRY_RUN=0
STATUS_ONLY=0
BINARY_OVERRIDE=""
SERVICE_OVERRIDE=""
CONFIG_OVERRIDE=""
SHA_OVERRIDE=""
KEEP_BACKUPS=5
SETTLE_SECONDS=10

log()  { printf '\033[1m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

usage() { awk 'NR>1 && /^#/ {print} NR>1 && !/^#/ {exit}' "$0"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --version)  VERSION="${2:?--version needs a tag}"; shift 2 ;;
    --repo)     REPO="${2:?--repo needs a URL}"; shift 2 ;;
    --asset)    ASSET="${2:?--asset needs a file name}"; shift 2 ;;
    --binary)   BINARY_OVERRIDE="${2:?--binary needs a path}"; shift 2 ;;
    --service)  SERVICE_OVERRIDE="${2:?--service needs a unit name}"; shift 2 ;;
    --config)   CONFIG_OVERRIDE="${2:?--config needs a path}"; shift 2 ;;
    --sha256)   SHA_OVERRIDE="${2:?--sha256 needs a digest}"; shift 2 ;;
    --keep)     KEEP_BACKUPS="${2:?--keep needs a number}"; shift 2 ;;
    --settle)   SETTLE_SECONDS="${2:?--settle needs seconds}"; shift 2 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    --status)   STATUS_ONLY=1; shift ;;
    -h|--help)  usage; exit 0 ;;
    *)          die "unknown argument: $1" ;;
  esac
done

command -v systemctl >/dev/null || die "systemctl not found; this script upgrades a systemd service"

first_existing() {
  for candidate in "$@"; do
    if [ -e "$candidate" ]; then printf '%s' "$candidate"; return 0; fi
  done
  return 1
}

# marker_present greps for a literal marker without letting SIGPIPE decide the
# answer. `strings "$bin" | grep -q` looks right and is not: grep -q exits at the
# first hit, strings dies of SIGPIPE on the way out, and under `set -o pipefail`
# that non-zero status is reported as "marker absent". grep -c reads all the
# input, so it cannot lose the race — and it gives a count, not just a verdict.
marker_present() {
  local bin="$1" marker="$2"
  [ "$(strings "$bin" | grep -cF -- "$marker" || true)" -gt 0 ]
}

# --- locate the installation --------------------------------------------------

BINARY="${BINARY_OVERRIDE:-$(first_existing "${BINARY_CANDIDATES[@]}" || true)}"
[ -n "$BINARY" ] || die "no server binary found; pass --binary /path/to/xray"

SERVICE=""
for candidate in ${SERVICE_OVERRIDE:+"$SERVICE_OVERRIDE"}; do
  if systemctl cat "$candidate" >/dev/null 2>&1; then SERVICE="$candidate"; break; fi
done
if [ -z "$SERVICE" ]; then
  for candidate in "${SERVICE_CANDIDATES[@]}"; do
    if systemctl cat "$candidate" >/dev/null 2>&1; then SERVICE="$candidate"; break; fi
  done
fi
[ -n "$SERVICE" ] || die "no systemd unit found; pass --service <unit>"

CONFIG="${CONFIG_OVERRIDE:-$(first_existing "${CONFIG_CANDIDATES[@]}" || true)}"

# show_config is a function rather than a `[ -n "$CONFIG" ] && log ...` line:
# under `set -e`, an `&&` list that short-circuits returns non-zero and takes
# the whole script down with it. The no-config case is normal, not an error.
show_config() {
  if [ -n "$CONFIG" ]; then log "config: $CONFIG (read only, never modified)"; fi
}

report_version() {
  "$1" version 2>/dev/null | head -1 || echo "unreadable"
}

RUNNING_VERSION="$(report_version "$BINARY")"
RUNNING_DIGEST="$(sha256sum "$BINARY" | awk '{print $1}')"

# --- status only --------------------------------------------------------------

if [ "$STATUS_ONLY" -eq 1 ]; then
  log "binary : $BINARY"
  log "version: $RUNNING_VERSION"
  log "digest : $RUNNING_DIGEST"
  log "unit   : $SERVICE ($(systemctl is-active "$SERVICE" 2>/dev/null || echo unknown))"
  show_config
  missing=0
  for marker in "${REQUIRED_MARKERS[@]}"; do
    if marker_present "$BINARY" "$marker"; then
      log "marker : present — $marker"
    else
      warn "marker : MISSING — $marker"
      missing=1
    fi
  done
  [ "$missing" -eq 0 ] || warn "this binary predates some fixes in this release"
  exit 0
fi

[ "$(id -u)" -eq 0 ] || die "run as root (it replaces a system binary and restarts a service)"
for tool in curl unzip strings sha256sum awk; do
  command -v "$tool" >/dev/null || die "$tool is required"
done

ZIP_URL="${REPO}/releases/download/${VERSION}/${ASSET}"
DGST_URL="${ZIP_URL}.dgst"

log "current binary : $BINARY"
log "current version: $RUNNING_VERSION"
log "current unit   : $SERVICE"
show_config

if [ "$DRY_RUN" -eq 1 ]; then
  log "dry run — would install $VERSION from"
  log "  $ZIP_URL"
  if [ -n "$CONFIG" ]; then
    log "  after test-running it against $CONFIG"
  else
    log "  (no config found — the preflight would be skipped)"
  fi
  exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# --- fetch and verify ---------------------------------------------------------

log "downloading ${VERSION} / ${ASSET}"
curl -fsSL "$ZIP_URL" -o "$WORK/core.zip" || die "cannot download $ZIP_URL"
curl -fsSL "$DGST_URL" -o "$WORK/core.dgst" || die "cannot download $DGST_URL — refusing an unverifiable binary"

ACTUAL_SHA="$(sha256sum "$WORK/core.zip" | awk '{print $1}')"
EXPECTED_SHA=""
if [ -n "$SHA_OVERRIDE" ]; then
  EXPECTED_SHA="$SHA_OVERRIDE"
else
  EXPECTED_SHA="$(awk -F'[:=]' '/^SHA2-256=/ {gsub(/[[:space:]]/, "", $2); print $2}' "$WORK/core.dgst")"
fi
[ -n "$EXPECTED_SHA" ] || die "no SHA2-256 in ${ASSET}.dgst and no --sha256 given; refusing an unverifiable binary"
if [ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]; then
  die "SHA-256 mismatch for ${ASSET}
  expected $EXPECTED_SHA
  actual   $ACTUAL_SHA"
fi
log "digest verified"

unzip -qo "$WORK/core.zip" -d "$WORK/unpacked"
# -quit rather than `| head -1`: under pipefail, head closing the pipe early
# can hand find a SIGPIPE and fail a search that actually succeeded.
NEW_BINARY="$(find "$WORK/unpacked" -type f -name xray -print -quit)"
[ -n "$NEW_BINARY" ] || die "no xray binary inside ${ASSET}"
chmod +x "$NEW_BINARY"

# The marker check is what stops a wrong build from installing and looking like
# a success. An upstream binary passes every other step: it downloads, its
# digest is published, and it starts cleanly.
found=0
for marker in "${REQUIRED_MARKERS[@]}"; do
  if marker_present "$NEW_BINARY" "$marker"; then
    found=$((found + 1))
  else
    die "downloaded binary has no \"$marker\" marker — this is not the expected build, aborting before the running binary is touched"
  fi
done
log "build verified (${found}/${#REQUIRED_MARKERS[@]} markers present)"

# --- preflight: will the new binary accept the config we are actually running? --

if [ -n "$CONFIG" ] && [ -r "$CONFIG" ]; then
  log "test-running the new binary against the live config"
  if "$NEW_BINARY" run -test -c "$CONFIG" >"$WORK/configtest.log" 2>&1; then
    log "config accepted"
  else
    warn "the new binary rejected the live config:"
    sed -n '1,20p' "$WORK/configtest.log" >&2
    die "aborting before the swap; the running binary is untouched and still serving"
  fi
else
  warn "no readable config found — skipping the config preflight"
fi

# --- swap ---------------------------------------------------------------------

BACKUP="${BINARY}.bak.$(date +%Y%m%d%H%M%S)"
log "backing up to $BACKUP"
cp -a "$BINARY" "$BACKUP"

# Write beside the target and rename over it. rename(2) is atomic within a
# filesystem, so the unit can never restart onto a half-written binary.
log "installing $VERSION"
STAGED="${BINARY}.new.$$"
trap 'rm -rf "$WORK" "$STAGED"' EXIT
install -m 0755 "$NEW_BINARY" "$STAGED"
mv -f "$STAGED" "$BINARY"

rollback() {
  warn "rolling back to $BACKUP"
  cp -a "$BACKUP" "$BINARY"
  systemctl restart "$SERVICE" || warn "rollback restart failed — check the unit by hand"
}

log "restarting ${SERVICE}.service"
if ! systemctl restart "$SERVICE"; then
  rollback
  die "restart failed; the previous binary is back in place"
fi

# A service that starts and then dies is not healthy, and `systemctl restart`
# reports success for that. So watch it stay up.
log "watching it settle for ${SETTLE_SECONDS}s"
settled=1
i=0
while [ "$i" -lt "$SETTLE_SECONDS" ]; do
  if ! systemctl is-active --quiet "$SERVICE"; then
    settled=0
    break
  fi
  sleep 1
  i=$((i + 1))
done

if [ "$settled" -ne 1 ]; then
  warn "service did not stay up for ${SETTLE_SECONDS}s"
  systemctl --no-pager --lines=30 status "$SERVICE" >&2 || true
  journalctl -u "$SERVICE" -n 30 --no-pager >&2 || true
  rollback
  die "rolled back; inspect: journalctl -u ${SERVICE} -n 50 --no-pager"
fi

NEW_VERSION="$(report_version "$BINARY")"
NEW_DIGEST="$(sha256sum "$BINARY" | awk '{print $1}')"
if [ "$NEW_DIGEST" = "$RUNNING_DIGEST" ]; then
  warn "the installed binary is byte-identical to the one that was running — nothing changed"
fi

log "pruning old backups, keeping $KEEP_BACKUPS"
# `|| true` because with no backups yet ls fails, and pipefail would otherwise
# turn "nothing to prune" into a failed upgrade.
OLD_BACKUPS="$(ls -1t "${BINARY}".bak.* 2>/dev/null | tail -n "+$((KEEP_BACKUPS + 1))" || true)"
if [ -n "$OLD_BACKUPS" ]; then
  printf '%s\n' "$OLD_BACKUPS" | while read -r old; do rm -f "$old"; done
fi

log "done: $RUNNING_VERSION → $NEW_VERSION"
cat <<EOF

  Installed $VERSION over ${RUNNING_DIGEST:0:12}
  Backup kept at: ${BACKUP}

  Roll back by hand at any time:
    systemctl stop ${SERVICE} && cp -a ${BACKUP} ${BINARY} && systemctl start ${SERVICE}

  Confirm it under real traffic before calling it good — see
  deploy/UPGRADE-SERVER.ru.md. The symptom this release targets is a 15-29s
  stall on the first request after connecting.

  Watch for these, which only exist in the new code:
    journalctl -u ${SERVICE} -f | grep -E 'read list failed|delete queue full|cleanup:'
EOF