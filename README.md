# s3-bypass-protocol

Our hardened build of the **fedarisha** tunnel engine — the transport behind
[S3 Bypass Desktop](https://github.com/thevladimirzh/s3-bypass-desktop).

Traffic is exchanged through an S3-compatible bucket: the client writes data as
objects and reads the peer's objects back, while a small coordination server
accepts sessions. That is why the tunnel survives censorship of a single host —
blocking it means blocking object storage itself.

## Why this fork exists

Under a parallel-download burst the upstream client froze all traffic for about
four minutes while the core process stayed alive and reported "connected". The
log evidence identified the cause:

**the writer abandoned a file after `uploadAttempts`, and the peer reads strictly
in sequence with no way to skip a missing object — so one lost file wedged the
session permanently.** The client could only fall over and re-dial; three slow
handshakes at 60 seconds each made the outage last.

This fork fixes the writer, and a client that fails over fast when a peer really
is broken.

| Area | Upstream | Here |
| --- | --- | --- |
| **Dropped files** | abandoned after 3 attempts | `uploadUntilDelivered` retries until delivered, or the session ends |
| Upload retry pacing | hot loop — over a million PUTs in 600 ms measured against a failing backend | the error is returned and the caller spaces the retries |
| Read concurrency | 48 per file, so up to 96 requests on the wire with hedging | 16, enforced per request |
| Per-GET budget | fixed `1200ms` | adaptive `1.2s → 4s`, stretches on consecutive timeouts, decays on success |
| Hole watchdog | `7s` — below the upload budget, so it tore sessions down mid-PUT | `25s` |
| Session handshake wait | flat `60s` | `5s / 10s / 20s / 30s` backoff |
| Handshake hello/ACK | written once each | retried |
| GET failures | swallowed, only a counter | logged with the underlying error |
| `Close()` counters | data race against the poll loop | atomic snapshots |

### Measured

Our server and our client, against a real bucket on an isolated prefix: six
parallel 50 MB downloads, two rounds, **600 MB, every byte delivered, zero
holes, zero teardowns, zero failed handshakes, one session for the entire run**
(~10 MB/s aggregate).

The same test against the upstream server stalls within ~9 seconds and needs
~4 minutes to recover.

**The wire protocol is untouched** — session layout, file naming and encryption
are unchanged, so this core interoperates with the upstream server on both
sides.

## Releases

Assets use the upstream release names (`Xray-linux-64.zip`, `Xray-macos-64.zip`,
`Xray-macos-arm64-v8a.zip`), so a consumer only has to change the download URL.
Latest: [v0.1.0-fork.2](https://github.com/thevladimirzh/s3-bypass-protocol/releases).

## Running our own server

The client fix alone recovers faster; the stall is gone once we run both ends.
See [`deploy/README.md`](deploy/README.md) for requirements, the multi-user
discovery rule, config shape and what to watch in the journal. The install kit
lives in [`deploy/`](deploy/).

## Building and testing

```sh
go build ./...
go vet ./proxy/fedarisha/...
go run ./infra/vformat/main.go -mode check
go test ./proxy/fedarisha/...
go test -race ./proxy/fedarisha/transport/...
```

Our CI gate is `.github/workflows/fork-ci.yml`: format, vet, race tests and
cross-builds for darwin and linux on x64 and arm64.

Specs for this work:

- `proxy/fedarisha/transport/robustness_spec_test.go` — read-path behaviour
- `proxy/fedarisha/transport/delivery_spec_test.go` — the delivery guarantee
- `proxy/fedarisha/transport/endtoend_spec_test.go` — client+server stand

The stand runs both protocol ends in one process against a temp directory, so a
stall can be reproduced and fixed with no server or credentials. Config helpers
live in [`scripts/`](scripts/).

## Licence and attribution

MPL-2.0, inherited unchanged — see [`LICENSE`](LICENSE). MPL-2.0 permits
forking and redistribution provided the licence and upstream notices are kept,
which we do.

- Forked from [`Fedarisha/Xray-core-fedarisha`](https://github.com/Fedarisha/Xray-core-fedarisha),
  based on its release `26.9.9-1.0.1fed` (commit `03660664`) — the same base the
  desktop client pins, so the engine version and log wording match the pinned
  build.
- Which is itself a fork of [`XTLS/Xray-core`](https://github.com/XTLS/Xray-core).

### Bundled third-party components

Some optional features dynamically load third-party components that ship in the
release ZIP for convenience. They are separate works under their own licences
and may be replaced by the user under those licences.

- **Wintun** (Windows tunnel driver) — redistributed unmodified under its own
  licence, which is included as `LICENSE-Wintun` in the Windows assets.
- **Geo databases** (`geoip.dat`, `geosite.dat`) — vendored in `resources/`,
  published by [Loyalsoldier/v2ray-rules-dat](https://github.com/Loyalsoldier/v2ray-rules-dat).
  Sources, digests and refresh commands: [`docs/analysis/geodata-assets.md`](docs/analysis/geodata-assets.md).

## Documented behaviour we did not change

Two points worth knowing before operating this core:

- **A slow read looks like a slow read, not a missing file.** The per-GET budget
  adapts; the hole watchdog only fires on a genuinely wedged peer.
- **A file the writer cannot deliver is never dropped.** It is retried until the
  session ends. `upload retry` lines in the journal are the guarantee working,
  not an error.