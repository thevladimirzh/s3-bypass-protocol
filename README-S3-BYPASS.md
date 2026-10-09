# s3-bypass-protocol

Our hardened fork of the **fedarisha** tunnel engine that powers
[S3 Bypass Desktop](https://github.com/thevladimirzh/s3-bypass-desktop).

This repository exists because the read path of the upstream client was not
safe under sustained network load: it provokes the stall it then blames on
object storage.

## Upstream and licence

- Upstream: [`Fedarisha/Xray-core-fedarisha`](https://github.com/Fedarisha/Xray-core-fedarisha),
  itself a fork of [`XTLS/Xray-core`](https://github.com/XTLS/Xray-core).
- Licence: **MPL-2.0**, inherited unchanged — see [`LICENSE`](LICENSE). MPL-2.0
  permits forking and redistribution; the licence text and the upstream
  copyright notices are preserved as required.
- Fork base at the time of writing: upstream `3a40518` (release `26.9.9-1.0.1fed`).

## What we changed and why

Observed on the maintainer's machine (2026-10-09): six parallel 50 MB downloads
through the local SOCKS froze all traffic for ~4 minutes while the core process
stayed alive and healthy. The session teardown, not the producer, was the
outage.

| Area | Upstream | Here | Reason |
| --- | --- | --- | --- |
| Per-GET budget | fixed `1200ms` | adaptive `1.2s → 4s`, +400ms per consecutive timeout, decays on success | Under tail latency a slow read was misread as a missing file |
| Read concurrency | `48` per file — up to `96` on the wire with hedging | `16`, enforced **per request** | The client was the burst; the hedge doubled the old per-file limit |
| Hole watchdog | `7s` — below the upload budget (`8s` timeout, `3s` large hedge) | `25s` | It tore sessions down while a legitimate PUT was still in flight |
| ACK wait | flat `60s` poll | `5s / 10s / 20s / 30s` backoff | A flat wait cost a minute of dead time per failed handshake; three of them were the 4-minute outage |
| GET error reporting | swallowed, only a counter | logged with the underlying error | Diagnosis previously required forking the core |
| `Close()` counters | data race against the poll loop | atomic snapshots | Reported by `-race`; the log line raced with the reader |
| Upload retry | hot loop: an immediate re-fire when every attempt failed | the error is returned and the caller spaces retries | Measured at over a million PUTs in 600 ms against a store rejecting everything |
| **Dropped files** | `uploadAttempts` tries, then the file was abandoned | `uploadUntilDelivered` retries until the backend takes it or the session ends | **Root cause of the stall**: the peer reads strictly in order, so one dropped object is a permanent hole (`hole at seq 12 … 120 present`) |
| Handshake writes | hello and ACK written once each | retried with the same pacing | One rejected request killed an otherwise healthy session |

**The wire protocol is untouched.** Session layout, file naming and encryption
are unchanged, so this core still interoperates with the upstream server on
both sides.

## Why the delivery guarantee matters

A writer that gives up on a file is not merely late. The peer consumes strictly
in sequence and there is no in-protocol way to skip a missing object, so the
session wedges until it is torn down. That is the stall we hit under load. A
consumer-side patch cannot repair a lost byte — so the writer must either
deliver the file or die trying, which is what `uploadUntilDelivered` does.

## Testing without a VPS

Both ends of the protocol live in this repo and `storage/local` implements the
storage interface over a directory, so `Listener` + `Dialer` pairs run in a
single process against a temp dir:

- a session carries payloads both ways, byte for byte;
- four parallel sessions each arrive complete;
- a ~2 MB stream spans many chunk files with no hole and no corruption;
- with a backend rejecting every 4th upload, the stream still arrives intact.

These stand specs live in `proxy/fedarisha/transport/endtoend_spec_test.go`.

## Verification

```sh
go build ./...
go vet ./proxy/fedarisha/...
go test ./proxy/fedarisha/...
go test -race ./proxy/fedarisha/transport/...
```

Known pre-existing failures on a clean upstream checkout (missing geodata
assets, not related to this fork): `app/router`, `common/geodata`,
`infra/conf`.

Specs for the robustness behaviour live in
`proxy/fedarisha/transport/robustness_spec_test.go`; the delivery guarantee in
`delivery_spec_test.go`.

## Relationship to s3-bypass-desktop

The desktop client pins a core binary; this repository is what a future pin
points at. `scripts/prepare-core.mjs` in the desktop repo is the integration
point.