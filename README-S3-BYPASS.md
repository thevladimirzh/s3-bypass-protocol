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

**The wire protocol is untouched.** Session layout, file naming and encryption
are unchanged, so this core still interoperates with the upstream server.

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
`proxy/fedarisha/transport/robustness_spec_test.go`.

## Relationship to s3-bypass-desktop

The desktop client pins a core binary; this repository is what a future pin
points at. `scripts/prepare-core.mjs` in the desktop repo is the integration
point.