# Running our own fedarisha server

The upstream server drops a file when all of its upload attempts fail
(`uploadAttempts`), and because the peer reads strictly in sequence that single
missing object wedges the session — measured: a parallel-download burst stalls
in ~9 s and takes ~4 minutes to recover. This fork's `uploadUntilDelivered`
retries instead of dropping, and with our code on both ends the same test moves
600 MB with zero holes.

So running our own server is what turns the fix from "recover faster" into "do
not stall at all".

## What was proven

Against the owner's own bucket, on an isolated prefix, with our patched server
and our patched client (2026-10-09):

| | upstream server | our server |
| --- | --- | --- |
| 6 × 50 MB, two rounds | all streams stall in ~9 s | **600 MB, every byte delivered** |
| `hole at seq` | 2 | 0 |
| teardowns / re-dials | 5 dials, 3 failed handshakes | 0 |
| sessions used | recreated repeatedly | one, for the whole run |
| recovery | ~4 min | not needed |

Roughly 1.7 MB/s per parallel stream, ~10 MB/s aggregate.

## Before you start: you need

- **A host** that can reach your S3 bucket. The server is stateless — it holds
  no user data — so one small VPS is enough. It is the coordination point, not
  a data path: traffic flows between client and bucket.
- **A storage prefix per deployment**, distinct from the upstream one. Both
  servers watch their prefix, and whoever writes the session ACK first wins —
  sharing a prefix means upstream can steal your sessions (and vice versa).
- **A user id** in the server's `clients` list. The server always runs
  `ListenMultiUser`: it scans `<user>/sessions` and refuses any prefix it does
  not know. A client that writes to a bare `sessions/` is never discovered —
  this cost us a debugging round.

## Install

```sh
VERSION=v0.1.0-fork.1 SHA256=<digest from the release> \
  sudo -E ./deploy/install-server.sh
```

The script creates a `s3bypass` system user, installs a versioned binary to
`/opt/s3bypass-protocol/xray`, and drops a hardened systemd unit. It refuses to
start without a readable config, and it never writes one — credentials are
provisioned separately so a re-run cannot clobber live ones.

## Config

`/etc/s3bypass-protocol/server.json`, mode 0640, owned by `root:s3bypass`:

```jsonc
{
  "log": { "loglevel": "info" },
  "inbounds": [
    {
      "tag": "fedarisha-in",
      "listen": "127.0.0.1",
      "port": 8443,
      "protocol": "fedarisha",
      "settings": {
        "storage": {
          "type": "s3",
          "bucket": "…",
          "endpoint": "…",
          "region": "…",
          "accessKey": "…",
          "secretKey": "…",
          "prefix": "fedarisha/your-deployment/",
          "sessionsDir": "sessions"
        },
        "clients": [{ "id": "your-user", "level": 0 }]
      }
    }
  ],
  "outbounds": [{ "tag": "direct", "protocol": "freedom", "settings": {} }]
}
```

`scripts/build-isolated-pair.mjs` generates a matching **client** config from an
existing one — same bucket and credentials, different prefix and user — without
printing any value. `scripts/build-server-config.mjs` derives a server config
from a client config for the shared-prefix case (useful for a dry run; see the
caveat above).

## Operating it

```sh
systemctl status s3bypass-protocol
journalctl -u s3bypass-protocol -f
```

What to watch:

- `upload retry N` lines are normal under load and prove the guarantee working —
  a file is being retried rather than dropped.
- `upload ERR` means a file was lost: the session is going to wedge, and
  `hole at seq …` on the client side will follow.
- `new session …` is the handshake; if it stops appearing while clients report
  `no ACK`, check the prefix and the `clients` list first.

## Cost and scope

One small VPS, one bucket prefix, a config handed to users. The client app
already imports a JSON profile, so distribution is a preset rather than a new
feature. The client must point at our release binary — that is the desktop
side of the work (`prepare-core.mjs`).