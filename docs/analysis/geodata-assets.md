# Vendored geo and tun assets

A fork cannot run `.github/workflows/scheduled-assets-update.yml`: the workflow
dispatch it relies on is refused with *"Resource not accessible by
integration"*. Without these files the inherited `Tests and Checkings` job
fails on `app/router`, `common/geodata`, `infra/conf` and `testing/scenarios`,
and the Windows packaging step has nothing to ship.

They are vendored here, each verified against the publisher's own checksum
before it was committed.

| File | Publisher | Version / digest check |
| --- | --- | --- |
| `resources/geoip.dat` | [Loyalsoldier/v2ray-rules-dat](https://github.com/Loyalsoldier/v2ray-rules-dat) `release/geoip.dat` | matches upstream `geoip.dat.sha256sum` |
| `resources/geosite.dat` | [Loyalsoldier/v2ray-rules-dat](https://github.com/Loyalsoldier/v2ray-rules-dat) `release/geosite.dat` | matches upstream `geosite.dat.sha256sum` |
| `resources/wintun/**` | [wintun.net](https://www.wintun.net) `0.14.1` | archive matches the `ASSETHASH` pinned in `scheduled-assets-update.yml` |

To refresh, re-run the same commands and re-verify before committing:

```sh
for f in geoip geosite; do
  curl -fsSL "https://raw.githubusercontent.com/Loyalsoldier/v2ray-rules-dat/release/${f}.dat.sha256sum"
  curl -fsSL "https://raw.githubusercontent.com/Loyalsoldier/v2ray-rules-dat/release/${f}.dat" -o "resources/${f}.dat"
  shasum -a 256 "resources/${f}.dat"
done

curl -fsSL "https://www.wintun.net/builds/wintun-0.14.1.zip" -o /tmp/wintun.zip
shasum -a 256 /tmp/wintun.zip   # must equal the ASSETHASH in the workflow
unzip -o -q /tmp/wintun.zip -d resources/
```

Nothing here is fedarisha-specific. If the upstream fork ever regains the
ability to run its asset workflow, these files can be dropped and the
`.gitignore` exceptions removed.