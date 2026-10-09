# Security Policy

## Scope

This repository is the tunnel engine. The desktop client lives in
[thevladimirzh/s3-bypass-desktop](https://github.com/thevladimirzh/s3-bypass-desktop) —
please report client-side problems there.

In scope: the fedarisha protocol implementation, the transport code under
`proxy/fedarisha/`, and the release binaries built from this repository.

## Reporting a vulnerability

Report privately via GitHub's security advisory form for this repository
(**Security → Report a vulnerability**), not as a public issue. Please include
the core version, the platform, and the steps to reproduce.

You should get an acknowledgement within a few days. Please give us a reasonable
window to ship a fix before disclosing publicly.

## What to expect from a fix

The engine is used by a desktop client that pins a specific release binary, so
fixes land as a new release tag here first, and only reach users after the
desktop repo re-pins it. That path takes a little longer than in a typical
project — it is a deliberate trade for reproducible, load-tested builds.

## Upstream

This is a fork of
[Fedarisha/Xray-core-fedarisha](https://github.com/Fedarisha/Xray-core-fedarisha),
based on its `26.9.9-1.0.1fed` release. Vulnerabilities in code we did not
change are best reported upstream, though we will still fix anything that
affects our users. Note that upstream's issue tracker is disabled.