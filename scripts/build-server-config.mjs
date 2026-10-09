#!/usr/bin/env node
// Builds a server-shaped fedarisha config from the client's own config.
//
// The client config already carries the storage block (bucket, endpoint,
// credentials) the server needs — both ends talk to the same bucket. This
// script copies that block into an inbound so our own server can be run
// against the same backend.
//
// Secret hygiene: nothing is printed. Only booleans and key NAMES are
// reported, never values. The server config is written with mode 600.

import { readFileSync, writeFileSync } from 'node:fs'

const [, , clientPath, serverPath, listenPort] = process.argv
if (!clientPath || !serverPath) {
  console.error('usage: build-server-config.mjs <client.json> <server.json> [listenPort]')
  process.exit(2)
}

const client = JSON.parse(readFileSync(clientPath, 'utf8'))

// Pull the storage block out of the fedarisha outbound.
const outbound = (client.outbounds ?? []).find((o) => o?.protocol === 'fedarisha')
if (!outbound) {
  console.error('FAIL: client config has no outbound with protocol "fedarisha"')
  process.exit(1)
}
const storage = outbound.settings?.storage
if (!storage) {
  console.error('FAIL: fedarisha outbound has no settings.storage')
  process.exit(1)
}

const required = ['bucket', 'endpoint', 'accessKey', 'secretKey']
const missing = required.filter((k) => !storage[k])
if (missing.length > 0) {
  // Report the NAMES only — never the values.
  console.error(`FAIL: storage is missing required keys: ${missing.join(', ')}`)
  process.exit(1)
}

const tuning = outbound.settings?.tuning ?? {}

const server = {
  log: { loglevel: 'info' },
  inbounds: [
    {
      tag: 'fedarisha-in',
      listen: '127.0.0.1',
      port: Number(listenPort ?? 8443),
      protocol: 'fedarisha',
      settings: {
        storage: { ...storage },
        tuning: { ...tuning },
        clients: [],
        userLevel: 0,
      },
    },
  ],
  outbounds: [
    { tag: 'direct', protocol: 'freedom', settings: {} },
  ],
}

writeFileSync(serverPath, JSON.stringify(server, null, 2), { mode: 0o600 })

// Booleans only — the point is to prove the shape without leaking anything.
console.log('server config written:', serverPath)
console.log('storage keys present :', Object.keys(storage).sort().join(', '))
console.log('credentials present  :', Boolean(storage.accessKey && storage.secretKey))
console.log('inbound listen       : 127.0.0.1:' + server.inbounds[0].port)
console.log('tuning carried over  :', Object.keys(tuning).length > 0)