#!/usr/bin/env node
// Builds an ISOLATED client+server config pair that shares one storage prefix.
//
// The point: the production bucket prefix is watched by the upstream server as
// well as by ours, so whichever one writes the session ACK first wins — a test
// against the shared prefix cannot prove our server is in the path. Pointing a
// fresh client AND our server at a dedicated prefix means only our server can
// pick the session up, so the test exercises our code on both ends.
//
// Secret hygiene: values are never printed, only booleans and key names.

import { readFileSync, writeFileSync } from 'node:fs'

const [, , clientPath, prefix, outClient, outServer, listenPort, userPrefix] = process.argv
if (!clientPath || !prefix || !outClient || !outServer) {
  console.error('usage: build-isolated-pair.mjs <client.json> <prefix> <outClient.json> <outServer.json> [port] [userPrefix]')
  process.exit(2)
}

// The server side always runs ListenMultiUser: it scans "<user>/sessions" and
// refuses any prefix that is not listed in its `clients`. So the client has to
// write under a user directory and the server has to know that user — otherwise
// the session is never discovered. This mirrors how the real deployment issues
// per-user configs.
const user = userPrefix ?? 'b18-selftest'

const client = JSON.parse(readFileSync(clientPath, 'utf8'))
const outbound = (client.outbounds ?? []).find((o) => o?.protocol === 'fedarisha')
if (!outbound?.settings?.storage) {
  console.error('FAIL: no fedarisha outbound with settings.storage in the client config')
  process.exit(1)
}

const storage = { ...outbound.settings.storage, prefix, sessionsDir: `${user}/sessions` }
const tuning = outbound.settings.tuning ?? {}

// Client: same shape as before, only the prefix differs.
const isoClient = {
  ...client,
  outbounds: client.outbounds.map((o) =>
    o?.protocol === 'fedarisha'
      ? { ...o, settings: { ...o.settings, storage } }
      : o,
  ),
}
writeFileSync(outClient, JSON.stringify(isoClient, null, 2), { mode: 0o600 })

// Server: an inbound watching the same prefix.
const isoServer = {
  log: { loglevel: 'info' },
  inbounds: [
    {
      tag: 'fedarisha-in',
      listen: '127.0.0.1',
      port: Number(listenPort ?? 8443),
      protocol: 'fedarisha',
      settings: {
        storage: { ...storage, sessionsDir: 'sessions' },
        tuning,
        clients: [{ id: user, level: 0 }],
        userLevel: 0,
      },
    },
  ],
  outbounds: [{ tag: 'direct', protocol: 'freedom', settings: {} }],
}
writeFileSync(outServer, JSON.stringify(isoServer, null, 2), { mode: 0o600 })

console.log('isolated pair written')
console.log('prefix used      :', prefix)
console.log('bucket present   :', Boolean(storage.bucket))
console.log('credentials      :', Boolean(storage.accessKey && storage.secretKey))
console.log('client config    :', outClient)
console.log('server config    :', outServer)
console.log('server listen    : 127.0.0.1:' + isoServer.inbounds[0].port)