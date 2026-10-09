#!/usr/bin/env node
// Generates the fedarisha server config and one client config per user.
//
// Why this exists: hand-writing these files is how deployments go wrong.
// The trap is `sessionsDir` — the server always scans `<user>/sessions`, so a
// client pointed at a bare `sessions/` is never discovered and then waits
// silently for an ACK that will not come. It cost us an hour to find once;
// here you cannot get it wrong, because the user id and the directory are the
// same value.
//
// Secret hygiene: values are never printed. Each file is written with mode 600
// and the summary reports booleans only.
//
// Usage:
//   node scripts/generate-configs.mjs \
//     --bucket my-bucket --endpoint https://storage.example.net --region ru-central1 \
//     --prefix fedarisha/my-server/ \
//     --access-key AK --secret-key SK \
//     --users vasya petya \
//     --out-dir ./out
//
// --secret-file reads the secret key from a file instead of argv, so it does
// not land in the shell history or in `ps` output.

import { readFileSync, writeFileSync, mkdirSync } from 'node:fs'
import { resolve, join } from 'node:path'

const args = process.argv.slice(2)
function flag(name) {
  const i = args.indexOf(`--${name}`)
  return i === -1 ? undefined : args[i + 1]
}
function required(name) {
  const v = flag(name)
  if (!v) {
    console.error(`missing required flag --${name}`)
    process.exit(2)
  }
  return v
}

const bucket = required('bucket')
const endpoint = required('endpoint')
const region = required('region')
const prefix = required('prefix')
const accessKey = required('access-key')
const secretFile = flag('secret-file')
const secretKey = secretFile ? readFileSync(resolve(secretFile), 'utf8').trim() : required('secret-key')
const users = (flag('users') ?? '').split(',').map((u) => u.trim()).filter(Boolean)
const outDir = resolve(flag('out-dir') ?? './out')
const serverPort = Number(flag('server-port') ?? 8443)

if (users.length === 0) {
  console.error('missing required flag --users (comma-separated, e.g. --users vasya,petya)')
  process.exit(2)
}
if (!prefix.endsWith('/')) {
  console.error(`--prefix must end with a slash, got "${prefix}"`)
  process.exit(2)
}
for (const u of users) {
  if (!/^[a-z0-9._-]+$/i.test(u)) {
    console.error(`invalid user id "${u}" — use letters, digits, dot, dash, underscore`)
    process.exit(2)
  }
}

mkdirSync(outDir, { recursive: true })

// --- server -----------------------------------------------------------------
// The server scans <user>/sessions inside the prefix and refuses any prefix not
// listed in clients, so every user gets an entry.
const server = {
  log: { loglevel: 'info' },
  inbounds: [
    {
      tag: 'fedarisha-in',
      listen: '127.0.0.1',
      port: serverPort,
      protocol: 'fedarisha',
      settings: {
        storage: {
          type: 's3',
          bucket,
          endpoint,
          region,
          accessKey,
          secretKey,
          prefix,
          sessionsDir: 'sessions',
        },
        clients: users.map((id) => ({ id, level: 1 })),
        userLevel: 1,
      },
    },
  ],
  outbounds: [{ tag: 'direct', protocol: 'freedom', settings: {} }],
}

const serverPath = join(outDir, 'server.json')
writeFileSync(serverPath, JSON.stringify(server, null, 2), { mode: 0o600 })

// --- clients ----------------------------------------------------------------
// Shape matches what the desktop client accepts: one loopback SOCKS inbound and
// a fedarisha outbound. sessionsDir is <user>/sessions — the whole point.
const clientPaths = []
for (const user of users) {
  const client = {
    log: { loglevel: 'warning' },
    inbounds: [
      {
        tag: 'socks-in',
        listen: '127.0.0.1',
        port: 10808,
        protocol: 'socks',
        settings: { auth: 'noauth', udp: true },
      },
    ],
    outbounds: [
      {
        tag: 'proxy',
        protocol: 'fedarisha',
        settings: {
          storage: {
            type: 's3',
            bucket,
            endpoint,
            region,
            accessKey,
            secretKey,
            prefix,
            sessionsDir: `${user}/sessions`,
          },
          tuning: {
            idleTimeoutSec: 300,
            pollIntervalMs: 100,
            writeIntervalMs: 20,
            maxFileSizeBytes: 2097152,
          },
        },
      },
      { tag: 'direct', protocol: 'freedom', settings: {} },
    ],
  }
  const p = join(outDir, `client-${user}.json`)
  writeFileSync(p, JSON.stringify(client, null, 2), { mode: 0o600 })
  clientPaths.push(p)
}

console.log('written:')
console.log(`  ${serverPath}   (clients: ${users.join(', ')})`)
for (const p of clientPaths) console.log(`  ${p}`)
console.log('checks:')
console.log(`  prefix ends with slash      ${prefix.endsWith('/')}`)
console.log(`  credentials present         ${Boolean(accessKey && secretKey)}`)
console.log(`  sessionsDir per user        ${users.map((u) => `${u}/sessions`).join(', ')}`)
console.log(`  server sessionsDir          sessions (server scans <user>/sessions)`)
console.log('')
console.log('Next:')
console.log(`  sudo install -o root -g s3bypass -m 640 ${serverPath} /etc/s3bypass-protocol/server.json`)
console.log('  hand each client-<user>.json to that user — it goes straight into the desktop app')