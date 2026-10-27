import assert from 'node:assert/strict'
import test from 'node:test'
import { createServer } from 'node:http'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { ExternalConnector } from '../../src/host/connector/external.js'
import { writeJson } from '../../src/host/storage/files.js'
import { launchConnector, rendezvousPath } from '../helpers/python-connector.js'

async function until(check: () => boolean | Promise<boolean>, message: string) {
  for (let i = 0; i < 1000; i++) { if (await check()) return; await delay(20) }
  assert.fail(message)
}

async function fixture(t: import('node:test').TestContext) {
  const root = await mkdtemp(join(tmpdir(), 'aa-shared-'))
  const calls: string[] = []
  const server = createServer((request, response) => {
    const path = new URL(request.url!, 'http://fixture').pathname
    calls.push(request.method + ' ' + path)
    response.setHeader('Content-Type', 'application/json')
    if (path === '/api/v2/auth/me') return response.end(JSON.stringify({ userId: 'owner', displayName: 'Owner' }))
    if (path === '/api/v2/connectors/existing') return response.end(JSON.stringify({ connector: { id: 'existing', userId: 'owner', name: 'Machine', status: 'online' } }))
    if (path === '/api/v2/connector/auth') {
      assert.equal(request.headers.authorization, 'Connector existing:DEVICE-SECRET')
      return response.end('{}')
    }
    response.statusCode = 500
    response.end('{}')
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const apiBaseUrl = 'http://127.0.0.1:' + (server.address() as import('node:net').AddressInfo).port
  const configPath = join(root, 'private', 'shared', 'connector.json')
  await writeJson(configPath, { serverUrl: apiBaseUrl, connectorId: 'existing', connectorToken: 'DEVICE-SECRET' })
  const before = await readFile(configPath, 'utf8')
  let service = launchConnector(root, 'shared', 'serve')
  const client = new ExternalConnector(rendezvousPath(root))
  const account = { apiBaseUrl, userId: 'owner', displayName: 'Owner', accessToken: 'OAUTH-SECRET', expiresAt: Date.now() + 3600_000 }
  const signal = new AbortController().signal
  t.after(async () => {
    await client.stop()
    await service.close()
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
    await rm(root, { recursive: true, force: true })
  })
  await until(async () => (await readFile(rendezvousPath(root), 'utf8').catch(() => '')).includes('meshcall'),
    'Shared service published no rendezvous: ' + (service.stderr || `exit ${service.child.exitCode}`))
  assert.equal(service.child.exitCode, null, service.stderr)
  return {
    root, client, account, calls, signal, configPath, before, apiBaseUrl,
    get service() { return service },
    async restart() {
      await service.close()
      await until(() => !client.running, 'Client kept reporting a stopped service as running')
      service = launchConnector(root, 'shared', 'serve')
      await until(async () => (await readFile(rendezvousPath(root), 'utf8').catch(() => '')).includes('meshcall'), 'Restarted service published no rendezvous')
      assert.equal(service.child.exitCode, null, service.stderr)
    },
  }
}

test('external clients manage the shared service without owning or stopping it', async t => {
  const h = await fixture(t)
  await h.client.prepare()
  const identity = await h.client.bindAccount(h.account, h.signal)
  assert.deepEqual(identity, { connectorId: 'existing', name: 'Machine' })
  assert.doesNotMatch(JSON.stringify(identity), /SECRET/)
  const pid = h.service.child.pid
  await h.client.start(identity, h.apiBaseUrl, h.signal)
  await h.client.assertHealthy()
  await h.client.stop()
  assert.equal(h.service.child.exitCode, null)
  // Detaching only drops the management connection: the service and its device stay.
  await h.client.prepare()
  await h.client.assertHealthy()
  assert.equal(h.service.child.pid, pid)
  assert.equal(await readFile(h.configPath, 'utf8'), h.before)
  assert.equal(h.calls.some(call => /revoke|POST \/api\/v2\/connectors$/.test(call)), false)
  // An explicit command does change every Agent, so the state must follow it.
  await h.client.controlRuntime('stop')
  await until(() => !h.client.running, 'Shared runtime kept running after an explicit stop')
  await h.client.controlRuntime('start')
  await until(() => h.client.running, 'Shared runtime did not restart')
})

test('a restarted shared service is re-attached without re-registering the device', async t => {
  const h = await fixture(t)
  await h.client.prepare()
  const identity = await h.client.bindAccount(h.account, h.signal)
  const firstPid = h.service.child.pid
  await h.restart()
  await h.client.stop()
  await h.client.prepare()
  await h.client.assertHealthy()
  assert.notEqual(h.service.child.pid, firstPid)
  const again = await h.client.bindAccount(h.account, h.signal)
  assert.deepEqual(again, identity)
  assert.equal(h.calls.filter(call => call === 'POST /api/v2/connectors').length, 0)
  assert.equal(await readFile(h.configPath, 'utf8'), h.before)
})

test('a missing shared service reports an actionable error instead of provisioning one', async t => {
  const root = await mkdtemp(join(tmpdir(), 'aa-shared-missing-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  const client = new ExternalConnector(join(root, 'connector-service'))
  await assert.rejects(() => client.prepare(), /共享 Connector 服务不可用/)
  assert.equal(client.running, false)
})
