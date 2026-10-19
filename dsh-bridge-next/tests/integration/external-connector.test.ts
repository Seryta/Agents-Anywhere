import assert from 'node:assert/strict'
import test from 'node:test'
import { createServer } from 'node:http'
import { chmod, mkdir, mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { ExternalConnector } from '../../src/host/connector/external.js'
import { OnboardingManager } from '../../src/host/onboarding/manager.js'
import { writeJson } from '../../src/host/storage/files.js'
import { launchConnector, connectorProject } from '../helpers/python-connector.js'
import { SourceConnector } from '../../src/host/connector/process.js'

async function until(check: () => boolean | Promise<boolean>) {
  for (let i = 0; i < 1000; i++) { if (await check()) return; await delay(20) }
  assert.fail('Shared service did not reach the expected state')
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
    if (path === '/api/v2/connectors' && request.method === 'GET') return response.end(JSON.stringify({ connectors: [{ id: 'existing', userId: 'owner', name: 'Machine', status: 'online' }] }))
    if (path === '/api/v2/connector/auth') { assert.equal(request.headers.authorization, 'Connector existing:DEVICE-SECRET'); return response.end('{}') }
    response.statusCode = 500
    response.end('{}')
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const apiBaseUrl = 'http://127.0.0.1:' + (server.address() as import('node:net').AddressInfo).port
  const configPath = join(root, 'private', 'shared', 'connector.json')
  await writeJson(configPath, { serverUrl: apiBaseUrl, connectorId: 'existing', connectorToken: 'DEVICE-SECRET' })
  const before = await readFile(configPath, 'utf8')
  let service = launchConnector(root, 'shared', 'serve')
  const client = new ExternalConnector(join(root, 'control.sock'), 25)
  const account = { apiBaseUrl, userId: 'owner', displayName: 'Owner', accessToken: 'OAUTH-SECRET', expiresAt: Date.now() + 3600_000 }
  const signal = new AbortController().signal
  t.after(async () => {
    await client.stop()
    await service.close()
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
    await rm(root, { recursive: true, force: true })
  })
  await until(() => service.stderr.includes('backend-ready') || service.child.exitCode !== null)
  assert.equal(service.child.exitCode, null, service.stderr)
  return {
    root, client, account, calls, signal, configPath, before, apiBaseUrl,
    get service() { return service },
    async restart() {
      await service.close()
      await until(() => !client.running)
      service = launchConnector(root, 'shared', 'serve')
      await until(() => service.stderr.includes('backend-ready') || service.child.exitCode !== null)
      assert.equal(service.child.exitCode, null, service.stderr)
    },
  }
}

test('external clients attach to the real Python service without provisioning or stopping peers', async t => {
  const h = await fixture(t)
  await h.client.prepare()
  const identity = await h.client.bindAccount(h.account, h.signal)
  assert.deepEqual(identity, { connectorId: 'existing', name: 'Machine' })
  const pid = h.service.child.pid
  await h.client.start(identity, h.apiBaseUrl, h.signal)
  await h.client.stop()
  assert.equal(h.service.child.exitCode, null)
  await h.client.prepare()
  await h.client.assertHealthy()
  assert.equal(h.service.child.pid, pid)
  assert.equal(await readFile(h.configPath, 'utf8'), h.before)
  assert.equal(h.calls.some(call => /revoke|POST \/api\/v2\/connectors$/.test(call)), false)
  assert.doesNotMatch(JSON.stringify(identity), /SECRET/)
  await h.client.control('stop')
  assert.equal(h.client.running, false)
  await h.client.control('start')
  assert.equal(h.client.running, true)
})

test('external mode reconnects after the system service restarts', async t => {
  const h = await fixture(t)
  await h.client.prepare()
  await h.restart()
  await until(() => h.client.running)
  await h.client.assertHealthy()
  assert.equal(await readFile(h.configPath, 'utf8'), h.before)
})

test('DSH reload, logout and reset preserve the shared device and runtime', async t => {
  const h = await fixture(t)
  const pluginRoot = join(h.root, 'plugin')
  await mkdir(pluginRoot)
  await writeJson(join(pluginRoot, 'account.json'), h.account)
  const makeManager = () => new OnboardingManager({
    apiBaseUrl: h.apiBaseUrl, stateRoot: pluginRoot, connectorSourceDir: '/missing-source', uvPath: '/missing-uv', connectorSocketPath: join(h.root, 'control.sock'),
  }, { detect: async () => ({ status: 'absent', message: 'fixture' }), checkServer: async () => undefined, systemLanguages: async () => ['en-US'], pollIntervalMs: 10 })
  let manager = makeManager()
  t.after(() => manager.dispose())
  await manager.resume()
  await until(async () => (await manager.inspect()).stage === 'ready')
  assert.equal((await manager.inspect()).connector.mode, 'external')
  assert.equal((await manager.inspect()).connectorId, 'existing')
  await manager.dispose()
  assert.equal(h.service.child.exitCode, null)
  manager = makeManager()
  await manager.resume()
  await until(async () => (await manager.inspect()).stage === 'ready')
  await manager.logout()
  assert.equal(h.service.child.exitCode, null)
  await manager.resetConnector(false)
  await h.client.prepare()
  await h.client.assertHealthy()
  assert.equal(await readFile(h.configPath, 'utf8'), h.before)
  assert.equal(h.calls.some(call => /revoke|POST \/api\/v2\/connectors$/.test(call)), false)
  assert.equal((await manager.inspect()).account, null)
})

test('external mode rejects insecure socket permissions without a fallback connector', async t => {
  const h = await fixture(t)
  await chmod(join(h.root, 'control.sock'), 0o666)
  await assert.rejects(h.client.prepare(), /权限无效/)
  assert.equal(h.service.child.exitCode, null)
  assert.equal(h.calls.length, 0)
})


test('managed mode rejects a shared owner before looking up or rotating device credentials', async t => {
  const h = await fixture(t)
  const pluginRoot = join(h.root, 'managed-plugin')
  await writeJson(join(pluginRoot, 'account.json'), h.account)
  const config = { apiBaseUrl: h.apiBaseUrl, stateRoot: pluginRoot, connectorSourceDir: connectorProject, uvPath: process.execPath }
  const managed = new SourceConnector(config, () => {
    const control = launchConnector(h.root, 'managed-reservation')
    t.after(() => control.close())
    return control.child
  })
  const manager = new OnboardingManager(config, {
    connector: managed, detect: async () => ({ status: 'absent', message: 'fixture' }),
    checkServer: async () => undefined, systemLanguages: async () => ['en-US'],
    machineState: { readConnectorIds: async () => ['existing'] }, pollIntervalMs: 10,
  })
  t.after(() => manager.dispose())
  await manager.resume()
  await until(async () => (await manager.inspect()).stage === 'error')
  assert.equal(h.calls.some(call => call.includes('/connectors')), false, 'ownership must be acquired before any device credential operation')
  assert.equal(h.service.child.exitCode, null)
  assert.equal(await readFile(h.configPath, 'utf8'), h.before)
})
