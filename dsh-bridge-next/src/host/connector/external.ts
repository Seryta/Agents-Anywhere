import { readFile, stat } from 'node:fs/promises'
import { Mesh, MeshCallError, Router } from 'meshcall'
import type { Account } from '../account/api.js'
import { DeviceRecoveryRequired, type DeviceIdentity } from '../account/binding.js'
import type { ConnectorAction } from '../../contracts/connector.js'
import type { RuntimeLogPage, RuntimeLogQuery } from '../../contracts/logs.js'
import type { ControlEvent, ConnectorIdentity, ConnectorState as SharedState } from '../generated/connector-shared.js'
import { SharedServiceClient } from '../generated/connector-shared.js'
import { LogsServiceClient } from '../generated/connector-logs.js'
import { readRuntimeLogPage } from '../dsh-runtime/log-reader.js'
import { ConnectorCredentialError, type ConnectorProcess, type ConnectorState } from './process.js'

const RENDEZVOUS_VERSION = 1
const CONTROL_CONNECT_TIMEOUT_MS = 10_000

/** What `anywhere-cli serve` publishes for this user; the key is per Router, not per file. */
interface Rendezvous {
  version: number
  protocol: string
  instanceId: string
  pid: number
  endpoint: { unixPath: string } | { pipe: string }
  key: string
}

function parseRendezvous(value: unknown): Rendezvous {
  const document = value as Partial<Rendezvous> | null
  if (!document || document.version !== RENDEZVOUS_VERSION || document.protocol !== 'meshcall') throw new Error('共享 Connector 会合文件版本不受支持。')
  const endpoint = document.endpoint
  const local = endpoint && typeof endpoint === 'object' && typeof (endpoint as { unixPath?: unknown }).unixPath === 'string' ? { unixPath: (endpoint as { unixPath: string }).unixPath }
    : endpoint && typeof endpoint === 'object' && typeof (endpoint as { pipe?: unknown }).pipe === 'string' ? { pipe: (endpoint as { pipe: string }).pipe } : undefined
  if (!local || typeof document.key !== 'string' || !document.key.startsWith('mk_') || typeof document.instanceId !== 'string') throw new Error('共享 Connector 会合文件无效。')
  return { version: RENDEZVOUS_VERSION, protocol: 'meshcall', instanceId: document.instanceId, pid: Number(document.pid), endpoint: local, key: document.key }
}

/** The conversation states ConnectorState reports and this client mirrors. */
type State = { running: boolean; authFailed: boolean }

/**
 * Attaches to the shared Connector: one service for every local Agent.
 *
 * Nothing here provisions Python or starts a Connector process; closing this
 * client only drops the management connection. Stopping or restarting the
 * runtime through it affects every Agent on the machine, which is why the
 * plugin keeps those behind an explicit action.
 */
export class ExternalConnector implements ConnectorProcess {
  readonly external = true
  private mesh: Mesh | null = null
  private shared: SharedServiceClient | null = null
  private logsClient: LogsServiceClient | null = null
  private events: AbortController | null = null
  private failure: string | null = null
  private state: State = { running: false, authFailed: false }
  private listeners = new Set<(state: ConnectorState) => void>()

  constructor(private readonly servicePath: string) {}

  get running(): boolean { return this.shared !== null && this.state.running && !this.state.authFailed }
  get lastError(): string | null { return this.failure }
  /** Logs live in the Connector's own store, read through `aa.local.v2.Logs`. */
  get logsPath(): string | null { return null }
  hasRuntimeLogs(): boolean { return this.logsClient !== null }

  onState(listener: (state: ConnectorState) => void): () => void {
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }

  private updateState(value: Partial<SharedState> | undefined): void {
    if (!value || typeof value.running !== 'boolean' || typeof value.authFailed !== 'boolean') return
    if (value.running === this.state.running && value.authFailed === this.state.authFailed) return
    this.state = { running: value.running, authFailed: value.authFailed }
    for (const listener of this.listeners) listener({ running: value.running, authFailed: value.authFailed } as ConnectorState)
  }

  async prepare(): Promise<void> {
    await this.connect()
    this.updateState(await this.shared!.getState({ timeoutMs: CONTROL_CONNECT_TIMEOUT_MS }))
    this.watchEvents()
  }

  async bindAccount(account: Account, signal: AbortSignal, recovery?: 'reconnect' | 'recreate'): Promise<DeviceIdentity> {
    signal.throwIfAborted()
    await this.connect()
    let identity: ConnectorIdentity
    try {
      identity = await this.shared!.attachAccount(
        account.apiBaseUrl, account.userId, account.accessToken,
        recovery ?? null, { timeoutMs: 70_000 },
      )
    } catch (error) {
      throw this.recoveryError(error)
    }
    signal.throwIfAborted()
    if (!identity?.connectorId) throw new Error('共享 Connector 未返回设备身份。')
    return { connectorId: identity.connectorId, name: identity.name ?? identity.connectorId }
  }

  async start(binding: DeviceIdentity, apiBaseUrl: string, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted()
    await this.connect()
    const identity = await this.shared!.getIdentity({ timeoutMs: CONTROL_CONNECT_TIMEOUT_MS })
    if (identity.connectorId !== binding.connectorId || identity.serverUrl.replace(/\/$/, '') !== apiBaseUrl.replace(/\/$/, '')) {
      throw new Error('共享 Connector 的设备身份已变化，请重新连接。')
    }
    this.updateState(await this.shared!.start({ timeoutMs: 70_000 }))
    signal.throwIfAborted()
  }

  async assertHealthy(): Promise<void> {
    await this.connect()
    this.updateState(await this.shared!.getState({ timeoutMs: 15_000 }))
    if (this.state.authFailed) throw new ConnectorCredentialError()
    if (!this.state.running) throw new Error('共享 Connector 尚未运行，请检查常驻服务。')
  }

  /** Start, stop or restart the shared runtime; every Agent on the device is affected. */
  async controlRuntime(action: ConnectorAction): Promise<void> {
    await this.connect()
    const call = action === 'stop' ? this.shared!.stop({ timeoutMs: 70_000 })
      : action === 'restart' ? this.shared!.restart({ timeoutMs: 70_000 })
        : this.shared!.start({ timeoutMs: 70_000 })
    this.updateState(await call)
  }

  async readRuntimeLogs(query?: RuntimeLogQuery): Promise<RuntimeLogPage> {
    const client = this.logsClient
    if (!client) {
      return { available: false, items: [], firstSeq: null, lastSeq: null, total: 0, hasMoreBefore: false, hasMoreAfter: false, logsPath: null, updatedAt: new Date().toISOString() }
    }
    return readRuntimeLogPage(client, query, null)
  }

  /** Nothing is installed by this client: the shared service owns its environment. */
  async resetEnvironment(): Promise<void> { await this.stop() }

  async stop(): Promise<void> {
    this.events?.abort()
    this.events = null
    const mesh = this.mesh
    this.mesh = null
    this.shared = null
    this.logsClient = null
    this.state = { running: false, authFailed: false }
    await mesh?.close().catch(() => undefined)
  }

  private async connect(): Promise<void> {
    if (this.shared) return
    let document: string
    try {
      const info = await stat(this.servicePath)
      if (!info.isFile() || (info.mode & 0o077) !== 0 || (process.getuid && info.uid !== process.getuid())) throw new Error('权限无效')
      document = await readFile(this.servicePath, 'utf8')
    } catch {
      throw new Error('共享 Connector 服务不可用，请检查常驻服务是否运行。')
    }
    const rendezvous = parseRendezvous(JSON.parse(document))
    const router = new Router('unixPath' in rendezvous.endpoint ? { unixPath: rendezvous.endpoint.unixPath } : { pipe: rendezvous.endpoint.pipe })
    const mesh = new Mesh(router, { token: rendezvous.key, peerId: 'dsh-plugin-' + process.pid })
    const connecting = mesh.connect()
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      await Promise.race([
        connecting,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error('共享 Connector 管理通道连接超时。')), CONTROL_CONNECT_TIMEOUT_MS)
          timer.unref()
        }),
      ])
    } catch (error) {
      connecting.catch(() => undefined)
      await mesh.close().catch(() => undefined)
      throw error
    } finally {
      if (timer) clearTimeout(timer)
    }
    this.mesh = mesh
    this.shared = new SharedServiceClient(mesh)
    this.logsClient = new LogsServiceClient(mesh)
  }

  /** Follow state changes from the Router; a dropped stream is re-opened by the service. */
  private watchEvents(): void {
    if (this.events) return
    const controller = new AbortController()
    this.events = controller
    const run = async () => {
      let since: number | null = null
      while (!controller.signal.aborted) {
        try {
          const stream = this.shared!.events(since, { signal: controller.signal })
          for await (const event of stream as AsyncIterable<ControlEvent>) {
            since = event.seq
            if (event.kind === 'state' && event.state) this.updateState(event.state)
          }
        } catch { /* The service may be restarting; retry below. */ }
        if (controller.signal.aborted) break
        await new Promise(resolve => setTimeout(resolve, 500))
      }
    }
    void run()
  }

  /** Credential failures become the plugin's explicit recovery flow, as the stdio channel did. */
  private recoveryError(error: unknown): unknown {
    if (!(error instanceof MeshCallError)) return error
    const connectorId = (error.details as { connectorId?: unknown } | undefined)?.connectorId
    if (typeof connectorId === 'string' && connectorId) {
      if (error.code === 'credential_expired') return new DeviceRecoveryRequired(connectorId, 'disconnected')
      if (error.code === 'device_deleted') return new DeviceRecoveryRequired(connectorId, 'deleted')
    }
    return new Error(error.message)
  }
}
