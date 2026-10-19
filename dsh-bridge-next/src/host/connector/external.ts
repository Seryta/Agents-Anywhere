import { lstat } from 'node:fs/promises'
import { createConnection, type Socket } from 'node:net'
import type { Account } from '../account/api.js'
import { DeviceRecoveryRequired, type DeviceIdentity } from '../account/binding.js'
import type { ConnectorAction } from '../../contracts/connector.js'
import type { ConnectorLogPage, ConnectorLogQuery } from '../../contracts/logs.js'
import { ConnectorCredentialError, type ConnectorProcess, type ConnectorState } from './process.js'

const MAX_FRAME = 1024 * 1024
interface Pending {
  resolve: (value: unknown) => void
  reject: (error: Error) => void
  timer: ReturnType<typeof setTimeout>
}

/** Attaches to the shared service. Disposing this client never stops that service. */
export class ExternalConnector implements ConnectorProcess {
  readonly external = true
  private socket: Socket | null = null
  private connecting: Promise<void> | null = null
  private pending = new Map<number, Pending>()
  private nextId = 0
  private buffer = ''
  private attached = false
  private timer: ReturnType<typeof setInterval> | null = null
  private polling = false
  private failure: string | null = null
  private state: ConnectorState = { running: false, authFailed: false }
  private listeners = new Set<(state: ConnectorState) => void>()

  constructor(private readonly path: string, private readonly pollIntervalMs = 2000) {}
  get running(): boolean { return this.attached && this.state.running && !this.state.authFailed }
  get lastError(): string | null { return this.failure }
  onState(listener: (state: ConnectorState) => void): () => void {
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }
  private updateState(value: unknown): void {
    if (!value || typeof value !== 'object') throw new Error('公共 Connector 状态响应无效。')
    const state = value as Partial<ConnectorState>
    if (typeof state.running !== 'boolean' || typeof state.authFailed !== 'boolean') throw new Error('公共 Connector 状态响应无效。')
    this.failure = null
    this.state = { running: state.running, authFailed: state.authFailed }
    for (const listener of this.listeners) listener({ ...this.state })
  }
  async prepare(): Promise<void> {
    this.attached = true
    const state = await this.call('connector.getState') as { protocolVersion?: number }
    if (state?.protocolVersion !== 1) { await this.stop(); throw new Error('公共 Connector 本机协议版本不兼容。') }
    this.updateState(state)
    this.timer ??= setInterval(() => { void this.poll() }, this.pollIntervalMs)
    this.timer.unref()
  }
  private async poll(): Promise<void> {
    if (!this.attached || this.polling) return
    this.polling = true
    try { this.updateState(await this.call('connector.getState')) }
    catch (error) { this.failure = error instanceof Error ? error.message : '公共 Connector 服务不可用。' }
    finally { this.polling = false }
  }
  async bindAccount(account: Account, signal: AbortSignal, recovery?: 'reconnect' | 'recreate'): Promise<DeviceIdentity> {
    signal.throwIfAborted()
    if (!this.attached) await this.prepare()
    const result = await this.call('connector.attachAccount', {
      serverUrl: account.apiBaseUrl, userId: account.userId, accessToken: account.accessToken,
      ...(recovery ? { recovery } : {}),
    }, 70_000) as Partial<DeviceIdentity> | null
    signal.throwIfAborted()
    if (!result || typeof result.connectorId !== 'string' || !result.connectorId || typeof result.name !== 'string') throw new Error('公共 Connector 设备响应无效。')
    return { connectorId: result.connectorId, name: result.name }
  }
  async start(binding: DeviceIdentity, apiBaseUrl: string, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted()
    const identity = await this.call('connector.getIdentity') as { connectorId?: string; serverUrl?: string } | null
    if (identity?.connectorId !== binding.connectorId || identity.serverUrl !== apiBaseUrl) throw new Error('公共 Connector 的设备身份已变化，请重新连接。')
    this.updateState(await this.call('connector.start'))
    signal.throwIfAborted()
  }
  async assertHealthy(): Promise<void> {
    this.updateState(await this.call('connector.getState'))
    if (this.state.authFailed) throw new ConnectorCredentialError()
    if (!this.state.running) throw new Error('公共 Connector 尚未运行。')
  }
  async readLogs(query?: ConnectorLogQuery): Promise<ConnectorLogPage> {
    return await this.call('connector.readLogs', query) as ConnectorLogPage
  }
  async control(action: ConnectorAction): Promise<void> {
    this.updateState(await this.call('connector.' + action))
  }
  async stop(): Promise<void> {
    this.attached = false
    if (this.timer) clearInterval(this.timer)
    this.timer = null
    this.socket?.destroy()
    this.socket = null
    this.state = { running: false, authFailed: false }
    this.rejectPending(new Error('本机管理连接已关闭。'))
  }
  private async connect(): Promise<void> {
    if (this.socket && !this.socket.destroyed) return
    if (this.connecting) return this.connecting
    this.connecting = (async () => {
      const info = await lstat(this.path).catch(() => { throw new Error('公共 Connector 服务不可用，请检查常驻服务。') })
      if (!info.isSocket() || (info.mode & 0o077) !== 0 || (process.getuid && info.uid !== process.getuid())) throw new Error('公共 Connector 本机接口权限无效。')
      this.buffer = ''
      const socket = createConnection(this.path)
      this.socket = socket
      socket.setEncoding('utf8')
      socket.on('data', (chunk: string) => { if (this.socket === socket) this.receive(chunk) })
      socket.on('error', () => { if (this.socket === socket) this.failure = '公共 Connector 服务不可用，请检查常驻服务。' })
      socket.on('close', () => {
        if (this.socket !== socket) return
        this.socket = null
        this.state = { running: false, authFailed: false }
        this.rejectPending(new Error('公共 Connector 连接已断开。'))
        for (const listener of this.listeners) listener({ ...this.state })
      })
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => { socket.destroy(); reject(new Error('公共 Connector 连接超时。')) }, 5000)
        socket.once('connect', () => { clearTimeout(timer); resolve() })
        socket.once('error', () => { clearTimeout(timer); reject(new Error('公共 Connector 服务不可用，请检查常驻服务。')) })
      })
    })().finally(() => { this.connecting = null })
    return this.connecting
  }
  private async call(method: string, params?: unknown, timeoutMs = 10_000): Promise<unknown> {
    await this.connect()
    return new Promise((resolve, reject) => {
      const id = ++this.nextId
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error('公共 Connector 响应超时。')) }, timeoutMs)
      this.pending.set(id, { resolve, reject, timer })
      this.socket!.write(JSON.stringify({ jsonrpc: '2.0', id, method, ...(params === undefined ? {} : { params }) }) + '\n', error => {
        if (error) { clearTimeout(timer); this.pending.delete(id); reject(new Error('公共 Connector 写入失败。')) }
      })
    })
  }
  private receive(chunk: string): void {
    this.buffer += chunk
    if (Buffer.byteLength(this.buffer) > MAX_FRAME) { this.socket?.destroy(); return }
    let end: number
    while ((end = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, end); this.buffer = this.buffer.slice(end + 1)
      try {
        const frame = JSON.parse(line)
        if (frame.method === 'connector/state') { this.updateState(frame.params); continue }
        const pending = this.pending.get(frame.id)
        if (!pending) continue
        clearTimeout(pending.timer); this.pending.delete(frame.id)
        if (frame.error) {
          const id = frame.error.data?.connectorId
          if (typeof id === 'string' && (frame.error.code === -32011 || frame.error.code === -32012)) pending.reject(new DeviceRecoveryRequired(id, frame.error.code === -32012 ? 'deleted' : 'disconnected'))
          else pending.reject(new Error(typeof frame.error.message === 'string' ? frame.error.message : '公共 Connector 请求失败。'))
        } else pending.resolve(frame.result)
      } catch { this.socket?.destroy(); return }
    }
  }
  private rejectPending(error: Error): void {
    for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(error) }
    this.pending.clear()
  }
}
