import assert from 'node:assert/strict'
import test from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { InteractionStream, openInteractionEvents } from '../../src/host/dsh-runtime/interaction-stream.js'

test('current Gateway event streams receive the same admitted Peer used for replies', async () => {
  const peer = { principal: { kind: 'owner' }, interactions: 'answer', signal: new AbortController().signal }
  const signal = new AbortController().signal
  let received: unknown[] = []
  const wire = { open: async function (endpoint: unknown, payload: unknown, uplink: unknown, identity: unknown, abort: unknown) {
    received = [endpoint, payload, uplink, identity, abort]
    return (async function* () {})()
  } }
  await openInteractionEvents(wire as never, signal, peer)
  assert.equal(received[0], '$events')
  assert.equal(received[3], peer, 'Gateway 0.2.1 rejects an absent Peer before opening the event stream')
  assert.equal(received[4], signal)
})

test('failed interaction retries do not announce unchanged unavailable capabilities', async () => {
  let attempts = 0, changes = 0
  let dispose = async () => {}
  const ready = {
    typertGateway: { wireStream: { open: async function (_endpoint: unknown, _payload: unknown, _signal: unknown) {
      attempts++
      throw new Error('interaction source unavailable')
    } } },
    connection: {},
    effect: (start: () => () => Promise<void>) => { dispose = start() },
  }
  const ctx = { inject: (_keys: unknown, callback: (value: typeof ready) => void) => {
    callback(ready)
    return { dispose: () => dispose() }
  } }
  const stream = new InteractionStream(ctx as never, async () => {}, () => { changes++ })
  try {
    await delay(560)
    assert.ok(attempts >= 2, 'the unavailable source must still be retried')
    assert.equal(stream.available, false)
    assert.equal(changes, 0, 'retries must not flood every session with unchanged capability updates')
  } finally { await stream.close() }
})

test('the local consumer uses Connection browserPeer and announces only actual availability transitions', async () => {
  const peer = { principal: { kind: 'owner' }, interactions: 'answer', signal: new AbortController().signal }
  const received: unknown[] = [], availability: boolean[] = []
  let dispose = async () => {}
  const ready = {
    typertGateway: { wireStream: { open: async function (_endpoint: unknown, _payload: unknown, _uplink: unknown, identity: unknown, signal: AbortSignal) {
      received.push(identity)
      return (async function* () {
        yield { type: 'ready', clientId: 'local-consumer' }
        yield { type: 'ready', clientId: 'local-consumer' }
        await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }))
      })()
    } } },
    connection: { browserPeer: peer },
    effect: (start: () => () => Promise<void>) => { dispose = start() },
  }
  const ctx = { inject: (_keys: unknown, callback: (value: typeof ready) => void) => {
    callback(ready)
    return { dispose: () => dispose() }
  } }
  const stream = new InteractionStream(ctx as never, async () => {}, () => availability.push(stream.available))
  try {
    for (let n = 0; n < 50 && !stream.available; n++) await delay(5)
    assert.equal(stream.available, true)
    assert.deepEqual(received, [peer])
    assert.deepEqual(availability, [true], 'duplicate ready frames do not invalidate all session capabilities')
  } finally { await stream.close() }
  assert.deepEqual(availability, [true, false])
})
