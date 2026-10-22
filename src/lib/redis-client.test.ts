import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { describe, it } from 'node:test'

import { waitForRedisReady } from './redis-client'

type FakeRedis = EventEmitter & { status: string }

function createFakeRedis(status: string): FakeRedis {
  const redis = new EventEmitter() as FakeRedis
  redis.status = status
  return redis
}

describe('waitForRedisReady', () => {
  it('resolves immediately when already ready', async () => {
    const redis = createFakeRedis('ready')
    await waitForRedisReady(redis, { timeoutMs: 50 })
  })

  it('waits for the ready event', async () => {
    const redis = createFakeRedis('connecting')
    const pending = waitForRedisReady(redis, { timeoutMs: 500 })
    setTimeout(() => {
      redis.status = 'ready'
      redis.emit('ready')
    }, 10)
    await pending
  })

  it('rejects on timeout while still connecting', async () => {
    const redis = createFakeRedis('connecting')
    await assert.rejects(
      () => waitForRedisReady(redis, { timeoutMs: 20 }),
      /not ready within 20ms/,
    )
  })
})
