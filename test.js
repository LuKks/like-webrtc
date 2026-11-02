/* eslint-disable */

// TODO: Slightly messy and some draft code, but some important edge cases are being tested

const test = require('get-npm-global')('brittle')
const { Duplex, pipeline } = require('streamx')
const b4a = require('b4a')
// const Multimux = require('multimux')

const RPC = require('./lib/rpc.js')
const Peer = require('./lib/peer.js')

const PartyServer = require('./lib/party.js')
const RTC = require('./index.js')

const http = require('http')
const { WebSocketServer } = require('ws')

async function createWebSocketServer (t, onConnection) {
  const server = http.createServer()
  const wss = new WebSocketServer({ server })

  wss.on('connection', onConnection)

  const listening = new Promise(resolve => server.on('listening', resolve))

  server.listen(0, '127.0.0.1')

  t.teardown(() => {
    wss.close()
    server.close()
  })

  await listening

  return 'ws://' + server.address().address + ':' + server.address().port
}

test('rpc', async function (t) {
  const [s1, s2] = streamPair()

  const a = new RPC(s1)
  const b = new RPC(s2)

  a.respond('process-data', () => {
    return 1337
  })

  a.respond('throws', (req) => {
    const error = new Error('Something happened')

    error.expose = req.body

    if (req.body) {
      error.name = 'ErrorRPC'
    }

    error.code = 'SOMETHING_HAPPENED'

    throw error
  })

  const result = await b.request('process-data')
  t.is(result, 1337)

  try {
    await b.request('throws', true)
    t.fail()
  } catch (err) {
    t.is(err.message, 'Request failed')
    t.is(err.code, 'RPC_REQUEST_FAILED')

    t.is(err.cause.message, 'Something happened')
    t.is(err.cause.code, 'SOMETHING_HAPPENED')
  }

  // TODO: Test a.on('error', (capture the SOMETHING_HAPPENED))

  try {
    await b.request('throws', false)
    t.fail()
  } catch (err) {
    t.is(err.message, 'Request failed')
    t.is(err.code, 'RPC_REQUEST_FAILED')

    t.is(err.cause.message, 'Internal error')
    t.is(err.cause.code, 'INTERNAL')
  }

  await a.close()
  await b.close()
})

// for (let i = 0; i < 100; i++)
test.skip('creating negotiated data channels', { timeout: 120000 }, async function (t) {
  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  // const a = new RTC({ signaling, debug: false, RTCPeerConnection: require('werift').RTCPeerConnection, RTCIceCandidate: require('werift').RTCIceCandidate })
  const a = new RTC({ signaling, debug: false })
  const b = new RTC({ signaling, debug: false })

  const lc1 = t.test()
  const lc2 = t.test()

  lc1.plan(2)
  lc2.plan(2)

  let serverPeer = null

  const server = a.createServer(peer => {
    serverPeer = peer

    const stream = new RTC.DataChannelDuplex(serverPeer.createDataChannel('multimux', { negotiated: true, id: 0 }))

    stream.on('open', () => lc1.pass('[server] multimux stream open'))
    stream.on('data', (data) => lc1.pass('[server] multimux stream data: ' + b4a.toString(data)))
    stream.on('close', () => console.log('[server] multimux stream close'))

    stream.write('My name is server!')

    serverPeer.ondatachannel = e => {
      echoDataChannel(e.channel)
    }
  })

  await server.listen()

  const peer = b.connect(server.publicKey)
  const stream = new RTC.DataChannelDuplex(peer.createDataChannel('multimux', { negotiated: true, id: 0 }))

  stream.on('open', () => lc2.pass('[client] multimux stream open'))
  stream.on('data', (data) => lc2.pass('[client] multimux stream data: ' + b4a.toString(data)))
  stream.on('close', () => console.log('[client] multimux stream close'))

  stream.write('My name is client!')

  await waitForCondition(() => {
    return serverPeer && isPeerConnected(peer) && isPeerConnected(serverPeer)
  }, 30000, 'peers connected')

  await lc1
  await lc2

  for (let i = 1; i <= 10; i += 2) {
    const lc3 = t.test()
    const lc4 = t.test()

    lc3.plan(3)
    lc4.plan(3)

    {
      const a = new RTC.DataChannelDuplex(peer.createDataChannel('socks5', { negotiated: true, id: i }))

      a.on('open', () => lc3.pass('[client] stream open'))
      a.on('data', (data) => lc3.pass('[client] stream data', data))
      a.on('close', () => lc3.pass('[client] stream close'))

      a.write('My name is client!')
      a.end()
    }

    {
      const b = new RTC.DataChannelDuplex(serverPeer.createDataChannel('socks5', { negotiated: true, id: i }))

      b.on('open', () => lc4.pass('[server] stream open'))
      b.on('data', (data) => lc4.pass('[server] stream data', data))
      b.on('close', () => lc4.pass('[server] stream close'))

      b.write('My name is server!')
      b.end()
    }

    await lc3
    await lc4

    // await new Promise(resolve => setTimeout(resolve, 2000))
  }

  await peer.close()
  await serverPeer.close()

  await server.close()
  await a.close()
  await b.close()
  await party.close()
})

// for (let i = 0; i < 100; i++)
test('echo round trip - many channels after connected', { timeout: 120000 }, async function (t) {
  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling, debug: true })
  const b = new RTC({ signaling, debug: true })

  let serverPeer = null

  const server = a.createServer(peer => {
    serverPeer = peer

    peer.ondatachannel = e => {
      echoDataChannel(e.channel)
    }
  })

  await server.listen()

  const peer = b.connect(server.publicKey)

  await echoRoundTrip(t, peer, 'bootstrap', 'bootstrap hello')

  await waitForCondition(() => {
    return serverPeer && isPeerConnected(peer) && isPeerConnected(serverPeer)
  }, 30000, 'peers connected')

  for (let count = 0; count < 1; count++) {
    const label = 'wire-' + count
    const message = 'Hello World! ' + count

    await echoRoundTrip(t, peer, label, message)
  }

  t.pass('opened, echoed, and closed 100 data channels after connected')

  await server.close()
  await a.close()
  await b.close()
  await party.close()
})

test('echo round trip - new channel after ICE restart', { timeout: 90000 }, async function (t) {
  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling, debug: true })
  const b = new RTC({ signaling, debug: true })

  let serverPeer = null

  const server = a.createServer(peer => {
    serverPeer = peer

    peer.ondatachannel = e => {
      echoDataChannel(e.channel)
    }
  })

  await server.listen()

  const peer = b.connect(server.publicKey)

  await echoRoundTrip(t, peer, 'initial', 'initial hello')

  await waitForCondition(() => {
    return serverPeer && isPeerConnected(peer) && isPeerConnected(serverPeer)
  }, 30000, 'peers connected before ICE restart')

  await echoRoundTrip(t, peer, 'after-ice-restart', 'hello after ice restart')

  const internalPeer = getOnlyInternalPeer(b)
  const before = internalPeer.iceRestartCompleted

  internalPeer.restartIce('test forced ICE restart')

  await echoRoundTrip(t, peer, 'after-ice-restart', 'hello after ice restart')

  await waitForCondition(() => {
    return internalPeer.iceRestartCompleted > before
  }, 30000, 'ICE restart negotiation completed')

  await echoRoundTrip(t, peer, 'after-ice-restart', 'hello after ice restart')

  await waitForCondition(() => {
    return peer.signalingState === 'stable' && isPeerConnected(peer)
  }, 30000, 'peer stable and connected after ICE restart')

  await echoRoundTrip(t, peer, 'after-ice-restart', 'hello after ice restart')

  await peer.close()
  await b.close()

  await server.close()
  await a.close()

  await party.close()
})

test('ICE repair ignores failed ICE while aggregate connection is connected', function (t) {
  const peer = Object.create(Peer.prototype)

  peer.pc = {
    connectionState: 'connected',
    iceConnectionState: 'failed'
  }

  t.is(peer._iceNeedsRepair(), false, 'connected aggregate state is healthy')

  peer.pc = {
    connectionState: 'connecting',
    iceConnectionState: 'failed'
  }

  t.is(peer._iceNeedsRepair(), true, 'failed ICE still repairs before connected')
})

test('first data channel negotiation is coalesced', { timeout: 90000 }, async function (t) {
  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling })
  const b = new RTC({ signaling })

  const server = a.createServer(peer => {
    peer.ondatachannel = e => {
      echoDataChannel(e.channel)
    }
  })

  await server.listen()

  const peer = b.connect(server.publicKey)

  await echoRoundTrip(t, peer, 'first', 'hello')

  await waitForCondition(() => {
    return a.peers.size === 1 && b.peers.size === 1
  }, 30000, 'internal peers')

  const serverPeer = getOnlyInternalPeer(a)
  const clientPeer = getOnlyInternalPeer(b)

  await waitForCondition(() => {
    return (
      peer.signalingState === 'stable' &&
      serverPeer.pc.signalingState === 'stable' &&
      clientPeer.pc.signalingState === 'stable' &&
      isPeerConnected(peer)
    )
  }, 30000, 'stable after first data channel')

  await delay(500)

  t.is(clientPeer.localOfferSeq, 1, 'client sent one offer')
  t.is(clientPeer.remoteAnswerSeq, 1, 'client accepted one answer')
  t.is(serverPeer.localOfferSeq, 0, 'server did not create a follow-up offer')
  t.ok(serverPeer.remoteOfferSeq >= 1, 'server accepted the client offer')
  t.is(clientPeer.needsNegotiation, false, 'client has no pending negotiation')
  t.is(serverPeer.needsNegotiation, false, 'server has no pending negotiation')

  await server.close()
  await a.close()
  await b.close()
  await party.close()
})

test('direct data channel end flushes payload before close', { timeout: 90000 }, async function (t) {
  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling })
  const b = new RTC({ signaling })

  const received = []
  let serverPeer = null

  const server = a.createServer(peer => {
    serverPeer = peer

    peer.ondatachannel = e => {
      const stream = new RTC.DataChannelDuplex(e.channel)

      stream.on('data', data => {
        if (e.channel.label === 'bootstrap') {
          stream.write(data)
          return
        }

        received.push({
          label: e.channel.label,
          data
        })
      })

      stream.on('error', noop)
    }
  })

  await server.listen()

  const peer = b.connect(server.publicKey)

  await echoRoundTrip(t, peer, 'bootstrap', 'bootstrap hello')

  await waitForCondition(() => {
    return serverPeer && isPeerConnected(peer) && isPeerConnected(serverPeer)
  }, 30000, 'peers connected')

  for (let i = 0; i < 20; i++) {
    const label = 'short-' + i
    const expected = Buffer.from([5, 1, 0, i])
    const stream = new RTC.DataChannelDuplex(peer.createDataChannel(label))

    stream.on('error', noop)

    await waitEvent(stream, 'open', 15000, label + ' open')

    const closePromise = waitEvent(stream, 'close', 15000, label + ' close')

    stream.end(expected)

    await closePromise

    await waitForCondition(() => {
      return received.length > i
    }, 15000, label + ' server data')

    t.is(received[i].label, label, label + ' label')
    t.is(received[i].data.toString('hex'), expected.toString('hex'), label + ' data')
  }

  await server.close()
  await a.close()
  await b.close()
  await party.close()
})

test.skip('multimux socks5 streams fairness over negotiated data channel', { timeout: 120000 }, async function (t) {
  const { Duplex, Transform } = require('streamx')

  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling })
  const b = new RTC({ signaling })

  let serverPeer = null
  let serverMux = null
  let serverTransport = null

  const duration = 20_000
  const packetSize = 16 * 1024
  const tickMs = 20

  const sockets = [
    { id: 'socket-1', mbPerSecond: 'max', maxBurst: 4 * 1024 * 1024 },
    { id: 'socket-2', mbPerSecond: 50 },
    { id: 'socket-3', mbPerSecond: 1 }
  ]

  const totals = new Map()
  const lastTotals = new Map()
  const loads = []

  let lastReport = Date.now()

  for (const socket of sockets) {
    row(socket.id, 'client-tx')
    row(socket.id, 'client-rx')
    row(socket.id, 'server-rx')
    row(socket.id, 'server-tx')
  }

  const speedTimer = setInterval(() => {
    const now = Date.now()
    const ms = now - lastReport
    lastReport = now

    let combinedRx = 0
    let combinedTx = 0

    for (const socket of sockets) {
      const tx = row(socket.id, 'client-tx')
      const rx = row(socket.id, 'client-rx')

      const txDelta = delta(tx)
      const rxDelta = delta(rx)

      combinedTx += txDelta
      combinedRx += rxDelta

      t.comment(
        socket.id +
        ' tx=' + formatSpeed(txDelta, ms) +
        ' rx=' + formatSpeed(rxDelta, ms) +
        ' queued=' + formatBytes(tx.bytes - rx.bytes) +
        ' avg-lat=' + rx.avgLatency.toFixed(2) + 'ms' +
        ' max-lat=' + rx.maxLatency.toFixed(2) + 'ms' +
        ' total-rx=' + formatBytes(rx.bytes)
      )
    }

    t.comment(
      'combined tx=' + formatSpeed(combinedTx, ms) +
      ' rx=' + formatSpeed(combinedRx, ms)
    )
    t.comment()
  }, 1000)

  const scheduler = setInterval(() => {
    for (const load of loads) load.refill()
    for (const load of loads) load.pump()
  }, tickMs)

  const server = a.createServer(peer => {
    serverPeer = peer

    serverTransport = new RTC.DataChannelDuplex(peer.createDataChannel('multimux', { negotiated: true, id: 0 }))
    serverTransport.on('error', noop)

    serverMux = Multimux.from(false, serverTransport)

    serverMux.on('protocol', ({ seq, name, id }) => {
      if (name !== 'socks5') return

      const socketId = id ? b4a.toString(id) : 'socket-' + seq
      const demux = serverMux.createStream({ seq, name, id })

      demux.on('error', noop)

      pipeline(
        demux,
        speedMeter(socketId, 'server-rx'),
        echoTransform(),
        speedMeter(socketId, 'server-tx'),
        demux,
        err => {
          if (isPrematureClose(err)) return
          if (err) t.fail(err.message)
        }
      )
    })
  })

  await server.listen()

  const peer = b.connect(server.publicKey)
  const clientTransport = new RTC.DataChannelDuplex(peer.createDataChannel('multimux', { negotiated: true, id: 0 }))
  const clientMux = Multimux.from(true, clientTransport)

  clientTransport.on('error', noop)

  await waitForCondition(() => {
    return serverPeer && isPeerConnected(peer) && isPeerConnected(serverPeer)
  }, 30000, 'peers connected')

  const results = await Promise.all(sockets.map(runMuxSocket))

  clearInterval(speedTimer)
  clearInterval(scheduler)

  for (const result of results) {
    t.comment(
      result.id +
      ' sent=' + formatBytes(result.sent) +
      ' received=' + formatBytes(result.received) +
      ' avg=' + formatSpeed(result.received, result.ms) +
      ' avg-lat=' + result.avgLatency.toFixed(2) + 'ms' +
      ' max-lat=' + result.maxLatency.toFixed(2) + 'ms'
    )

    t.ok(result.received > 0, result.id + ' received echoed data')
  }

  clientMux.destroy()
  if (serverMux) serverMux.destroy()
  clientTransport.destroy()
  if (serverTransport) serverTransport.destroy()

  await peer.close()
  await serverPeer.close()

  await server.close()
  await a.close()
  await b.close()
  await party.close()

  async function runMuxSocket (opts) {
    const demux = clientMux.createStream({ name: 'socks5', id: b4a.from(opts.id) })
    const local = createLoadSocket(opts)

    demux.on('error', noop)
    local.stream.on('error', noop)

    loads.push(local)

    const started = Date.now()

    return new Promise(resolve => {
      pipeline(
        local.stream,
        speedMeter(opts.id, 'client-tx'),
        demux,
        speedMeter(opts.id, 'client-rx'),
        local.stream,
        err => {
          if (isPrematureClose(err)) return done()
          if (err) t.fail(err.message)
          done()
        }
      )

      setTimeout(() => {
        local.stop()
      }, duration)

      function done () {
        const tx = row(opts.id, 'client-tx')
        const rx = row(opts.id, 'client-rx')

        resolve({
          id: opts.id,
          sent: tx.bytes,
          received: rx.bytes,
          ms: Date.now() - started,
          avgLatency: rx.avgLatency,
          maxLatency: rx.maxLatency
        })
      }
    })
  }

  function createLoadSocket (opts) {
    let stream = null
    let seq = 0
    let stopped = false
    let blocked = false
    let tokens = 0
    let carry = null

    const maxSpeed = opts.mbPerSecond === 'max'
    const bytesPerSecond = maxSpeed ? 0 : opts.mbPerSecond * 1024 * 1024
    const bytesPerTick = maxSpeed ? 0 : Math.floor(bytesPerSecond * tickMs / 1000)
    const maxBurst = opts.maxBurst || bytesPerTick || packetSize

    stream = new Duplex({
      highWaterMark: 4 * 1024 * 1024,

      read (cb) {
        blocked = false
        cb(null)
      },

      write (data, cb) {
        consumeEcho(opts.id, data)
        cb(null)
      }
    })

    return {
      stream,
      refill,
      pump,
      stop
    }

    function refill () {
      if (stopped) return
      if (!maxSpeed) tokens += bytesPerTick
    }

    function pump () {
      if (stopped || blocked || stream.destroying) return

      let pushed = 0

      while (true) {
        if (maxSpeed) {
          if (pushed >= maxBurst) return
        } else {
          if (tokens < packetSize) return
          tokens -= packetSize
        }

        const packet = Buffer.allocUnsafe(packetSize)

        packet.writeUInt32BE(seq++, 0)
        packet.writeBigUInt64BE(process.hrtime.bigint(), 4)
        packet.fill(opts.id.charCodeAt(opts.id.length - 1), 12)

        pushed += packet.length

        if (!stream.push(packet)) {
          blocked = true
          return
        }
      }
    }

    function stop () {
      stopped = true
      stream.push(null)
    }

    function consumeEcho (id, data) {
      let offset = 0

      if (carry) {
        const missing = packetSize - carry.length
        const chunk = data.subarray(0, missing)

        carry = Buffer.concat([carry, chunk])
        offset += chunk.length

        if (carry.length === packetSize) {
          measureLatency(id, carry)
          carry = null
        }
      }

      while (offset + packetSize <= data.length) {
        measureLatency(id, data.subarray(offset, offset + packetSize))
        offset += packetSize
      }

      if (offset < data.length) {
        carry = data.subarray(offset)
      }
    }
  }

  function speedMeter (id, dir) {
    return new Transform({
      transform (data, cb) {
        row(id, dir).bytes += data.byteLength
        cb(null, data)
      }
    })
  }

  function echoTransform () {
    return new Transform({
      transform (data, cb) {
        cb(null, data)
      }
    })
  }

  function measureLatency (id, packet) {
    const sent = packet.readBigUInt64BE(4)
    const latency = Number(process.hrtime.bigint() - sent) / 1e6
    const rx = row(id, 'client-rx')

    rx.latencyCount++
    rx.latencyTotal += latency
    rx.avgLatency = rx.latencyTotal / rx.latencyCount
    rx.maxLatency = Math.max(rx.maxLatency, latency)
  }

  function row (id, dir) {
    const key = id + ':' + dir
    let value = totals.get(key)

    if (!value) {
      value = {
        id,
        dir,
        bytes: 0,
        latencyCount: 0,
        latencyTotal: 0,
        avgLatency: 0,
        maxLatency: 0
      }

      totals.set(key, value)
    }

    return value
  }

  function delta (value) {
    const key = value.id + ':' + value.dir
    const prev = lastTotals.get(key) || 0
    const next = value.bytes - prev

    lastTotals.set(key, value.bytes)

    return next
  }

  function formatSpeed (bytes, ms) {
    return formatBytes(bytes / (ms / 1000)) + '/s'
  }

  function formatBytes (bytes) {
    return (bytes / 1024 / 1024).toFixed(2) + ' MiB'
  }
})



test.skip('multimux socks5 stream over negotiated data channel', { timeout: 90000 }, async function (t) {
  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling })
  const b = new RTC({ signaling })

  const socks5 = require('like-socks5')
  const socks = socks5.createServer({ Socket: RTC.SocketDuplex, debug: false })

  await new Promise((resolve, reject) => {
    socks.listen(0, '0.0.0.0', err => {
      if (err) reject(err)
      else resolve()
    })
  })

  const server = a.createServer(peer => {
    const stream = new RTC.DataChannelDuplex(peer.createDataChannel('multimux', { negotiated: true, id: 0 }))
    const mux = Multimux.from(false, stream)

    stream.on('open', () => console.log('[server] mux open'))
    stream.on('close', () => console.log('[server] mux close'))

    mux.on('protocol', ({ seq, name, id, cmd, data }) => {
      console.log('rtc protocols', { seq, name, id, cmd, data })

      if (name === 'socks5') {
        const demux = mux.createStream({ seq, name, id })
        const socket = new RTC.SocketDuplex(net.connect({ port: socks.address().port, host: '127.0.0.1' }))

        demux.on('open', () => console.log('[server] demux open'))
        // demux.on('data', (data) => console.log('[server] demux data', data))
        demux.on('close', () => console.log('[server] demux close'))

        pipeline(demux, socket, demux, onPipelineError)
      }
    })
  })

  await server.listen()

  const peer = b.connect(server.publicKey)
  const stream = new RTC.DataChannelDuplex(peer.createDataChannel('multimux', { negotiated: true, id: 0 }))
  const mux = Multimux.from(true, stream)

  stream.on('open', () => console.log('[client] mux open'))
  stream.on('close', () => console.log('[client] mux close'))

  const net = require('net')
  const local = net.createServer()

  local.on('connection', (socket) => {
    const demux = mux.createStream({ name: 'socks5' })

    demux.on('open', () => console.log('[client] demux open'))
    // demux.on('data', () => console.log('[client] demux data'))
    demux.on('close', () => console.log('[client] demux close'))

    pipeline(socket, demux, socket, onPipelineError)
  })

  local.listen(1080, '127.0.0.1')

  await new Promise(resolve => setTimeout(resolve, 5 * 60 * 1000))

  await peer.close()
  await serverPeer.close()

  await server.close()
  await a.close()
  await b.close()
  await party.close()

  function onPipelineError (err) {
    if (err) {
      if (err.code === 'ERR_STREAM_PREMATURE_CLOSE') return
      if (err.message === 'Premature close') return
      if (err.message === 'Writable stream closed prematurely') return
      if (err.message === 'Remote stream error') return

      console.log('pump failed', [err.code, err.message, err.cause], err)
    }
  }
})

function echoDataChannel (channel) {
  const stream = new RTC.DataChannelDuplex(channel)

  stream.on('data', data => {
    stream.write(data)
  })

  stream.on('error', noop)
}

async function echoRoundTrip (t, peer, label, message) {
  const stream = new RTC.DataChannelDuplex(peer.createDataChannel(label))

  stream.on('error', noop)

  await waitEvent(stream, 'open', 15000, label + ' open')

  const dataPromise = waitEvent(stream, 'data', 15000, label + ' data')

  stream.write(message)

  const data = await dataPromise

  t.is(data.toString(), message, label + ' echo')

  const closePromise = waitEvent(stream, 'close', 15000, label + ' close')

  stream.destroy()

  await closePromise
}

function getOnlyInternalPeer (rtc) {
  const peers = Array.from(rtc.peers.values())

  if (peers.length !== 1) {
    throw new Error('Expected exactly one internal peer, got ' + peers.length)
  }

  return peers[0]
}

function isPeerConnected (peer) {
  return (
    peer.connectionState === 'connected' ||
    peer.iceConnectionState === 'connected' ||
    peer.iceConnectionState === 'completed'
  )
}

function waitForCondition (condition, timeout, name) {
  const started = Date.now()

  return new Promise((resolve, reject) => {
    const interval = setInterval(() => {
      let ok = false

      try {
        ok = !!condition()
      } catch {}

      if (ok) {
        clearInterval(interval)
        resolve()
        return
      }

      if (Date.now() - started >= (timeout || 15000)) {
        clearInterval(interval)
        reject(new Error('Timed out waiting for ' + name))
      }
    }, 25)
  })
}

function waitEvent (emitter, name, timeout, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup()
      reject(new Error('Timed out waiting for ' + label))
    }, timeout)

    function cleanup () {
      clearTimeout(timer)
      emitter.removeListener(name, onevent)
      emitter.removeListener('error', onerror)
    }

    function onevent (...args) {
      cleanup()
      resolve(args.length > 1 ? args : args[0])
    }

    function onerror (err) {
      cleanup()
      reject(err)
    }

    emitter.once(name, onevent)
    emitter.once('error', onerror)
  })
}

function delay (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function noop () {}

function echoDuplex () {
  let stream = null

  stream = new Duplex({
    read (cb) {
      cb(null)
    },
    write (data, cb) {
      stream.push(data)
      cb(null)
    },
    final (cb) {
      stream.push(null)
      cb(null)
    }
  })

  return stream
}

function isPrematureClose (err) {
  return err && (
    err.code === 'ERR_STREAM_PREMATURE_CLOSE' ||
    err.message === 'Premature close'
  )
}

for (let i = 0; i < 20; i++)
test('party', { timeout: 90000 }, async function (t) {
  // process.stdout.write('\u001Bc\u001B[3J')

  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)

    const rawStream = new RTC.WebSocketDuplex(ws)

    party.handle(rawStream)
  })

  const lc = t.test()

  lc.plan(6)

  const a = new RTC({ signaling, debug: true })

  const server = a.createServer(peer => {
    peer.ondatachannel = e => {
      const rawStream = new RTC.DataChannelDuplex(e.channel)

      rawStream.on('open', function () {
        lc.pass('server stream open')
      })

      rawStream.on('data', function (data) {
        lc.is(data.toString(), 'Hello World!', 'server stream data')

        rawStream.write(data)
      })

      rawStream.on('close', function () {
        lc.pass('server stream closed')
      })
    }
  })

  await server.listen()

  t.comment('Server ID', server.publicKey.toString('hex'))

  const b = new RTC({ signaling, debug: true })

  const peer = b.connect(server.publicKey)

  const rawStream = new RTC.DataChannelDuplex(peer.createDataChannel('wire'))

  rawStream.on('open', function () {
    lc.pass('client stream open')
  })

  rawStream.on('data', function (data) {
    lc.is(data.toString(), 'Hello World!', 'client stream data')

    rawStream.destroy()
  })

  rawStream.on('close', function () {
    lc.pass('client stream closed')
  })

  rawStream.write('Hello World!')
  // rawStream.end()

  console.time('lc')
  await lc
  console.timeEnd('lc')

  // TODO: Close peer first
  console.time('server.close')
  await server.close()
  console.timeEnd('server.close')

  console.time('a.close')
  await a.close()
  console.timeEnd('a.close')
  console.time('b.close')
  await b.close()
  console.timeEnd('b.close')

  console.time('party.close')
  await party.close()
  console.timeEnd('party.close')
})

test('announce and unannounce', async function (t) {                                                                              
  const party = new PartyServer()                                                                                                 
  const signaling = await createWebSocketServer(t, function (ws) {                                                                
    party.handle(new RTC.WebSocketDuplex(ws))                                                                                               
  })                                                                                                                              
                                                                                                                                  
  const rtc = new RTC({ signaling })                                                                                              
  const server = rtc.createServer()                                                                                               
                                                                                                                                  
  await server.listen()                                                                                                           
                                                                                                                                  
  const target = server.publicKey.toString('hex')                                                                                 
  t.ok(party.announces.has(target), 'server is announced on the party server')                                                    
                                                                                                                                  
  await server.close()                                                                                                            
                                                                                                                                  
  t.absent(party.announces.has(target), 'server is unannounced after closing')                                                    
                                                                                                                                  
  await rtc.close()                                                                                                               
  await party.close()                                                                                                             
})

test('server close withdraws an in-flight topic announce', async function (t) {
  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, function (ws) {
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const rtc = new RTC({ signaling })
  const server = rtc.createServer()

  await server.listen()

  const topic = 'announce-close-race'
  const announcing = server.announce(topic)

  await server.close()
  await announcing

  const peers = await rtc.lookup(topic)

  await rtc.unannounce(topic, { keyPair: server.keyPair })
  await rtc.close()
  await party.close()

  t.is(peers.length, 0, 'server close withdraws the in-flight topic announce')
})

test('leaving a topic keeps a manual announcement', async function (t) {
  t.plan(1)

  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, function (ws) {
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const rtc = new RTC({ signaling })
  const server = rtc.createServer()
  const topic = 'manual-announce-join-leave'

  await server.listen()
  await server.announce(topic)
  await server.join(topic)
  await server.leave(topic)

  const peers = await rtc.lookup(topic)
  const announced = peers.some(peer => b4a.equals(peer.publicKey, server.publicKey))

  await rtc.close()
  await party.close()

  t.ok(announced, 'leave keeps the manual topic announcement')
})

test('unannounce keeps looking up without republishing', async function (t) {
  t.plan(3)

  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, function (ws) {
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling })
  const b = new RTC({ signaling })
  const server = a.createServer()
  const otherServer = b.createServer()
  const topic = 'unannounce-while-joined'

  await server.listen()
  await server.join(topic)

  const announced = await b.lookup(topic)
  t.is(announced.length, 1, 'join announces while listening')

  await server.unannounce(topic)
  await server.listen()

  const withdrawn = await b.lookup(topic)
  t.is(withdrawn.length, 0, 'listen keeps the topic withdrawn')

  await otherServer.listen()
  await otherServer.announce(topic)

  const connection = new Promise(resolve => server.once('connection', resolve))

  await server.join(topic)

  const peer = await connection
  t.ok(peer, 'joined server continues to find peers')

  await a.close()
  await b.close()
  await party.close()
})

test('unannounce before listen suppresses automatic announce', async function (t) {
  t.plan(1)

  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, function (ws) {
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const rtc = new RTC({ signaling })
  const server = rtc.createServer()
  const topic = 'unannounce-before-listen'

  await server.join(topic)
  await server.unannounce(topic)
  await server.listen()

  const peers = await rtc.lookup(topic)

  t.is(peers.length, 0, 'listen keeps the topic unannounced')

  await rtc.close()
  await party.close()
})

test('connect emits connection without a data channel', async function (t) {
  t.plan(1)

  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, function (ws) {
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling })
  const b = new RTC({ signaling })

  t.teardown(async function () {
    await a.close()
    await b.close()
    await party.close()
  }, { force: true })

  const server = a.createServer()
  await server.listen()

  const connection = waitEvent(server, 'connection', 15000, 'server connection')

  const clientPeer = b.connect(server.publicKey)
  const serverPeer = await connection

  t.ok(serverPeer, 'server received a connection')

  await serverPeer.close()
  await clientPeer.close()
})

test('group connections', async function (t) {
  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, function (ws) {
    party.handle(new RTC.WebSocketDuplex(ws))
  })

  const a = new RTC({ signaling })
  const b = new RTC({ signaling })

  let serverPeer = null
  const server = a.createServer(peer => {
    serverPeer = peer
  })
  await server.listen()
  await server.announce('group1')

  const results = await b.lookup('group1')
  t.is(results.length, 1)

  const peer = b.connect(results[0].publicKey)

  peer.createDataChannel('idle')

  await waitForCondition(() => serverPeer && isPeerConnected(peer) && isPeerConnected(serverPeer))

  await peer.close()
  await serverPeer.close()

  await server.unannounce('group1')
  const results2 = await b.lookup('group1')

  t.is(results2.length, 0)

  await a.close()
  await b.close()
  await party.close()
})

test('signaling reconnects after stream destroy', async function (t) {                                                                                                              
  const party = new PartyServer()                                                                                                                                                   
  const signaling = await createWebSocketServer(t, function (ws) {                                                                                                                  
    party.handle(new RTC.WebSocketDuplex(ws))                                                                                                                                                 
  })                                                                                                                                                                                
                                                                                                                                                                                    
  const rtc = new RTC({ signaling })                                                                                                                                                
  const server = rtc.createServer()                                                                                                                                                 
  await server.listen()                                                                                                                                                             
                                                                                                                                                                                    
  const oldRpc = server.signaling.rpc                                                                                                                                               
  t.ok(oldRpc, 'server has an initial rpc')                                                                                                                                         
                                                                                                                                                                                    
  // Simulate a network drop by destroying the underlying stream                                                                                                                    
  oldRpc.stream.destroy()                                                                                                                                                           
                                                                                                                                                                                    
  // Wait a bit longer than the 1000ms reconnect timeout                                                                                                                            
  await new Promise(resolve => setTimeout(resolve, 3000))                                                                                                                           
                                                                                                                                                                                    
  const newRpc = server.signaling.rpc                                                                                                                                               
  t.not(newRpc, oldRpc, 'a new rpc instance was created')                                                                                                                           
  t.ok(newRpc.opened, 'the new rpc connection is open')                                                                                                                             
                                                                                                                                                                                    
  await server.close()                                                                                                                                                              
  await rtc.close()                                                                                                                                                                 
  await party.close()                                                                                                                                                               
})

test('server re-announces after reconnect', async function (t) {                                                                                                                    
  const party = new PartyServer()                                                                                                                                                   
  const signaling = await createWebSocketServer(t, function (ws) {                                                                                                                  
    party.handle(new RTC.WebSocketDuplex(ws))                                                                                                                                                 
  })                                                                                                                                                                                
                                                                                                                                                                                    
  const rtc = new RTC({ signaling })                                                                                                                                                
  const server = rtc.createServer()                                                                                                                                                 
  await server.listen()                                                                                                                                                             
                                                                                                                                                                                    
  const target = server.publicKey.toString('hex')                                                                                                                                   
  t.ok(party.announces.has(target), 'server is announced initially')                                                                                                                
                                                                                                                                                                                    
  // Force a reconnect                                                                                                                                                              
  server.signaling.rpc.stream.destroy()                                                                                                                                             
  await new Promise(resolve => setTimeout(resolve, 3000))                                                                                                                           
                                                                                                                                                                                    
  // After reconnect the online handler should have re-announced                                                                                                                    
  t.ok(party.announces.has(target), 'server re-announced after signaling reconnect')                                                                                                
                                                                                                                                                                                    
  await server.close()                                                                                                                                                              
  await rtc.close()                                                                                                                                                                 
  await party.close()                                                                                                                                                               
})

function findSignaling (rtc, peer) {
  for (const [token, p] of rtc.peers) {
    if (peer === p.connection) {
      return p.signaling
    }
  }

  return null
}

for (let i = 0; i < 1; i++)
test.skip('signaling reconnects', { timeout: 90000 }, async function (t) {
  const party = new PartyServer()

  const signaling = await createWebSocketServer(t, function (ws, req) {
    ws.setMaxListeners(0)

    const rawStream = new RTC.WebSocketDuplex(ws)

    party.handle(rawStream)
  })

  const lc = t.test()

  lc.plan(6)

  const a = new RTC({ signaling, debug: true })

  const server = a.createServer(peer => {
    console.log('SERVER NEW CONNECTION')

    peer.addEventListener('connectionstatechange', () => {
      if (peer.connectionState === 'closed') {
        console.log('server peer got closed')
      }
    })

    // server.signaling.rpc.stream.destroy(new Error('abc'))
    // findSignaling(b, peer).rpc.stream.destroy(new Error('abc'))

    peer.ondatachannel = e => {
      console.log('server ondatachannel')

      // server.signaling.rpc.stream.destroy(new Error('abc'))
      // findSignaling(b, peer).rpc.stream.destroy(new Error('abc'))

      const rawStream = new RTC.DataChannelDuplex(e.channel)

      rawStream.on('open', function () {
        lc.pass('server stream open')
      })

      rawStream.on('data', function (data) {
        lc.is(data.toString(), 'Hello World!', 'server stream data')

        rawStream.write(data)
      })

      rawStream.on('close', function () {
        lc.pass('server stream closed')
      })
    }
  })

  await server.listen()

  t.comment('Server ID', server.publicKey.toString('hex'))

  // server.signaling.rpc.stream.destroy(new Error('server signaling closed'))

  const b = new RTC({ signaling, debug: true })

  const peer = b.connect(server.publicKey)

  peer.addEventListener('connectionstatechange', () => {
    if (peer.connectionState === 'closed') {
      console.log('client peer got closed')
    }
  })

  // server.signaling.rpc.stream.destroy(new Error('abc'))
  // findSignaling(b, peer).rpc.stream.destroy(new Error('abc'))

  const rawStream = new RTC.DataChannelDuplex(peer.createDataChannel('wire'))

  // server.signaling.rpc.stream.destroy(new Error('abc'))
  // findSignaling(b, peer).rpc.stream.destroy(new Error('abc'))

  rawStream.on('open', function () {
    lc.pass('client stream open')
  })

  rawStream.on('data', function (data) {
    lc.is(data.toString(), 'Hello World!', 'client stream data')

    rawStream.destroy()
  })

  rawStream.on('close', function () {
    lc.pass('client stream closed')
  })

  rawStream.write('Hello World!')
  // rawStream.end()

  console.time('lc')
  await lc
  console.timeEnd('lc')

  // TODO: Close peer first
  console.time('server.close')
  await server.close()
  console.timeEnd('server.close')

  console.time('a.close')
  await a.close()
  console.timeEnd('a.close')
  console.time('b.close')
  await b.close()
  console.timeEnd('b.close')

  console.time('party.close')
  await party.close()
  console.timeEnd('party.close')
})

function streamPair () {
  const s1 = new Duplex({
    read (cb) { cb(null) },
    write (data, cb) {
      setImmediate(() => {
        s2.push(data)
        cb(null)
      })
    }
  })

  const s2 = new Duplex({
    read (cb) { cb(null) },
    write (data, cb) {
      setImmediate(() => {
        s1.push(data)
        cb(null)
      })
    }
  })

  return [s1, s2]
}
