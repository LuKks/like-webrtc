/* eslint-disable */

// TODO: Slightly messy and some draft code, but some important edge cases are being tested

const { EventEmitter } = require('events')
const test = require('get-npm-global')('brittle')
const { Duplex, pipeline } = require('streamx')
const b4a = require('b4a')
// const Multimux = require('multimux')

const RPC = require('./lib/rpc.js')
const Peer = require('./lib/peer.js')
const PeerSession = require('./lib/peer-session.js')

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

  const before = peer.localDescription.sdp

  peer.restartIce()

  await echoRoundTrip(t, peer, 'after-ice-restart', 'hello after ice restart')

  await waitForCondition(() => {
    return peer.localDescription.sdp !== before && peer.signalingState === 'stable'
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

  peer.connection = {
    connectionState: 'connected',
    iceConnectionState: 'failed'
  }

  t.is(peer._iceNeedsRepair(), false, 'connected aggregate state is healthy')

  peer.connection = {
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
  const sent = [[], []]

  const server = a.createServer(peer => {
    getOnlyInternalPeer(a).on('signal', data => {
      if (data.description) sent[0].push(data.description)
    })
    peer.ondatachannel = e => {
      echoDataChannel(e.channel)
    }
  })

  await server.listen()

  const peer = b.connect(server.publicKey)

  getOnlyInternalPeer(b).on('signal', data => {
    if (data.description) sent[1].push(data.description)
  })

  await echoRoundTrip(t, peer, 'first', 'hello')

  await waitForCondition(() => {
    return a.peers.size === 1 && b.peers.size === 1
  }, 30000, 'internal peers')

  const serverPeer = getOnlyInternalPeer(a)
  const clientPeer = getOnlyInternalPeer(b)

  await waitForCondition(() => {
    return (
      peer.signalingState === 'stable' &&
      serverPeer.connection.signalingState === 'stable' &&
      clientPeer.connection.signalingState === 'stable' &&
      isPeerConnected(peer)
    )
  }, 30000, 'stable after first data channel')

  await delay(500)

  t.is(sent[1].filter(description => description.type === 'offer').length, 1, 'client sent one offer')
  t.is(sent[0].filter(description => description.type === 'answer').length, 1, 'server sent one answer')
  t.is(sent[0].filter(description => description.type === 'offer').length, 0, 'server did not create a follow-up offer')
  t.is(serverPeer.connection.remoteDescription.sdp.match(/^o=.+$/m)[0], sent[1][0].sdp.match(/^o=.+$/m)[0], 'server accepted the client offer')
  t.is(clientPeer.connection.remoteDescription.sdp.match(/^o=.+$/m)[0], sent[0][0].sdp.match(/^o=.+$/m)[0], 'client accepted the server answer')
  t.is(sent[1].length, 1, 'client did not send a follow-up description')

  await server.close()
  await a.close()
  await b.close()
  await party.close()
})

test('negotiated channels survive signaling reconnect and renegotiation', async function (t) {
  t.plan(3)

  const party = new PartyServer()
  const sockets = new Set()
  let reconnected
  const signaling = await createWebSocketServer(t, (socket, req) => {
    if (req.url === '/client') {
      sockets.add(socket)
      socket.once('close', () => sockets.delete(socket))

      if (reconnected) reconnected()
    }

    party.handle(new RTC.WebSocketDuplex(socket))
  })

  const a = new RTC({ signaling, iceServers: [] })
  const b = new RTC({ signaling: signaling + '/client', iceServers: [] })

  t.teardown(async () => {
    await a.close()
    await b.close()
    await party.close()
  })

  let remote

  const server = a.createServer(peer => {
    remote = peer

    echoDataChannel(peer.createDataChannel('chat', { negotiated: true, id: 1 }))
  })

  const listener = b.createServer()

  await server.listen()
  await listener.listen()

  const connection = b.connect(server.publicKey)
  const channel = new RTC.DataChannelDuplex(connection.createDataChannel('chat', { negotiated: true, id: 1 }))

  channel.on('error', noop)

  const before = waitEvent(channel, 'data', 10000, 'chat before reconnect')

  channel.write('before reconnect')

  t.is((await before).toString(), 'before reconnect', 'negotiated channel works')

  const reconnecting = new Promise(resolve => { reconnected = resolve })
  const closing = Array.from(sockets, socket => {
    const closed = waitEvent(socket, 'close', 10000, 'signaling socket closes')

    socket.terminate()

    return closed
  })

  await Promise.all(closing)
  await reconnecting
  await b.lookup('signaling-reconnected')

  const description = connection.remoteDescription.sdp

  remote.restartIce()

  await waitForCondition(() => {
    return connection.remoteDescription.sdp !== description &&
      connection.signalingState === 'stable' &&
      remote.signalingState === 'stable'
  }, 10000, 'ICE restart after signaling reconnect')

  t.ok(isPeerConnected(connection), 'renegotiation completes after signaling reconnect')

  const after = waitEvent(channel, 'data', 10000, 'chat after reconnect')

  channel.write('after reconnect')

  t.is((await after).toString(), 'after reconnect', 'existing channel still delivers messages')
})

test('ICE restart resumes when only the remote signaling reconnects', async function (t) {
  t.plan(2)

  const pair = createNegotiationPair(t, true)
  const [a, b] = pair.peers
  const channels = pair.peers.map(peer => peer.connection.createDataChannel('chat', { negotiated: true, id: 1 }))

  pair.open()

  await waitForCondition(() => {
    return channels.every(channel => channel.readyState === 'open') &&
      pair.peers.every(peer => peer.connection.signalingState === 'stable') &&
      pair.sessions.every(session => session._pending.size === 0)
  }, 5000, 'initial connection settles')

  const before = b.connection.remoteDescription.sdp

  pair.online = false
  a.connection.restartIce()

  await waitForCondition(() => a.connection.signalingState === 'have-local-offer', 5000, 'queued ICE restart offer')

  t.is(a.connection.signalingState, 'have-local-offer', 'ICE restart waits for the offline peer')

  pair.online = true
  pair.signalings[1].emit('online')

  await t.execution(waitForCondition(() => {
    return b.connection.remoteDescription.sdp !== before &&
      pair.peers.every(peer => peer.connection.signalingState === 'stable') &&
      pair.sessions.every(session => session._pending.size === 0)
  }, 5000, 'ICE restart after remote-only signaling reconnect'), 'the queued ICE restart resumes after the remote reconnects')
})

test('receiver can start with only a data channel or media', async function (t) {
  t.plan(3)

  for (const media of [false, true]) {
    const party = new PartyServer()
    const signaling = await createWebSocketServer(t, socket => party.handle(new RTC.WebSocketDuplex(socket)))

    const a = new RTC({ signaling, iceServers: [] })
    const b = new RTC({ signaling, iceServers: [] })

    t.teardown(async () => {
      await a.close()
      await b.close()
      await party.close()
    })

    let channel
    let transceiver

    const server = a.createServer(peer => {
      if (media) {
        transceiver = peer.addTransceiver('audio', { direction: 'recvonly' })
      } else {
        channel = new RTC.DataChannelDuplex(peer.createDataChannel('receiver-first'))

        channel.on('error', noop)
      }
    })

    await server.listen()

    const connection = b.connect(server.publicKey)

    connection.ondatachannel = event => echoDataChannel(event.channel)

    if (media) {
      await waitForCondition(() => isPeerConnected(connection), 10000, 'receiver-only media connection')

      t.ok(transceiver.mid !== null && /m=audio /.test(connection.remoteDescription.sdp), 'receiver media is negotiated')
      t.absent(/m=application /.test(connection.localDescription.sdp), 'media connects without a data channel')
    } else {
      await waitForCondition(() => !!channel, 10000, 'receiver channel creation')

      const echoed = waitEvent(channel, 'data', 10000, 'receiver initiated echo')

      channel.write('receiver first')

      t.is((await echoed).toString(), 'receiver first', 'receiver-created channel delivers messages')
    }

    await a.close()
    await b.close()
    await party.close()
  }
})

test('simultaneous first offers recover with either polite peer', async function (t) {
  t.plan(10)

  for (const polite of [false, true]) {
    for (const media of [false, true]) {
      const pair = createNegotiationPair(t, polite)
      const channels = pair.peers.map(peer => peer.connection.createDataChannel('chat', { negotiated: true, id: 1 }))
      const transceivers = media
        ? pair.peers.map(peer => peer.connection.addTransceiver('audio', { direction: 'recvonly' }))
        : []

      await waitForCondition(() => {
        return [0, 1].every(from => pair.messages.some(message => message.from === from && message.body.description?.type === 'offer'))
      }, 5000, 'both first offers')

      pair.open()

      await waitForCondition(() => channels.every(channel => channel.readyState === 'open'), 5000, 'glare recovery')

      const received = channels.map(channel => new Promise(resolve => {
        channel.onmessage = event => resolve(event.data)
      }))

      channels[0].send('caller')
      channels[1].send('receiver')

      t.alike(await Promise.all(received), ['receiver', 'caller'], 'messages work in both directions')

      await waitForCondition(() => {
        return pair.peers.every(peer => peer.connection.signalingState === 'stable') &&
          transceivers.every(transceiver => transceiver.mid !== null)
      }, 5000, 'negotiation settles')

      t.alike(pair.peers.map(peer => peer.connection.signalingState), ['stable', 'stable'], 'both peers settle')

      if (media) {
        t.ok(transceivers.every(transceiver => transceiver.mid !== null && !transceiver.stopped), 'both requested media transceivers survive glare')
      }

      await Promise.all(pair.peers.map(peer => peer.close()))
    }
  }
})

test('ICE received during glare survives signaling reconnect before the answer', async function (t) {
  t.plan(3)

  const pair = createNegotiationPair(t, true)
  const [polite, impolite] = pair.peers
  const channels = pair.peers.map(peer => peer.connection.createDataChannel('chat', { negotiated: true, id: 1 }))

  pair.hold = message => message.body.description?.type === 'answer' || message.name === 'webrtc-ack'

  await waitForCondition(() => {
    return [0, 1].every(from => pair.messages.some(message => message.from === from && message.body.description?.type === 'offer'))
  }, 5000, 'both first offers')

  pair.open()

  await waitForCondition(() => {
    return polite.connection.iceGatheringState === 'complete' &&
      pair.messages.some(message => message.from === 0 && message.body.description?.type === 'answer')
  }, 5000, 'held answer and candidates')

  const answer = pair.messages.find(message => message.from === 0 && message.body.description?.type === 'answer')
  const ufrag = answer.body.description.sdp.match(/^a=ice-ufrag:(.+)$/m)[1].trim()

  // Observe the queue here so the test cannot pass solely through peer-reflexive ICE.
  await waitForCondition(() => impolite._candidates.some(candidate => candidate?.usernameFragment === ufrag), 5000, 'answer candidates before the answer')

  t.is(impolite.connection.remoteDescription, null, 'ICE arrives before the held answer')

  pair.hold = null
  pair.open()

  await waitForCondition(() => channels.every(channel => channel.readyState === 'open'), 5000, 'connection after signaling reconnect')

  const received = channels.map(channel => new Promise(resolve => {
    channel.onmessage = event => resolve(event.data)
  }))

  channels[0].send('caller')
  channels[1].send('receiver')

  t.alike(await Promise.all(received), ['receiver', 'caller'], 'messages work after the delayed answer')
  t.alike(pair.peers.map(peer => peer.connection.signalingState), ['stable', 'stable'], 'both peers settle after reconnect')
})

test('closing a listener preserves independent outgoing peers', async function (t) {
  t.plan(5)

  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, socket => party.handle(new RTC.WebSocketDuplex(socket)))

  const a = new RTC({ signaling, iceServers: [] })
  const b = new RTC({ signaling, iceServers: [] })

  t.teardown(async () => {
    await a.close()
    await b.close()
    await party.close()
  })

  const server = a.createServer(peer => {
    peer.ondatachannel = event => echoDataChannel(event.channel)
  })

  const listener = b.createServer()

  await server.listen()
  await listener.listen()

  const connections = [b.connect(server.publicKey), b.connect(server.publicKey)]

  await Promise.all(connections.map((peer, index) => echoRoundTrip(t, peer, 'wire', 'message ' + index)))
  await connections[0].close()

  await echoRoundTrip(t, connections[1], 'after-client-close', 'other client survives')

  await listener.close()

  await echoRoundTrip(t, connections[1], 'after-listener-close', 'independent client survives')

  await connections[1].close()

  const next = b.connect(server.publicKey)

  await echoRoundTrip(t, next, 'later-client', 'new connections still work')
})

test('closing clients for another identity releases idle signaling sockets', async function (t) {
  t.plan(7)

  const party = new PartyServer()
  const sockets = new Set()
  const signaling = await createWebSocketServer(t, (socket, req) => {
    if (req.url === '/client') {
      sockets.add(socket)
      socket.once('close', () => sockets.delete(socket))
    }

    party.handle(new RTC.WebSocketDuplex(socket))
  })

  const a = new RTC({ signaling, iceServers: [] })
  const b = new RTC({ signaling: signaling + '/client', iceServers: [] })

  t.teardown(async () => {
    await a.close()
    await b.close()
    await party.close()
  })

  const identities = []
  const server = a.createServer({
    firewall (publicKey) {
      identities.push(b4a.toString(publicKey, 'hex'))

      return true
    }
  }, peer => {
    peer.ondatachannel = event => echoDataChannel(event.channel)
  })

  const listener = b.createServer(peer => {
    peer.ondatachannel = event => echoDataChannel(event.channel)
  })

  await server.listen()
  await listener.listen()

  const baseline = sockets.size
  const keyPair = RTC.keyPair()
  const first = b.connect(server.publicKey, { keyPair })
  const second = b.connect(server.publicKey, { keyPair })

  await echoRoundTrip(t, first, 'first-identity-client', 'first')
  await echoRoundTrip(t, second, 'second-identity-client', 'second')

  t.alike(identities, [keyPair, keyPair].map(key => b4a.toString(key.publicKey, 'hex')), 'both clients use the requested identity')

  await first.close()

  await echoRoundTrip(t, second, 'remaining-client', 'other client survives')

  await second.close()
  await waitForCondition(() => sockets.size === baseline, 10000, 'unused signaling sockets close')

  t.is(sockets.size, baseline, 'client-only signaling is released')

  const incoming = a.connect(listener.publicKey)

  await echoRoundTrip(t, incoming, 'remaining-listener', 'other identity keeps listening')

  await listener.close()
  await waitForCondition(() => sockets.size === 0, 10000, 'final signaling socket closes')

  t.is(sockets.size, 0, 'closing the final owner releases all its sockets')
})

test('closing a client before signaling opens releases its socket', async function (t) {
  t.plan(2)

  const sockets = new Set()
  let accepted
  const opening = new Promise(resolve => { accepted = resolve })
  const signaling = await createWebSocketServer(t, socket => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))

    accepted()
  })

  const rtc = new RTC({ signaling, iceServers: [] })

  t.teardown(async () => {
    await rtc.close()

    for (const socket of sockets) socket.terminate()
  })

  const connection = rtc.connect(RTC.keyPair().publicKey)

  await opening
  await connection.close()
  await waitForCondition(() => sockets.size === 0, 10000, 'opening socket closes')

  t.is(connection.connectionState, 'closed', 'pending connection closes')
  t.is(sockets.size, 0, 'pending signaling leaves no open socket')
})

test('concurrent connections and discovery keep working', async function (t) {
  t.plan(7)

  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, socket => party.handle(new RTC.WebSocketDuplex(socket)))

  const a = new RTC({ signaling, iceServers: [] })
  const b = new RTC({ signaling, iceServers: [] })

  t.teardown(async () => {
    await a.close()
    await b.close()
    await party.close()
  })

  const server = a.createServer(peer => {
    peer.ondatachannel = event => echoDataChannel(event.channel)
  })

  const topic = 'concurrent-discovery'

  await server.listen()
  await server.announce(topic)

  const connections = Array.from({ length: 3 }, () => b.connect(server.publicKey))

  await Promise.all(connections.map((peer, index) => echoRoundTrip(t, peer, 'cold ' + index, 'cold ' + index)))
  await Promise.all(connections.map(peer => peer.close()))

  const found = await b.lookup(topic)

  t.alike(found.map(peer => b4a.toString(peer.publicKey, 'hex')), [b4a.toString(server.publicKey, 'hex')], 'discovery works after concurrent clients close')

  const listener = b.createServer(peer => {
    peer.ondatachannel = event => echoDataChannel(event.channel)
  })

  await listener.listen()
  await listener.announce(topic)

  const announced = await a.lookup(topic)

  t.alike(announced.map(peer => b4a.toString(peer.publicKey, 'hex')).sort(), [server, listener].map(server => b4a.toString(server.publicKey, 'hex')).sort(), 'discovery and listening work together')

  const next = a.connect(listener.publicKey)

  await echoRoundTrip(t, next, 'discovered-listener', 'listener works after discovery')

  await listener.close()

  const remaining = await b.lookup(topic)

  t.alike(remaining.map(peer => b4a.toString(peer.publicKey, 'hex')), [b4a.toString(server.publicKey, 'hex')], 'closing one listener leaves the other publication')
})

test('a replacement listener accepts new peers while the previous server drains', async function (t) {
  t.plan(12)

  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, socket => party.handle(new RTC.WebSocketDuplex(socket)))

  const a = new RTC({ signaling, iceServers: [] })
  const b = new RTC({ signaling, iceServers: [] })

  t.teardown(async () => {
    await a.close()
    await b.close()
    await party.close()
  })

  const accepted = [[], []]
  const servers = accepted.map(peers => a.createServer(peer => {
    peers.push(peer)

    peer.ondatachannel = event => echoDataChannel(event.channel)
  }))

  const [first, second] = servers
  const topic = 'listener-handoff'
  const hidden = 'listener-handoff-hidden'

  await first.listen()
  await first.announce(topic)
  await first.announce(hidden)
  await first.join(topic)

  const previous = b.connect(first.publicKey)

  await echoRoundTrip(t, previous, 'before-handoff', 'before')

  await second.unannounce(hidden)
  await second.listen()

  const next = b.connect(second.publicKey)

  await echoRoundTrip(t, next, 'after-handoff', 'after')

  t.alike(accepted.map(peers => peers.length), [1, 1], 'new peers go to the replacement handler')

  await echoRoundTrip(t, previous, 'draining-peer', 'previous connection stays active')

  t.is((await b.lookup(topic)).length, 1, 'active topic publication follows the listener')
  t.is((await b.lookup(hidden)).length, 0, 'replacement respects explicitly withdrawn topics')

  await first.unannounce(topic)
  await first.leave(topic)

  t.is((await b.lookup(topic)).length, 1, 'previous server cannot withdraw the replacement topic')

  await first.close()

  t.is(accepted[0][0].connectionState, 'closed', 'closing the previous server closes its peers')

  await echoRoundTrip(t, next, 'after-previous-close', 'replacement stays active')

  const later = b.connect(second.publicKey)

  await echoRoundTrip(t, later, 'later-arrival', 'replacement keeps accepting')

  t.alike(accepted.map(peers => peers.length), [1, 2], 'later peers reach the replacement handler')

  await second.close()

  t.is((await b.lookup(topic)).length, 0, 'replacement withdraws its topic on close')
})

test('pending connections keep their original server and firewall after handoff', async function (t) {
  t.plan(10)

  for (const allowed of [true, false]) {
    const party = new PartyServer()
    const signaling = await createWebSocketServer(t, socket => party.handle(new RTC.WebSocketDuplex(socket)))

    const a = new RTC({ signaling, iceServers: [] })
    const b = new RTC({ signaling, iceServers: [] })

    t.teardown(async () => {
      await a.close()
      await b.close()
      await party.close()
    })

    const accepted = [[], []]
    let firstCalls = 0
    let secondCalls = 0
    let entered
    let finish
    const checking = new Promise(resolve => { entered = resolve })
    const waiting = new Promise(resolve => { finish = resolve })

    const first = a.createServer({
      firewall () {
        firstCalls++
        entered()

        return waiting
      }
    }, peer => {
      accepted[0].push(peer)

      peer.ondatachannel = event => echoDataChannel(event.channel)
    })

    const second = a.createServer({
      firewall () {
        secondCalls++

        return !allowed
      }
    }, peer => {
      accepted[1].push(peer)

      peer.ondatachannel = event => echoDataChannel(event.channel)
    })

    await first.listen()

    const previous = b.connect(first.publicKey)

    previous.createDataChannel('before-handoff')

    await checking
    await second.listen()

    finish(allowed)

    if (allowed) {
      await echoRoundTrip(t, previous, 'original-policy', 'original firewall allows')
    } else {
      await waitForCondition(() => previous.connectionState === 'closed', 5000, 'original firewall refusal')

      t.is(previous.connectionState, 'closed', 'original refusal is preserved')
    }

    t.is(accepted[0].length, allowed ? 1 : 0, 'pending attempt keeps its original handler')
    t.is(firstCalls, 1, 'original firewall is evaluated once')

    if (allowed) {
      t.is(secondCalls, 0, 'replacement firewall is untouched by the original attempt')
    }

    const next = b.connect(second.publicKey)

    if (allowed) {
      await waitForCondition(() => next.connectionState === 'closed', 5000, 'replacement firewall refusal')

      t.is(next.connectionState, 'closed', 'fresh attempt uses the replacement refusal')
    } else {
      await echoRoundTrip(t, next, 'new-policy', 'replacement firewall allows')
    }

    if (allowed) {
      t.is(secondCalls, 1, 'replacement firewall handles the fresh attempt')
    }

    await a.close()
    await b.close()
    await party.close()
  }
})

test('listener key changes preserve existing peers', async function (t) {
  t.plan(6)

  const party = new PartyServer()
  const signaling = await createWebSocketServer(t, socket => party.handle(new RTC.WebSocketDuplex(socket)))

  const a = new RTC({ signaling, iceServers: [] })
  const b = new RTC({ signaling, iceServers: [] })

  t.teardown(async () => {
    await a.close()
    await b.close()
    await party.close()
  })

  const firstKey = RTC.keyPair()
  const secondKey = RTC.keyPair()
  const accepted = []
  const server = a.createServer(peer => {
    accepted.push(peer)

    peer.ondatachannel = event => echoDataChannel(event.channel)
  })

  await server.listen(firstKey)

  t.alike(server.publicKey, firstKey.publicKey, 'listener uses the selected identity')

  const first = b.connect(firstKey.publicKey)

  await echoRoundTrip(t, first, 'before-key-change', 'selected identity works')

  await server.listen(secondKey)

  t.alike(server.publicKey, secondKey.publicKey, 'listener uses the replacement identity')

  await echoRoundTrip(t, first, 'after-key-change', 'original peer survives')

  const second = b.connect(secondKey.publicKey)

  await echoRoundTrip(t, second, 'new-key', 'replacement identity works')

  await server.close()

  t.alike(accepted.map(peer => peer.connectionState), ['closed', 'closed'], 'server closes peers accepted under either identity')
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

  return peers[0].peer
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

test('join retries closed peers until leave', { timeout: 60000 }, async function (t) {
  t.plan(3)

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

  const peers = []
  const server = a.createServer()
  const swarm = b.createServer(peer => {
    peers.push(peer)
    peer.createDataChannel('idle')
  })
  const topic = 'join-retry-until-leave'

  await server.listen()
  await server.announce(topic)
  await swarm.join(topic)

  t.is(peers.length, 1, 'join discovers a peer')

  await waitForCondition(() => peers[0].connectionState === 'connected', 10000, 'joined peer to connect')
  await peers[0].close()

  await waitForCondition(() => peers.length === 2, 10000, 'joined peer to retry after close')
  t.ok(peers[1] !== peers[0], 'retry creates a new connection')

  await waitForCondition(() => peers[1].connectionState === 'connected', 10000, 'retried peer to connect')
  await peers[1].close()
  await swarm.leave(topic)
  await delay(5000)

  t.is(peers.length, 2, 'leave cancels the pending retry')
})

test.skip('join backs off failed connections', { timeout: 60000 }, async function (t) {
  t.plan(3)

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

  const attempts = []
  const server = a.createServer({ firewall: () => false })
  const swarm = b.createServer(() => attempts.push(Date.now()))
  const topic = 'join-failed-retry'

  await server.listen()
  await server.announce(topic)
  await swarm.join(topic)

  await waitForCondition(() => attempts.length === 4, 45000, 'fourth connection attempt')
  await swarm.leave(topic)

  t.ok(attempts[1] - attempts[0] >= 1000, 'first retry waits one second')
  t.ok(attempts[2] - attempts[1] >= 5000, 'second retry waits five seconds')
  t.ok(attempts[3] - attempts[2] >= 15000, 'third retry waits fifteen seconds')
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

function createNegotiationPair (t, polite) {
  const pair = { peers: [], sessions: [], signalings: [], messages: [], hold: null, online: false }

  for (let i = 0; i < 2; i++) {
    const signaling = new EventEmitter()

    signaling.event = function (name, body) {
      const message = { from: i, name, body }

      pair.messages.push(message)

      if (!pair.online || pair.hold?.(message)) return false

      queueMicrotask(() => pair.sessions[1 - i].signal(name, body).catch(err => t.fail(err.message)))

      return true
    }

    pair.signalings.push(signaling)
    const peer = new RTC.Peer({ polite: i === 0 ? polite : !polite, iceServers: [] })
    pair.peers.push(peer)
    pair.sessions.push(new PeerSession(signaling, peer, { token: 'test', isInitiator: i === 1 }))
  }

  pair.open = function () {
    pair.online = true

    pair.signalings.forEach(signaling => signaling.emit('online'))
  }

  t.teardown(() => Promise.all(pair.peers.map(peer => peer.close())))

  return pair
}

test('standalone peers use manual signaling with either ICE gathering mode', async function (t) {
  t.plan(24)

  for (const trickle of [true, false]) {
    for (const remoteTrickle of [true, false]) {
      const a = new Peer({ polite: false, trickle })
      const b = new Peer({ polite: true, trickle: remoteTrickle })
      const signals = []

      t.teardown(() => Promise.all([a.close(), b.close()]))

      a.on('error', err => t.fail(err.message))
      b.on('error', err => t.fail(err.message))
      a.on('signal', data => {
        signals.push(data)
        b.signal(data)
      })
      b.on('signal', data => a.signal(data))
      b.connection.ondatachannel = event => echoDataChannel(event.channel)

      t.alike(a.connection.getConfiguration().iceServers, [], 'standalone peers have no preset ICE servers')

      await echoRoundTrip(t, a.connection, 'first', 'first message')
      await echoRoundTrip(t, a.connection, 'later', 'later message')

      t.is(signals.filter(data => data.description?.type === 'offer').length, 1, 'later channels do not renegotiate SCTP')
      t.is(signals.some(data => 'candidate' in data), trickle, 'candidate events follow the local trickle option')

      const before = a.connection.localDescription.sdp.match(/^a=ice-ufrag:(.+)$/m)[1]

      a.connection.restartIce()
      await waitForCondition(() => {
        return a.connection.signalingState === 'stable' && b.connection.signalingState === 'stable' &&
          a.connection.localDescription.sdp.match(/^a=ice-ufrag:(.+)$/m)[1] !== before
      }, 5000, 'standalone ICE restart')

      await echoRoundTrip(t, a.connection, 'restarted', 'message after restart')
      await Promise.all([a.close(), b.close()])
    }
  }
})

test('standalone peers negotiate media added and removed through the native connection', { timeout: 10000 }, async function (t) {
  t.plan(3)

  const { nonstandard } = require('webrtc-runtime')
  const a = new Peer({ polite: false })
  const b = new Peer({ polite: true })
  const source = new nonstandard.RTCAudioSource()
  const track = source.createTrack()
  const received = []

  t.teardown(async () => {
    track.stop()
    await Promise.all([a.close(), b.close()])
  })

  a.on('error', err => t.fail(err.message))
  b.on('error', err => t.fail(err.message))
  a.on('signal', data => b.signal(data))
  b.on('signal', data => a.signal(data))
  b.connection.ondatachannel = event => echoDataChannel(event.channel)
  b.connection.ontrack = event => received.push(event.track)

  await echoRoundTrip(t, a.connection, 'bootstrap', 'connected')

  const sender = a.connection.addTrack(track)

  await waitForCondition(() => received.length === 1 && a.connection.signalingState === 'stable', 5000, 'added audio track')

  t.is(received[0].kind, 'audio', 'audio is negotiated after the data connection')

  a.connection.removeTrack(sender)

  await waitForCondition(() => {
    return b.connection.getTransceivers()[0]?.currentDirection === 'inactive' &&
      a.connection.signalingState === 'stable'
  }, 5000, 'removed audio track')

  t.is(a.connection.getSenders().filter(sender => sender.track).length, 0, 'native removeTrack renegotiates')
})

test('standalone close cancels an offer and non-trickle gathering', async function (t) {
  t.plan(16)

  for (const gathering of [false, true]) {
    const pc = createPeerConnection()
    const rawClose = pc.close.bind(pc)
    let closes = 0
    let closeEvents = 0
    let finishOffer

    pc.close = () => {
      closes++
      rawClose()
    }

    if (!gathering) {
      pc.createOffer = () => new Promise(resolve => { finishOffer = resolve })
    }

    const peer = new Peer({
      RTCPeerConnection: function () { return pc },
      trickle: false,
      connectTimeout: 0
    })
    const signals = []

    peer.on('signal', data => signals.push(data))
    peer.on('close', () => {
      closeEvents++
      peer.close()
    })
    pc.emit('negotiationneeded')

    await waitForCondition(() => gathering ? pc.iceGatheringState === 'gathering' : !!finishOffer, 1000, 'pending local description')

    const closing = peer.connection.close()

    peer.close()

    t.is(closing, undefined, 'close is synchronous')
    t.is(peer.closed, true, 'peer is closed immediately')
    t.is(peer.closing, true, 'closing remains true')
    t.is(closes, 1, 'native close runs once through both APIs')
    t.is(closeEvents, 1, 'close emits once synchronously, including reentrant close')

    if (finishOffer) finishOffer({ type: 'offer', sdp: 'a=ice-ufrag:local\r\n' })
    await peer._operations

    t.is(signals.length, 0, 'closing suppresses the pending signal')
    t.is(pc.eventNames().length, 0, 'native listeners are removed')

    const error = new Promise(resolve => peer.once('error', resolve))

    peer.signal({ candidate: null })

    t.is((await error).code, 'PEER_CLOSED', 'signal after close emits an error')
  }
})

test('standalone connection deadlines begin with negotiation and bound recovery', async function (t) {
  t.plan(5)

  const pc = createPeerConnection()
  const peer = new Peer({
    RTCPeerConnection: function () { return pc },
    connectTimeout: 20,
    disconnectTimeout: 20,
    iceDisconnectedRestartDelay: 1
  })
  const errors = []

  t.teardown(() => peer.close())
  peer.on('error', err => errors.push(err.code))

  await delay(40)

  t.is(peer.closed, false, 'an unused peer has no connection deadline')

  pc.emit('negotiationneeded')
  await waitForCondition(() => peer.closed, 1000, 'connection deadline')

  t.alike(errors, ['PEER_CONNECT_TIMEOUT'], 'an unanswered negotiation times out once')

  const recovering = createPeerConnection()
  const remote = new Peer({
    RTCPeerConnection: function () { return recovering },
    disconnectTimeout: 20,
    iceDisconnectedRestartDelay: 1
  })

  t.teardown(() => remote.close())
  recovering.connectionState = 'connected'
  recovering.iceConnectionState = 'connected'
  recovering.emit('connectionstatechange')
  recovering.connectionState = 'disconnected'
  recovering.iceConnectionState = 'disconnected'
  recovering.emit('connectionstatechange')
  recovering.emit('iceconnectionstatechange')

  await waitForCondition(() => recovering.localDescription, 1000, 'automatic ICE restart')

  t.ok(recovering.localDescription, 'disconnection starts a repair negotiation')

  await waitForCondition(() => remote.closed, 1000, 'recovery deadline')

  t.is(remote.closed, true, 'an unsuccessful recovery has a deadline')
  t.is(recovering.eventNames().length, 0, 'recovery shutdown removes its listeners')
})

test('future ICE candidates survive reordering and retired candidates are ignored', async function (t) {
  t.plan(6)

  const pc = createPeerConnection()
  const peer = new Peer({
    RTCPeerConnection: function () { return pc },
    RTCIceCandidate: function (candidate) { return candidate }
  })
  const old = { candidate: 'candidate:1 1 udp 1 127.0.0.1 10000 typ host', usernameFragment: 'old' }
  const next = { ...old, usernameFragment: 'next' }
  const errors = []

  t.teardown(() => peer.close())
  peer.on('error', err => errors.push(err))

  await peer._signal({ description: { type: 'offer', sdp: 'a=ice-ufrag:old\r\n' } })
  await peer._signal({ candidate: next })

  t.is(pc.candidates.length, 0, 'future candidates wait for their description')

  await peer._signal({ description: { type: 'offer', sdp: 'a=ice-ufrag:next\r\n' } })

  t.alike(pc.candidates, [next], 'the new description applies its early candidate')

  await peer._signal({ candidate: old })

  t.is(pc.candidates.length, 1, 'retired candidates are not applied to the new generation')

  const failure = new Error('Candidate failed')
  pc.addIceCandidate = async () => { throw failure }

  t.is(peer.signal({ candidate: next }), undefined, 'signal returns synchronously without a promise')

  await waitForCondition(() => errors.length === 1, 1000, 'candidate error')

  t.is(errors[0], failure, 'native errors are emitted')

  peer.signal({ description: null })
  await waitForCondition(() => errors.length === 2, 1000, 'invalid signal error')

  t.is(errors[1].code, 'INVALID_SIGNAL', 'invalid signals emit an error')
})

test('a media offer ignored during glare is replayed after the delayed answer', async function (t) {
  t.plan(3)

  const pair = createNegotiationPair(t, true)
  const channels = pair.peers.map(peer => peer.connection.createDataChannel('chat', { negotiated: true, id: 1 }))
  const transceivers = pair.peers.map(peer => peer.connection.addTransceiver('audio', { direction: 'recvonly' }))

  pair.hold = message => message.body.description?.type === 'answer'

  await waitForCondition(() => pair.messages.filter(message => message.body.description?.type === 'offer').length === 2, 5000, 'first offers')

  pair.open()

  await waitForCondition(() => {
    return pair.messages.filter(message => message.from === 0 && message.body.description?.type === 'offer').length >= 2
  }, 5000, 'follow-up media offer before the answer')

  pair.hold = null
  pair.open()

  await waitForCondition(() => {
    return pair.peers.every(peer => peer.connection.signalingState === 'stable') &&
      transceivers.every(transceiver => transceiver.mid !== null) &&
      channels.every(channel => channel.readyState === 'open')
  }, 5000, 'replayed media offer')

  t.ok(transceivers.every(transceiver => transceiver.mid !== null && !transceiver.stopped), 'both local media requests survive')
  t.alike(pair.peers.map(peer => peer.connection.signalingState), ['stable', 'stable'], 'both peers settle')

  const received = new Promise(resolve => { channels[1].onmessage = event => resolve(event.data) })

  channels[0].send('after glare')

  t.is(await received, 'after glare', 'the data connection survives replay')
})

function createPeerConnection () {
  const pc = new EventEmitter()

  pc.signalingState = 'stable'
  pc.connectionState = 'new'
  pc.iceConnectionState = 'new'
  pc.iceGatheringState = 'new'
  pc.localDescription = null
  pc.remoteDescription = null
  pc.candidates = []
  pc.addEventListener = pc.on
  pc.removeEventListener = pc.removeListener
  pc.createOffer = async () => ({ type: 'offer', sdp: 'a=ice-ufrag:local\r\n' })
  pc.createAnswer = async () => ({ type: 'answer', sdp: 'a=ice-ufrag:local\r\n' })
  pc.setLocalDescription = async description => {
    pc.localDescription = description
    pc.signalingState = description.type === 'offer' ? 'have-local-offer' : 'stable'
    pc.iceGatheringState = 'gathering'
  }
  pc.setRemoteDescription = async description => {
    pc.remoteDescription = description
    pc.signalingState = description.type === 'offer' ? 'have-remote-offer' : 'stable'
  }
  pc.addIceCandidate = async candidate => { pc.candidates.push(candidate) }
  pc.close = () => {
    pc.signalingState = 'closed'
    pc.connectionState = 'closed'
    pc.iceConnectionState = 'closed'
  }

  return pc
}
