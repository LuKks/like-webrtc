const { RTCPeerConnection, RTCIceCandidate } = require('webrtc-runtime')
const b4a = require('b4a')
const Signaling = require('./signaling.js')
const Peer = require('./lib/peer.js')
const PeerSession = require('./lib/peer-session.js')
const crypto = require('hypercore-crypto')
const bs58 = maybeDefaultModule(require('bs58'))

const WebSocketDuplex = require('./duplex/websocket.js')
const DataChannelDuplex = require('./duplex/datachannel.js')
const SocketDuplex = require('./duplex/socket.js')

const Server = require('./server.js')
const Client = require('./client.js')

const ICE_SERVERS = [
  { urls: 'stun:turn.leet.ar:3478' },
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun.cloudflare.com:3478' },
  { urls: 'turn:turn.leet.ar:3478?transport=udp', username: 'us3rn4m3', credential: 'p4$$w0rd' },
  { urls: 'turn:turn.leet.ar:3478?transport=tcp', username: 'us3rn4m3', credential: 'p4$$w0rd' }
]

module.exports = class WebRTC {
  static Peer = Peer

  static WebSocketDuplex = WebSocketDuplex
  static DataChannelDuplex = DataChannelDuplex
  static SocketDuplex = SocketDuplex
  static WebStream = DataChannelDuplex // Compat

  static keyPair (seed) {
    seed = normalizeKey(seed)

    return crypto.keyPair(seed)
  }

  static randomBytes (size) {
    return crypto.randomBytes(size)
  }

  constructor (opts = {}) {
    this.RTCPeerConnection = opts.RTCPeerConnection || RTCPeerConnection
    this.RTCIceCandidate = opts.RTCIceCandidate || RTCIceCandidate

    this.keyPair = opts.keyPair || WebRTC.keyPair(opts.seed || crypto.randomBytes(32))

    this.signaling = opts.signaling || 'wss://party.leet.ar:41337'

    this.iceServers = opts.iceServers || ICE_SERVERS
    this.iceTransportPolicy = opts.iceTransportPolicy || 'all'

    this.trickle = opts.trickle !== false

    this.clients = new Set()
    this.servers = new Set()
    this.peers = new Map()
    this.signalings = new Set()

    this.debug = !!opts.debug

    this.closing = false
    this.closed = false
    this.closePromise = null
  }

  close () {
    if (this.closePromise) return this.closePromise

    this.closePromise = this._close()

    return this.closePromise
  }

  async _close () {
    if (this.closed) {
      return
    }

    this.closing = true

    const clients = Array.from(this.clients)

    for (const client of clients) {
      try {
        await client.close()
      } catch (err) {
        if (this.debug) console.error(err)
      }
    }

    await this._closeAllPeers()

    const servers = Array.from(this.servers)

    for (const server of servers) {
      try {
        await server.close()
      } catch (err) {
        if (this.debug) console.error(err)
      }
    }

    const signalings = Array.from(this.signalings)

    for (const signaling of signalings) {
      try {
        await signaling.close()
      } catch (err) {
        if (this.debug) console.error(err)
      }
    }

    await this._closeAllPeers()

    this.clients.clear()
    this.peers.clear()
    this.servers.clear()
    this.signalings.clear()

    this.closed = true
    this.closing = false
  }

  async _closeAllPeers () {
    const peers = Array.from(this.peers.values())

    for (const peer of peers) {
      try {
        await peer.connection.close()
      } catch (err) {
        if (this.debug) console.error(err)
      }
    }
  }

  get address () {
    return bs58.encode(this.keyPair.publicKey)
  }

  get publicKey () {
    return this.keyPair.publicKey
  }

  // TODO: Maybe signaling server could be a HTTPS server with EventSource
  _createSignalingStream () {
    if (this.closing || this.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    const rawStream = new WebSocketDuplex(new WebSocket(this.signaling)) // eslint-disable-line no-undef

    return rawStream
  }

  createServer (opts, handler) {
    if (this.closing || this.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    if (typeof opts === 'function') {
      handler = opts
      opts = {}
    }

    const server = new Server(this, opts)

    if (handler) {
      server.on('connection', handler)
    }

    return server
  }

  connect (remotePublicKey, opts = {}) {
    if (this.closing || this.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    remotePublicKey = normalizeKey(remotePublicKey)

    const client = new Client(this, remotePublicKey, opts)

    return client.connection
  }

  async announce (topic, opts = {}) {
    const keyPair = opts.keyPair || this.keyPair

    if (typeof topic !== 'string' || topic.length === 0) {
      throw new Error('Topic is required')
    }

    const timestamp = Date.now()
    const message = b4a.concat([
      b4a.from('party-announce'),
      b4a.from(topic),
      b4a.from(timestamp.toString())
    ])
    const signature = crypto.sign(message, keyPair.secretKey)

    const signaling = this._getPartySignaling(keyPair)

    return signaling.request('announce', {
      topic,
      publicKey: keyPair.publicKey,
      timestamp,
      signature
    })
  }

  async unannounce (topic, opts = {}) {
    const keyPair = opts.keyPair || this.keyPair

    if (typeof topic !== 'string' || topic.length === 0) {
      throw new Error('Topic is required')
    }

    const timestamp = Date.now()
    const message = b4a.concat([
      b4a.from('party-unannounce'),
      b4a.from(topic),
      b4a.from(timestamp.toString())
    ])
    const signature = crypto.sign(message, keyPair.secretKey)

    const signaling = this._getPartySignaling(keyPair)

    return signaling.request('unannounce', {
      topic,
      publicKey: keyPair.publicKey,
      timestamp,
      signature
    }, { _allowClosing: true })
  }

  async lookup (topic) {
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new Error('Topic is required')
    }

    const signaling = this._getPartySignaling()

    return signaling.request('lookup', { topic })
  }

  _getPartySignaling (keyPair) {
    const signaling = this._getSignaling(keyPair || this.keyPair)

    signaling._retain(this)

    return signaling
  }

  _getSignaling (keyPair) {
    for (const signaling of this.signalings) {
      if (!signaling.closed && b4a.equals(keyPair.publicKey, signaling.keyPair.publicKey)) {
        return signaling
      }
    }

    if (this.closing || this.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    return new Signaling(this, keyPair)
  }

  _createPeer (signaling, isInitiator, remotePublicKey, token) {
    if (this.closing || this.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    remotePublicKey = normalizeKey(remotePublicKey)

    const keyPair = signaling.keyPair || this.keyPair

    const peer = new Peer({
      polite: isPolitePeer(keyPair.publicKey, remotePublicKey),

      RTCPeerConnection: this.RTCPeerConnection,
      RTCIceCandidate: this.RTCIceCandidate,

      iceServers: this.iceServers,
      iceTransportPolicy: this.iceTransportPolicy,

      trickle: this.trickle,
      debug: this.debug
    })
    const session = new PeerSession(signaling, peer, { token, isInitiator, remotePublicKey })

    peer.connection.address = bs58.encode(keyPair.publicKey)
    peer.connection.publicKey = keyPair.publicKey
    peer.connection.remotePublicKey = remotePublicKey
    peer.connection.isInitiator = isInitiator

    peer.on('error', err => {
      if (this.debug) console.error(err)
    })

    peer.once('close', () => {
      if (this.peers.get(token) === session) {
        this.peers.delete(token)

        signaling._release(session).catch(err => {
          if (this.debug) console.error(err)
        })
      }
    })

    this.peers.set(token, session)

    signaling._retain(session)

    return peer
  }
}

function normalizeKey (input) {
  if (typeof input === 'string') {
    if (input.length === 64) return b4a.from(input, 'hex')
    if (input.length >= 26 && input.length <= 44) return bs58.decode(input)
  }

  return input
}

function isPolitePeer (localPublicKey, remotePublicKey) {
  return b4a.compare(b4a.from(localPublicKey), b4a.from(remotePublicKey)) > 0
}

function maybeDefaultModule (mod) {
  return mod.default ? mod.default : mod
}
