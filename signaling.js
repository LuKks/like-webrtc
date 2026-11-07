require('./lib/sodium.js')

const EventEmitter = require('events')
const b4a = require('b4a')
const SecretStream = require('@hyperswarm/secret-stream')
const RPC = require('./lib/rpc.js')

module.exports = class Signaling extends EventEmitter {
  constructor (rtc, keyPair) {
    super()

    if (rtc.closing || rtc.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    this.rtc = rtc
    this.rtc.signalings.add(this)

    this.keyPair = keyPair
    this._users = new Set()
    this._server = null
    this._incoming = new Map()

    this.rpc = null
    this.closed = false
    this.closePromise = null

    this._reconnectTimeout = null
    this._reconnectBackoff = exponentialBackoff({ delay: 100, max: 6 })

    this._setup()
  }

  _setup () {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      return
    }

    if (this.rtc.debug) console.log('[Signaling] _setup')

    this._clearReconnectTimeout()

    const prev = this.rpc

    if (prev) {
      try {
        prev.destroy()
      } catch {}

      prev.close().catch(() => {})
    }

    const rawStream = this.rtc._createSignalingStream()
    const stream = new SecretStream(true, rawStream, {
      keyPair: this.keyPair,
      keepAlive: 5000
    })
    stream.setTimeout(15000)

    const rpc = new RPC(stream)

    this.rpc = rpc

    this.rpc.on('error', err => {
      if (this.closed || this.rtc.closing || this.rtc.closed) return
      if (this.rtc.debug) console.error(err)
    })

    this.rpc.respond('webrtc-connect', this._onConnect.bind(this))
    this.rpc.respond('webrtc-negotiation', this._onSignal.bind(this, 'webrtc-negotiation'))
    this.rpc.respond('webrtc-ice', this._onSignal.bind(this, 'webrtc-ice'))
    this.rpc.respond('webrtc-ack', this._onAck.bind(this))

    this.rpc.stream.on('open', () => {
      if (this.closed || rpc.closed || rpc !== this.rpc) {
        return
      }

      if (this.rtc.debug) console.log('[Signaling] online')

      this._reconnectBackoff.reset()

      for (const user of Array.from(this._users)) {
        if (user.signaling === this && user._onSignalingOnline) {
          user._onSignalingOnline()
        }
      }

      this.emit('online')
    })

    this.rpc.stream.on('close', () => {
      if (rpc !== this.rpc) {
        return
      }

      if (this.closed || this.rtc.closing || this.rtc.closed) {
        return
      }

      if (this.rtc.debug) console.log('[Signaling] offline')

      for (const user of Array.from(this._users)) {
        if (user.signaling === this && user._onSignalingOffline) {
          user._onSignalingOffline()
        }
      }

      this.emit('offline')

      this._reconnectTimeout = setTimeout(() => {
        this._reconnectTimeout = null

        if (this.closed || this.rtc.closing || this.rtc.closed) {
          return
        }

        if (this.rtc.debug) console.log('[Signaling] reconnecting')

        this._setup()
      }, this._reconnectBackoff.next())
    })
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

    this.closed = true
    this._users.clear()
    this._server = null
    this._clearIncoming()

    this.rtc.signalings.delete(this)
    this._clearReconnectTimeout()

    const peers = Array.from(this.rtc.peers.values())

    for (const peer of peers) {
      if (peer.signaling !== this) continue

      try {
        await peer.connection.close()
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }

    const rpc = this.rpc
    this.rpc = null

    if (rpc) {
      try {
        rpc.destroy()
      } catch {}

      try {
        await rpc.close()
      } catch {}
    }
  }

  event (method, body) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      return false
    }

    if (!this.rpc || this.rpc.stream.destroying) {
      this._setup()
    }

    if (!this.rpc || this.rpc.stream.destroying) {
      return false
    }

    this.rpc.event(method, body)

    return true
  }

  async request (method, body, opts = {}) {
    if (this.closed || this.rtc.closed || (this.rtc.closing && opts._allowClosing !== true)) {
      throw new Error('Signaling is closed')
    }

    if (!this.rpc || this.rpc.stream.destroying) {
      if (this.rtc.closing) {
        throw new Error('Signaling is closed')
      }

      this._setup()
    }

    if (!this.rpc || this.rpc.stream.destroying) {
      throw new Error('Signaling is closed')
    }

    return this.rpc.request(method, body)
  }

  _clearReconnectTimeout () {
    if (!this._reconnectTimeout) return

    clearTimeout(this._reconnectTimeout)
    this._reconnectTimeout = null
  }

  _retain (user) {
    this._users.add(user)
  }

  async _release (user) {
    this._users.delete(user)

    if (this._users.size === 0) {
      await this.close()
    }
  }

  _getPeer (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed || !req.body || typeof req.body.token !== 'string') {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const peer = this.rtc.peers.get(req.body.token)

    if (!peer) {
      return null
    }

    if (peer.signaling !== this || req.body.isInitiator === peer.isInitiator || !req.body.remotePublicKey || !b4a.equals(req.body.remotePublicKey, peer.remotePublicKey)) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    return peer
  }

  _getOrCreatePeer (req) {
    const peer = this._getPeer(req)

    if (peer) {
      if (peer.closed || peer.closing) {
        throw new RPC.Error('PEER_CONNECTION_FAILED')
      }

      return peer
    }

    if (req.body.isInitiator !== true || !req.body.remotePublicKey) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const token = req.body.token

    let incoming = this._incoming.get(token)

    if (incoming) {
      if (!b4a.equals(incoming.remotePublicKey, req.body.remotePublicKey)) {
        throw new RPC.Error('PEER_CONNECTION_FAILED')
      }

      return incoming.promise
    }

    const server = this._server

    if (!server || server.closed || server.signaling !== this) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    incoming = { server, remotePublicKey: req.body.remotePublicKey, promise: null }

    this._incoming.set(token, incoming)

    incoming.promise = Promise.resolve().then(() => this._acceptPeer(req, incoming))
    incoming.promise.catch(() => {})

    return incoming.promise
  }

  async _acceptPeer (req, incoming) {
    const token = req.body.token
    const server = incoming.server

    try {
      if (!(await server._firewall(incoming.remotePublicKey))) {
        throw new RPC.Error('PEER_CONNECTION_FAILED')
      }

      if (this.closed || this.rtc.closing || this.rtc.closed || server.closed || server.signaling !== this || this._incoming.get(token) !== incoming) {
        throw new RPC.Error('PEER_CONNECTION_FAILED')
      }

      server._createIncomingPeer(req)

      return this.rtc.peers.get(token)
    } finally {
      if (this._incoming.get(token) === incoming) {
        this._incoming.delete(token)
      }
    }
  }

  _clearIncoming (server) {
    for (const [token, incoming] of this._incoming) {
      if (server && incoming.server !== server) {
        continue
      }

      this._incoming.delete(token)
    }
  }

  async _onConnect (req) {
    const peer = await this._getOrCreatePeer(req)

    if (peer.closed || peer.closing) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }
  }

  async _onSignal (name, req) {
    const peer = await this._getOrCreatePeer(req)

    if (this.closed || this.rtc.closing || this.rtc.closed || !peer || peer.closed || peer.closing) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    return peer.signal(name, req.body)
  }

  _onAck (req) {
    const peer = this._getPeer(req)

    if (!peer || peer.closed || peer.closing) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    return peer.signal('webrtc-ack', req.body)
  }
}

function exponentialBackoff (opts = {}) {
  let attempt = 0

  return { next, reset }

  function next () {
    if (attempt === 0) {
      attempt++
      return 0
    }

    const exponent = Math.min(attempt - 1, opts.max || 0)
    const time = (opts.delay || 0) * Math.pow(2, exponent)

    attempt++

    return time
  }

  function reset () {
    attempt = 0
  }
}
