require('./lib/sodium.js')

const EventEmitter = require('events')
const RPC = require('./lib/rpc.js')
const SecretStream = require('@hyperswarm/secret-stream')

module.exports = class Signaling extends EventEmitter {
  constructor (rtc, keyPair, opts = {}) {
    super()

    if (rtc.closing || rtc.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    this.rtc = rtc
    this.rtc.signalings.add(this)

    this.keyPair = keyPair

    // Consumer should implement them
    this._connect = req => {}
    this._negotiation = req => {}
    this._ice = req => {}
    this._ack = req => {}

    this.rpc = null
    this.closed = false
    this.closePromise = null

    this._reconnectTimeout = null
    this._reconnectBackoff = exponentialBackoff({ delay: 100, max: 6 })

    this._tag = opts._tag || null

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

    rpc._tag = this._tag

    this.rpc = rpc

    this.rpc.on('error', err => {
      if (this.closed || this.rtc.closing || this.rtc.closed) return
      if (this.rtc.debug) console.error(err)
    })

    this.rpc.respond('webrtc-connect', this._onConnect.bind(this))
    this.rpc.respond('webrtc-negotiation', this._onNegotiation.bind(this))
    this.rpc.respond('webrtc-ice', this._onIce.bind(this))
    this.rpc.respond('webrtc-ack', this._onAck.bind(this))

    this.rpc.stream.on('open', () => {
      if (this.closed || rpc.closed || rpc !== this.rpc) {
        return
      }

      if (this.rtc.debug) console.log('[Signaling] online')

      this._reconnectBackoff.reset()

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

    this.rtc.signalings.delete(this)
    this._clearReconnectTimeout()

    // This code is used by both clients and servers
    // Server-side peers are owned by the server Signaling instance so we close them
    // Client-side peers own their signalings so we don't close them here

    const peers = Array.from(this.rtc.peers.values())

    for (const peer of peers) {
      if (peer.signaling !== this) continue
      if (peer.isInitiator) continue

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

  _onConnect (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    return this._connect(req)
  }

  _onNegotiation (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    return this._negotiation(req)
  }

  _onIce (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    return this._ice(req)
  }

  _onAck (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    return this._ack(req)
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
