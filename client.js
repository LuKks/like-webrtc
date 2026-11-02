const { EventEmitter } = require('events')
const b4a = require('b4a')
const RPC = require('./lib/rpc.js')
const Signaling = require('./signaling.js')
const crypto = require('hypercore-crypto')

module.exports = class Client extends EventEmitter {
  constructor (rtc, remotePublicKey, opts = {}) {
    super()

    if (rtc.closing || rtc.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    this.rtc = rtc
    this.rtc.clients.add(this)

    this.keyPair = opts.keyPair || this.rtc.keyPair
    this.remotePublicKey = remotePublicKey
    this.token = b4a.toString(crypto.randomBytes(32), 'hex')

    this.closed = false
    this.closePromise = null

    this.signaling = new Signaling(this.rtc, this.keyPair, { _tag: 'client' })

    this.signaling._negotiation = this._onNegotiation.bind(this)
    this.signaling._ice = this._onIce.bind(this)
    this.signaling._ack = this._onAck.bind(this)

    this.peer = this.rtc._createPeer(this.signaling, true, this.remotePublicKey, this.token)

    this.connection = this.peer.connection

    this.peer.once('close', () => {
      this.close().catch(err => {
        if (this.rtc.debug) console.error(err)
      })
    })

    this._connect().catch(err => {
      if (this.rtc.debug) console.error(err)
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

    this.rtc.clients.delete(this)

    const peer = this.peer
    this.peer = null

    if (peer) {
      try {
        await peer.connection.close()
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }

    const signaling = this.signaling
    this.signaling = null

    if (signaling) {
      try {
        await signaling.close()
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }

    this.removeAllListeners()
  }

  async _connect () {
    try {
      await this.signaling.request('webrtc-connect', {
        token: this.token,
        isInitiator: true,
        remotePublicKey: this.remotePublicKey
      })
    } catch (err) {
      await this.close()
      throw err
    }
  }

  async _onNegotiation (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    if (!req.body || req.body.token !== this.token) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const peer = this.rtc.peers.get(this.token)

    if (!peer) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    await this.rtc._negotiation(peer, req)
  }

  async _onIce (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    if (!req.body || req.body.token !== this.token) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const peer = this.rtc.peers.get(this.token)

    if (!peer) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    await this.rtc._ice(peer, req)
  }

  async _onAck (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    if (!req.body || req.body.token !== this.token) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const peer = this.rtc.peers.get(this.token)

    if (!peer) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    await this.rtc._ack(peer, req)
  }
}
