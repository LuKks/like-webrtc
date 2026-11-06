const { EventEmitter } = require('events')
const b4a = require('b4a')
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

    this.signaling = this.rtc._getSignaling(this.keyPair)
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
}
