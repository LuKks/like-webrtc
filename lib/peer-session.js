module.exports = class PeerSession {
  constructor (signaling, peer, opts = {}) {
    this.signaling = signaling
    this.peer = peer
    this.connection = peer.connection
    this.token = opts.token
    this.isInitiator = opts.isInitiator
    this.remotePublicKey = opts.remotePublicKey

    this._id = 0
    this._pending = new Map()
    this._received = new Map()
    this._offer = null
    this._onSignal = this._send.bind(this)
    this._onSignalingOnline = this._online.bind(this)
    this._onRollback = this._rollback.bind(this)

    peer.on('signal', this._onSignal)
    peer.on('rollback', this._onRollback)
    peer.once('close', () => this._close())

    if (!signaling._users) {
      signaling.on('online', this._onSignalingOnline)
    }
  }

  get closed () {
    return this.peer.closed
  }

  get closing () {
    return this.peer.closing
  }

  async signal (name, body) {
    if (name === 'webrtc-ack') {
      if (body.ack === null) this._flush()
      else this._pending.delete(body.ack)
      return
    }

    const data = name === 'webrtc-negotiation' ? { description: body.description } : { candidate: body.candidate }
    const id = body.messageId
    const previous = id && this._received.get(id)

    if (previous) {
      if (await previous === false) {
        return
      }

      this._ack(id)

      if (data.description?.type === 'offer') {
        for (const message of this._pending.values()) {
          if (message.body.description?.type === 'answer' || message.name === 'webrtc-ice') {
            this._deliver(message)
          }
        }
      }
      return
    }

    const operation = this.peer._signal(data)

    if (id) this._received.set(id, operation)

    try {
      if (await operation === false) {
        if (id) this._received.delete(id)
        return
      }
    } catch (err) {
      if (id) this._received.delete(id)
      throw err
    }

    if (id) {
      if (this._received.size > 512) {
        this._received.delete(this._received.keys().next().value)
      }

      this._ack(id)
    }
  }

  _send (data) {
    const description = data.description
    const name = description ? 'webrtc-negotiation' : 'webrtc-ice'

    if (description) {
      for (const [id, message] of this._pending) {
        if (message.body.description?.type === description.type) {
          this._pending.delete(id)
        }
      }
    }

    const body = {
      token: this.token,
      isInitiator: this.isInitiator,
      remotePublicKey: this.remotePublicKey,
      messageId: this.token + ':' + Number(this.isInitiator) + ':' + (++this._id),
      ...data
    }
    const message = { name, body }

    this._pending.set(body.messageId, message)

    if (description?.type === 'offer') {
      this._offer = message
    }

    this._deliver(message)
  }

  _deliver (message) {
    if (this.closing || this.signaling.closed) return

    try {
      this.signaling.event(message.name, message.body)
    } catch (err) {
      // A reconnect replays messages that were not acknowledged by the peer.
      if (this.peer.debug) console.error(err)
    }
  }

  _ack (id) {
    this._deliver({
      name: 'webrtc-ack',
      body: {
        token: this.token,
        isInitiator: this.isInitiator,
        remotePublicKey: this.remotePublicKey,
        ack: id
      }
    })
  }

  _online () {
    if (this.closing) return

    // Refresh this side's relay route and request the other peer's pending signals.
    this._ack(null)
    this._flush()
  }

  _flush () {
    for (const message of this._pending.values()) this._deliver(message)

    if (this.connection.signalingState === 'have-local-offer' && this._offer && !this._pending.has(this._offer.body.messageId)) {
      this._deliver(this._offer)
    }
  }

  _rollback () {
    this._offer = null

    for (const [id, message] of this._pending) {
      if (!message.body.description || message.body.description.type === 'offer') {
        this._pending.delete(id)
      }
    }
  }

  _close () {
    this.signaling.removeListener('online', this._onSignalingOnline)
    this.peer.removeListener('signal', this._onSignal)
    this.peer.removeListener('rollback', this._onRollback)
    this._pending.clear()
    this._received.clear()
    this._offer = null
  }
}
