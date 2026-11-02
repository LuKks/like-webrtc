const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const SecretStream = require('@hyperswarm/secret-stream')
const RPC = require('./rpc.js')
const bs58 = maybeDefaultModule(require('bs58'))

module.exports = class PartyServer {
  constructor (opts = {}) {
    this.connections = new Set()
    this.streams = new Map()

    this.announces = new Map()
    this.topics = new Map()
    this.pairs = new Map()

    this.announceTTL = opts.announceTTL || 30 * 60 * 1000
    this.pairTTL = opts.pairTTL || 30 * 60 * 1000
    this.debug = !!opts.debug
  }

  async close () {
    for (const rpc of this.connections) {
      try {
        await rpc.close()
      } catch {}
    }

    for (const [, announce] of this.announces) {
      if (announce.timeout) clearTimeout(announce.timeout)
    }

    for (const peers of this.topics.values()) {
      for (const announce of peers.values()) {
        if (announce.timeout) clearTimeout(announce.timeout)
      }
    }

    for (const [, pair] of this.pairs) {
      if (pair.timeout) clearTimeout(pair.timeout)
    }

    this.connections.clear()
    this.streams.clear()
    this.announces.clear()
    this.topics.clear()
    this.pairs.clear()
  }

  handle (rawStream) {
    const stream = new SecretStream(false, rawStream, {
      keepAlive: 5000
    })
    stream.setTimeout(15000)

    const rpc = new RPC(stream)

    rpc.tag = 'party'

    rpc.respond('announce', this._announce.bind(this, rpc))
    rpc.respond('unannounce', this._unannounce.bind(this, rpc))
    rpc.respond('lookup', this._lookup.bind(this, rpc))

    rpc.respond('webrtc-connect', this._relayConnect.bind(this, rpc))
    rpc.respond('webrtc-negotiation', this._relayNegotiation.bind(this, rpc))
    rpc.respond('webrtc-ice', this._relayIce.bind(this, rpc))
    rpc.respond('webrtc-ack', this._relayAck.bind(this, rpc))

    this.connections.add(rpc)

    rpc.stream.on('handshake', () => {
      const id = this._streamId(rpc)

      this.streams.set(id, rpc)

      rpc.stream.on('close', () => {
        if (this.streams.get(id) === rpc) {
          this.streams.delete(id)
        }

        this._cleanupDeadPairs()
      })
    })

    rpc.stream.on('close', () => {
      this.connections.delete(rpc)
      this._cleanupDeadPairs()
    })
  }

  _announce (rpc, req) {
    if (req.body.topic !== undefined) {
      return this._announceTopic(rpc, req)
    }

    const publicKey = req.body.publicKey

    if (!b4a.equals(publicKey, rpc.stream.remotePublicKey)) {
      throw new RPC.Error('PUBLIC_KEY_MISMATCH')
    }

    this._verifyTimestamp(req.body.timestamp)

    const message = b4a.concat([
      b4a.from('party-announce'),
      b4a.from(req.body.timestamp.toString())
    ])

    if (!req.body.signature || !crypto.verify(message, req.body.signature, publicKey)) {
      throw new RPC.Error('SIGNATURE_INVALID')
    }

    const target = keyToHex(publicKey)
    const current = this.announces.get(target)

    if (current && current.timeout) {
      clearTimeout(current.timeout)
    }

    const timeout = setTimeout(() => {
      this.announces.delete(target)
    }, this.announceTTL)

    this.announces.set(target, {
      publicKey,
      remoteStreamId: rpc.stream.handshakeHash,
      streamId: this._streamId(rpc),
      timeout
    })

    if (this.debug) {
      console.log('- Announce', bs58.encode(publicKey))
    }
  }

  _announceTopic (rpc, req) {
    if (typeof req.body.topic !== 'string' || req.body.topic.length === 0) {
      throw new RPC.Error('INVALID_TOPIC')
    }

    this._verifyTimestamp(req.body.timestamp)

    const message = b4a.concat([
      b4a.from('party-announce'),
      b4a.from(req.body.topic),
      b4a.from(req.body.timestamp.toString())
    ])

    if (!req.body.signature || !crypto.verify(message, req.body.signature, req.body.publicKey)) {
      throw new RPC.Error('SIGNATURE_INVALID')
    }

    let peers = this.topics.get(req.body.topic)

    if (!peers) {
      peers = new Map()

      this.topics.set(req.body.topic, peers)
    }

    const target = keyToHex(req.body.publicKey)
    const current = peers.get(target)

    if (current && current.timeout) {
      clearTimeout(current.timeout)
    }

    const announce = {
      publicKey: req.body.publicKey,
      streamId: this._streamId(rpc),
      timeout: null
    }

    announce.timeout = setTimeout(() => {
      if (peers.get(target) !== announce) {
        return
      }

      peers.delete(target)

      if (peers.size === 0) {
        this.topics.delete(req.body.topic)
      }
    }, this.announceTTL)

    peers.set(target, announce)
  }

  _unannounce (rpc, req) {
    if (req.body.topic !== undefined) {
      return this._unannounceTopic(req)
    }

    const publicKey = req.body.publicKey

    if (!b4a.equals(publicKey, rpc.stream.remotePublicKey)) {
      throw new RPC.Error('PUBLIC_KEY_MISMATCH')
    }

    this._verifyTimestamp(req.body.timestamp)

    const message = b4a.concat([
      b4a.from('party-unannounce'),
      b4a.from(req.body.timestamp.toString())
    ])

    if (!req.body.signature || !crypto.verify(message, req.body.signature, publicKey)) {
      throw new RPC.Error('SIGNATURE_INVALID')
    }

    const target = keyToHex(publicKey)
    const current = this.announces.get(target)

    if (current && current.streamId === this._streamId(rpc)) {
      if (current.timeout) {
        clearTimeout(current.timeout)
      }

      this.announces.delete(target)
    }

    if (this.debug) {
      console.log('- Unannounce', bs58.encode(publicKey))
    }
  }

  _unannounceTopic (req) {
    if (typeof req.body.topic !== 'string' || req.body.topic.length === 0) {
      throw new RPC.Error('INVALID_TOPIC')
    }

    this._verifyTimestamp(req.body.timestamp)

    const message = b4a.concat([
      b4a.from('party-unannounce'),
      b4a.from(req.body.topic),
      b4a.from(req.body.timestamp.toString())
    ])

    if (!req.body.signature || !crypto.verify(message, req.body.signature, req.body.publicKey)) {
      throw new RPC.Error('SIGNATURE_INVALID')
    }

    const peers = this.topics.get(req.body.topic)

    if (!peers) {
      return
    }

    const target = keyToHex(req.body.publicKey)
    const announce = peers.get(target)

    if (announce && announce.timeout) {
      clearTimeout(announce.timeout)
    }

    peers.delete(target)

    if (peers.size === 0) {
      this.topics.delete(req.body.topic)
    }
  }

  _lookup (rpc, req) {
    if (req.body.topic !== undefined) {
      return this._lookupTopic(rpc, req)
    }

    const target = keyToHex(req.body.publicKey)
    const announce = this.announces.get(target)

    if (this.debug) {
      console.log('- Lookup', bs58.encode(req.body.publicKey))
    }

    if (!announce) {
      return []
    }

    return [{
      itself: true,
      remoteStreamId: announce.remoteStreamId
    }]
  }

  _lookupTopic (rpc, req) {
    const peers = this.topics.get(req.body.topic)

    if (!peers) {
      return []
    }

    const results = []

    for (const announce of peers.values()) {
      results.push({ publicKey: announce.publicKey })
    }

    return results
  }

  async _relayConnect (rpc, req) {
    this._validateRelayRequest(req)

    if (!req.body.isInitiator) {
      throw new RPC.Error('INVALID_SIDE')
    }

    const pair = this._getOrCreatePair(rpc, req)

    this._bindOrRefreshSide(pair, rpc, req)
    this._touchPair(pair)

    const target = pair.responder

    if (!target || target.closed || target.stream.destroying) {
      throw new RPC.Error('PEER_NOT_FOUND')
    }

    return target.request('webrtc-connect', {
      token: pair.token,
      isInitiator: true,
      remotePublicKey: rpc.stream.remotePublicKey
    })
  }

  async _relayNegotiation (rpc, req) {
    return this._relay(rpc, req, { method: 'webrtc-negotiation', field: 'description' })
  }

  async _relayIce (rpc, req) {
    return this._relay(rpc, req, { method: 'webrtc-ice', field: 'candidate' })
  }

  async _relayAck (rpc, req) {
    return this._relay(rpc, req, { method: 'webrtc-ack', field: 'ack' })
  }

  async _relay (rpc, req, opts) {
    this._validateRelayRequest(req, opts.field)

    const pair = this._getOrCreatePair(rpc, req)

    this._bindOrRefreshSide(pair, rpc, req)

    const target = req.body.isInitiator ? pair.responder : pair.initiator

    if (this.debug) {
      console.log(
        '- Relay',
        opts.method,
        req.body.token,
        'from',
        req.body.isInitiator ? 'initiator' : 'responder',
        'to',
        req.body.isInitiator ? 'responder' : 'initiator'
      )
    }

    this._touchPair(pair)

    if (!target || target.closed || target.stream.destroying) {
      return
    }

    const body = {
      token: pair.token,

      isInitiator: req.body.isInitiator,

      remotePublicKey: rpc.stream.remotePublicKey,

      [opts.field]: req.body[opts.field]
    }

    if (req.body.negotiationId !== undefined) {
      body.negotiationId = req.body.negotiationId
    }

    if (req.body.seq !== undefined) {
      body.seq = req.body.seq
    }

    if (req.body.messageId !== undefined) {
      body.messageId = req.body.messageId
    }

    target.event(opts.method, body)
  }

  _getOrCreatePair (rpc, req) {
    const token = req.body.token
    let pair = this.pairs.get(token)

    if (pair) {
      return pair
    }

    // Only the token creator can create the pair, because only that side
    // tells us which announced responder it wants.
    if (!req.body.isInitiator) {
      throw new RPC.Error('PAIR_NOT_FOUND')
    }

    const responderPublicKey = req.body.remotePublicKey
    const responderAnnounce = this.announces.get(keyToHex(responderPublicKey))

    if (!responderAnnounce) {
      throw new RPC.Error('PEER_NOT_FOUND')
    }

    const initiator = rpc
    const responder = this.streams.get(keyToHex(responderAnnounce.remoteStreamId)) || null

    pair = {
      token,

      initiator,
      responder,

      initiatorPublicKey: rpc.stream.remotePublicKey,
      responderPublicKey,

      created: Date.now(),
      updated: Date.now(),
      timeout: null
    }

    this.pairs.set(token, pair)

    this._touchPair(pair)

    if (this.debug) {
      console.log(
        '- Pair',
        token,
        bs58.encode(pair.initiatorPublicKey),
        '<->',
        bs58.encode(pair.responderPublicKey)
      )
    }

    return pair
  }

  _bindOrRefreshSide (pair, rpc, req) {
    const senderPublicKey = rpc.stream.remotePublicKey

    if (req.body.isInitiator) {
      if (!b4a.equals(senderPublicKey, pair.initiatorPublicKey)) {
        throw new RPC.Error('PAIR_INITIATOR_MISMATCH')
      }

      pair.initiator = this._replaceSide(pair.initiator, rpc)

      return
    }

    if (!b4a.equals(senderPublicKey, pair.responderPublicKey)) {
      throw new RPC.Error('PAIR_RESPONDER_MISMATCH')
    }

    pair.responder = this._replaceSide(pair.responder, rpc)
  }

  _replaceSide (current, next) {
    if (current === next) {
      return current
    }

    // Same public key, new secure stream. This is a reconnect.
    // Destroy the old stream to avoid two active RPC streams for one token side.
    if (current && !current.closed && !current.stream.destroying) {
      try {
        current.stream.destroy()
      } catch {}
    }

    return next
  }

  _touchPair (pair) {
    pair.updated = Date.now()

    if (pair.timeout) {
      clearTimeout(pair.timeout)
    }

    pair.timeout = setTimeout(() => {
      this.pairs.delete(pair.token)
    }, this.pairTTL)
  }

  _cleanupDeadPairs () {
    for (const [token, pair] of this.pairs) {
      const initiatorDead = !pair.initiator || pair.initiator.closed || pair.initiator.stream.destroying
      const responderDead = !pair.responder || pair.responder.closed || pair.responder.stream.destroying

      if (initiatorDead && responderDead) {
        if (pair.timeout) {
          clearTimeout(pair.timeout)
        }

        this.pairs.delete(token)
      }
    }
  }

  _validateRelayRequest (req, field) {
    if (!req || !req.body) {
      throw new RPC.Error('INVALID_REQUEST')
    }

    if (typeof req.body.token !== 'string' || req.body.token.length === 0) {
      throw new RPC.Error('INVALID_TOKEN')
    }

    if (typeof req.body.isInitiator !== 'boolean') {
      throw new RPC.Error('INVALID_SIDE')
    }

    if (!req.body.remotePublicKey) {
      throw new RPC.Error('INVALID_REMOTE_PUBLIC_KEY')
    }

    if (field && !(field in req.body)) {
      throw new RPC.Error('INVALID_PAYLOAD')
    }
  }

  _verifyTimestamp (timestamp) {
    if (typeof timestamp !== 'number' || Math.abs(Date.now() - timestamp) > 5 * 60 * 1000) {
      throw new RPC.Error('SIGNATURE_EXPIRED')
    }
  }

  _streamId (rpc) {
    return keyToHex(rpc.stream.handshakeHash)
  }
}

function keyToHex (key) {
  return b4a.toString(key, 'hex')
}

function maybeDefaultModule (mod) {
  return mod.default ? mod.default : mod
}
