const EventEmitter = require('events')
const b4a = require('b4a')
const RPC = require('./lib/rpc.js')
const Signaling = require('./signaling.js')
const crypto = require('hypercore-crypto')
const bs58 = maybeDefaultModule(require('bs58'))

// TODO: Should test server with interoperability, werift and client with node-datachannel
// TODO: Client needs to send proof that owns the public key
module.exports = class Server extends EventEmitter {
  constructor (rtc, opts = {}) {
    super()

    if (rtc.closing || rtc.closed) {
      throw new Error('WebRTC is closing or closed')
    }

    this.rtc = rtc
    this.rtc.servers.add(this)

    this.keyPair = opts.keyPair || this.rtc.keyPair
    this.firewall = opts.firewall || (() => true)

    this.closed = false
    this.closePromise = null
    this.announcedTopics = new Set()
    this.manualAnnouncedTopics = new Set()
    this.unannouncedTopics = new Set()
    this.joinedTopics = new Set()
    this.joinedPeers = new Map()
    this._lookupInterval = null

    this.pendingIce = new Map()
    this.pendingPeerCreates = new Map()
    this.pendingIceTimeout = opts.pendingIceTimeout || 30 * 1000

    this.signaling = new Signaling(this.rtc, this.keyPair, { _tag: 'server' })

    this.signaling._connect = this._onConnect.bind(this)
    this.signaling._negotiation = this._onNegotiation.bind(this)
    this.signaling._ice = this._onIce.bind(this)
    this.signaling._ack = this._onAck.bind(this)

    this._onSignalingOnline = () => {
      if (this.closed || this.rtc.closing || this.rtc.closed) {
        return
      }

      if (this._announceInterval) {
        this._announce(this.keyPair).catch(err => {
          if (this.rtc.debug) console.error(err)
        })

        this._announceTopics().catch(err => {
          if (this.rtc.debug) console.error(err)
        })
      }
    }

    this.signaling.on('online', this._onSignalingOnline)
  }

  async listen (keyPair) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new Error('Server is closing or closed')
    }

    const previous = this.keyPair

    if (!keyPair) {
      keyPair = this.keyPair
    }

    // TODO: Being defensive with b4a.from to avoid RN errors
    const changed = !b4a.equals(b4a.from(keyPair.publicKey), b4a.from(previous.publicKey))
    this.keyPair = keyPair

    await this._announce(this.keyPair)

    if (changed) {
      await this._unannounceTopics(previous)

      try {
        await this._unannounce(previous)
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }

    await this._announceTopics()

    if (!this._announceInterval) {
      this._announceInterval = setInterval(() => {
        if (this.closed || this.rtc.closing || this.rtc.closed) {
          return
        }

        this._announce(this.keyPair).catch(err => {
          if (this.rtc.debug) console.error(err)
        })

        this._announceTopics().catch(err => {
          if (this.rtc.debug) console.error(err)
        })
      }, 10 * 60 * 1000)
    }

    this._lookupTopics().catch(err => {
      if (this.rtc.debug) console.error(err)
    })
  }

  async announce (topic) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new Error('Server is closing or closed')
    }

    if (!this._announceInterval) {
      throw new Error('Server is not listening')
    }

    const wasUnannounced = this.unannouncedTopics.delete(topic)
    const wasAnnounced = this.announcedTopics.has(topic)
    const wasManuallyAnnounced = this.manualAnnouncedTopics.has(topic)

    this.announcedTopics.add(topic)
    this.manualAnnouncedTopics.add(topic)

    try {
      await this.rtc.announce(topic, { keyPair: this.keyPair })
    } catch (err) {
      if (!wasAnnounced) this.announcedTopics.delete(topic)
      if (!wasManuallyAnnounced) this.manualAnnouncedTopics.delete(topic)
      if (wasUnannounced) this.unannouncedTopics.add(topic)

      throw err
    }
  }

  async unannounce (topic) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new Error('Server is closing or closed')
    }

    if (typeof topic !== 'string' || topic.length === 0) {
      throw new Error('Topic is required')
    }

    const wasUnannounced = this.unannouncedTopics.has(topic)

    this.unannouncedTopics.add(topic)

    const announced = this.announcedTopics.delete(topic)
    const manuallyAnnounced = this.manualAnnouncedTopics.delete(topic)

    try {
      await this.rtc.unannounce(topic, { keyPair: this.keyPair })
    } catch (err) {
      if (announced) this.announcedTopics.add(topic)
      if (manuallyAnnounced) this.manualAnnouncedTopics.add(topic)
      if (!wasUnannounced) this.unannouncedTopics.delete(topic)

      throw err
    }
  }

  async join (topic) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new Error('Server is closing or closed')
    }

    if (typeof topic !== 'string' || topic.length === 0) {
      throw new Error('Topic is required')
    }

    if (!this.joinedTopics.has(topic)) {
      this.unannouncedTopics.delete(topic)
    }

    this.joinedTopics.add(topic)

    if (!this._lookupInterval) {
      this._lookupInterval = setInterval(() => {
        if (this.closed || this.rtc.closing || this.rtc.closed) {
          return
        }

        this._lookupTopics().catch(err => {
          if (this.rtc.debug) console.error(err)
        })
      }, 10 * 60 * 1000)
    }

    await this._lookupTopics()
  }

  async leave (topic) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new Error('Server is closing or closed')
    }

    if (typeof topic !== 'string' || topic.length === 0) {
      throw new Error('Topic is required')
    }

    const joined = this.joinedTopics.delete(topic)

    if (!joined && !this.announcedTopics.has(topic)) {
      return
    }

    if (this.joinedTopics.size === 0 && this._lookupInterval) {
      clearInterval(this._lookupInterval)
      this._lookupInterval = null
    }

    if (this.announcedTopics.has(topic) && !this.manualAnnouncedTopics.has(topic)) {
      await this.unannounce(topic)
    }

    this.unannouncedTopics.delete(topic)
  }

  // Be aware of replay attacks on reusing past signatures to announce or unannounce
  async _announce (keyPair) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new Error('Server is closing or closed')
    }

    const publicKey = keyPair.publicKey
    const timestamp = Date.now()
    const message = b4a.concat([b4a.from('party-announce'), b4a.from(timestamp.toString())])
    const signature = crypto.sign(message, keyPair.secretKey)

    await this.signaling.request('announce', { publicKey, timestamp, signature })
  }

  async _unannounce (keyPair) {
    if (this.signaling.closed || this.rtc.closed) {
      return
    }

    const publicKey = keyPair.publicKey
    const timestamp = Date.now()
    const message = b4a.concat([b4a.from('party-unannounce'), b4a.from(timestamp.toString())])
    const signature = crypto.sign(message, keyPair.secretKey)

    await this.signaling.request('unannounce', { publicKey, timestamp, signature }, { _allowClosing: true })
  }

  async _announceTopics () {
    for (const topic of this.announcedTopics) {
      if (this.closed || this.rtc.closing || this.rtc.closed) {
        return
      }

      await this.rtc.announce(topic, { keyPair: this.keyPair })
    }
  }

  async _lookupTopics () {
    for (const topic of this.joinedTopics) {
      if (this.closed || this.rtc.closing || this.rtc.closed) {
        return
      }

      if (this._announceInterval && !this.announcedTopics.has(topic) && !this.unannouncedTopics.has(topic)) {
        this.announcedTopics.add(topic)

        try {
          await this.rtc.announce(topic, { keyPair: this.keyPair })
        } catch (err) {
          if (!this.manualAnnouncedTopics.has(topic)) {
            this.announcedTopics.delete(topic)
          }

          throw err
        }
      }

      const peers = await this.rtc.lookup(topic)

      for (const peer of peers) {
        if (this.closed || this.rtc.closing || this.rtc.closed) {
          return
        }

        if (!this.joinedTopics.has(topic)) {
          break
        }

        if (b4a.equals(peer.publicKey, this.keyPair.publicKey)) {
          continue
        }

        const target = b4a.toString(peer.publicKey, 'hex')

        if (this.joinedPeers.has(target)) {
          continue
        }

        const connection = this.rtc.connect(peer.publicKey)

        this.joinedPeers.set(target, connection)

        connection.addEventListener('connectionstatechange', () => {
          if (connection.connectionState !== 'closed') {
            return
          }

          if (this.joinedPeers.get(target) === connection) {
            this.joinedPeers.delete(target)
          }
        })

        this.emit('connection', connection)
      }
    }
  }

  async _unannounceTopics (keyPair) {
    for (const topic of this.announcedTopics) {
      try {
        await this.rtc.unannounce(topic, { keyPair })
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }
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

    if (this._announceInterval) {
      clearInterval(this._announceInterval)
      this._announceInterval = null
    }

    if (this._lookupInterval) {
      clearInterval(this._lookupInterval)
      this._lookupInterval = null
    }

    this.joinedTopics.clear()

    this._clearPendingIce()
    this.pendingPeerCreates.clear()

    if (this.signaling && this._onSignalingOnline) {
      this.signaling.removeListener('online', this._onSignalingOnline)
    }

    // Close peers before unannounce/signaling teardown. This prevents late
    // offers/candidates from racing with server shutdown and creating orphan
    // RTCPeerConnections.
    const peers = Array.from(this.rtc.peers.values())

    for (const peer of peers) {
      if (peer.signaling !== this.signaling) {
        continue
      }

      try {
        await peer.connection.close()
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }

    const joinedPeers = Array.from(this.joinedPeers.values())

    for (const connection of joinedPeers) {
      try {
        await connection.close()
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }

    this.joinedPeers.clear()

    await this._unannounceTopics(this.keyPair)

    this.announcedTopics.clear()
    this.manualAnnouncedTopics.clear()
    this.unannouncedTopics.clear()

    if (
      this.signaling &&
      this.signaling.rpc &&
      this.signaling.rpc.opened &&
      this.signaling.rpc.stream &&
      !this.signaling.rpc.stream.destroying
    ) {
      try {
        await this._unannounce(this.keyPair)
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }

    try {
      await this.signaling.close()
    } catch (err) {
      if (this.rtc.debug) console.error(err)
    }

    this.rtc.servers.delete(this)
    this.removeAllListeners()
  }

  get address () {
    return bs58.encode(this.keyPair.publicKey)
  }

  get publicKey () {
    return this.keyPair.publicKey
  }

  _firewall (remotePublicKey) {
    if (typeof this.firewall === 'function') {
      return this.firewall(remotePublicKey)
    }

    if (Array.isArray(this.firewall)) {
      for (let publicKey of this.firewall) {
        if (publicKey === '*') {
          return true
        }

        if (typeof publicKey === 'string') {
          if (publicKey.length === 64) publicKey = b4a.from(publicKey, 'hex')
          else if (publicKey.length >= 26 && publicKey.length <= 44) publicKey = bs58.decode(publicKey)
        }

        // TODO: TypeError: Argument must be a Buffer
        // Not sure why it requires b4a.from here to avoid error on React Native
        if (b4a.equals(b4a.from(publicKey), b4a.from(remotePublicKey))) {
          return true
        }
      }

      return false
    }

    return false
  }

  async _onConnect (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const peer = await this._getOrCreatePeer(req)

    if (peer.closed || peer.closing) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }
  }

  async _onNegotiation (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const peer = await this._getOrCreatePeer(req)

    await this.rtc._negotiation(peer, req)

    peer.sendAck(req.body.messageId)
  }

  async _onIce (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    let peer = this.rtc.peers.get(req.body.token)

    if (!peer) {
      const pending = this.pendingPeerCreates.get(req.body.token)

      if (pending) {
        peer = await pending
        await this.rtc._ice(peer, req)
        peer.sendAck(req.body.messageId)
        return
      }

      if (!(await this._firewall(req.body.remotePublicKey))) {
        throw new RPC.Error('PEER_CONNECTION_FAILED')
      }

      if (this.closed || this.rtc.closing || this.rtc.closed) {
        throw new RPC.Error('PEER_CONNECTION_FAILED')
      }

      this._queuePendingIce(req.body.token, req.body.candidate, req.body.messageId)

      return
    }

    await this.rtc._ice(peer, req)

    peer.sendAck(req.body.messageId)
  }

  async _onAck (req) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    if (!req.body || typeof req.body.token !== 'string') {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const peer = this.rtc.peers.get(req.body.token)

    if (!peer) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    await this.rtc._ack(peer, req)
  }

  async _getOrCreatePeer (req) {
    const token = req.body.token

    const peer = this.rtc.peers.get(token)

    if (peer) {
      return peer
    }

    const pending = this.pendingPeerCreates.get(token)

    if (pending) {
      return pending
    }

    if (req.method !== 'webrtc-connect' && (!req.body.description || req.body.description.type !== 'offer')) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    const creating = this._createIncomingPeer(req)
      .finally(() => {
        this.pendingPeerCreates.delete(token)
      })

    this.pendingPeerCreates.set(token, creating)

    return creating
  }

  async _createIncomingPeer (req) {
    const token = req.body.token

    if (!(await this._firewall(req.body.remotePublicKey))) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new RPC.Error('PEER_CONNECTION_FAILED')
    }

    // Another concurrent request may have created it while firewall awaited.
    let peer = this.rtc.peers.get(token)

    if (peer) {
      return peer
    }

    peer = this.rtc._createPeer(this.signaling, false, req.body.remotePublicKey, token)

    this.emit('connection', peer.connection)

    for (const pending of this._takePendingIce(token)) {
      await peer.handleCandidate(pending.candidate, pending.messageId)

      peer.sendAck(pending.messageId)
    }

    return peer
  }

  _queuePendingIce (token, candidate, messageId) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      return
    }

    let pending = this.pendingIce.get(token)

    if (!pending) {
      pending = {
        candidates: [],
        timeout: null
      }

      this.pendingIce.set(token, pending)
    }

    pending.candidates.push({ candidate, messageId })

    if (pending.timeout) {
      clearTimeout(pending.timeout)
    }

    pending.timeout = setTimeout(() => {
      this.pendingIce.delete(token)
    }, this.pendingIceTimeout)
  }

  _takePendingIce (token) {
    const pending = this.pendingIce.get(token)

    if (!pending) {
      return []
    }

    if (pending.timeout) {
      clearTimeout(pending.timeout)
    }

    this.pendingIce.delete(token)

    return pending.candidates
  }

  _clearPendingIce () {
    for (const pending of this.pendingIce.values()) {
      if (pending.timeout) {
        clearTimeout(pending.timeout)
      }
    }

    this.pendingIce.clear()
  }
}

function maybeDefaultModule (mod) {
  return mod.default ? mod.default : mod
}
