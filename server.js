const EventEmitter = require('events')
const b4a = require('b4a')
const Client = require('./client.js')
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
    this.jitter = !!opts.jitter

    this.closed = false
    this.peers = new Set()
    this.closePromise = null
    this.announcedTopics = new Set()
    this.manualAnnouncedTopics = new Set()
    this.unannouncedTopics = new Set()
    this.joinedTopics = new Set()
    this.joinedPeers = new Map()
    this._lookupInterval = null

    this.signaling = this.rtc._getSignaling(this.keyPair)
    this.signaling._retain(this)

    this._onSignalingOnline = () => {
      if (this.closed || this.rtc.closing || this.rtc.closed) {
        return
      }

      if (this.signaling._server === this) {
        this._announce(this.keyPair, this.signaling).catch(err => {
          if (this.rtc.debug) console.error(err)
        })

        this._announceTopics().catch(err => {
          if (this.rtc.debug) console.error(err)
        })
      }
    }
  }

  async listen (keyPair) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new Error('Server is closing or closed')
    }

    if (!keyPair) {
      keyPair = this.keyPair
    }

    const previous = this.signaling
    const signaling = this.rtc._getSignaling(keyPair)
    const wasListening = previous._server === this

    signaling._retain(this)

    try {
      await this._announce(keyPair, signaling)

      if (this.closed || this.rtc.closing || this.rtc.closed) {
        await this._unannounce(keyPair, signaling)

        throw new Error('Server is closing or closed')
      }
    } catch (err) {
      if (signaling !== previous) {
        await signaling._release(this)
      }

      throw err
    }

    if (signaling !== previous) {
      if (wasListening) {
        previous._server = null
      }

      this.signaling = signaling
      this.keyPair = keyPair

      previous._clearIncoming(this)
    }

    const server = signaling._server

    signaling._server = this

    if (server && server !== this) {
      await this._replaceListener(server)
    }

    if (signaling !== previous) {
      if (wasListening) {
        await this._unannounceTopics(previous.keyPair, previous)

        try {
          await this._unannounce(previous.keyPair, previous)
        } catch (err) {
          if (this.rtc.debug) console.error(err)
        }
      }

      await previous._release(this)
    }

    if (server && server !== this) {
      this._announceTopics().catch(err => {
        if (this.rtc.debug) console.error(err)
      })
    } else {
      await this._announceTopics()
    }

    if (this.signaling._server !== this || this.closed || this.rtc.closing || this.rtc.closed) {
      return
    }

    if (!this._announceInterval) {
      this._announceInterval = setInterval(() => {
        if (this.signaling._server !== this || this.closed || this.rtc.closing || this.rtc.closed) {
          return
        }

        this._announce(this.keyPair, this.signaling).catch(err => {
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

    if (this.signaling._server !== this) {
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

    const server = this.signaling._server

    if (server && server !== this && server.announcedTopics.has(topic)) {
      return
    }

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

    for (const [target, peer] of this.joinedPeers) {
      peer.topics.delete(topic)

      if (peer.topics.size > 0) {
        continue
      }

      if (peer.timeout) {
        clearTimeout(peer.timeout)
        peer.timeout = null
      }

      if (!peer.connection) {
        this.joinedPeers.delete(target)
      }
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
  async _announce (keyPair, signaling) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      throw new Error('Server is closing or closed')
    }

    const publicKey = keyPair.publicKey
    const timestamp = Date.now()
    const message = b4a.concat([b4a.from('party-announce'), b4a.from(timestamp.toString())])
    const signature = crypto.sign(message, keyPair.secretKey)

    await signaling.request('announce', { publicKey, timestamp, signature })
  }

  async _unannounce (keyPair, signaling = this.signaling) {
    if (signaling.closed || this.rtc.closed || (signaling._server && signaling._server !== this)) {
      return
    }

    const publicKey = keyPair.publicKey
    const timestamp = Date.now()
    const message = b4a.concat([b4a.from('party-unannounce'), b4a.from(timestamp.toString())])
    const signature = crypto.sign(message, keyPair.secretKey)

    await signaling.request('unannounce', { publicKey, timestamp, signature }, { _allowClosing: true })
  }

  async _replaceListener (server) {
    clearInterval(server._announceInterval)
    server._announceInterval = null

    for (const topic of server.announcedTopics) {
      if (this.unannouncedTopics.has(topic)) {
        continue
      }

      this.announcedTopics.add(topic)

      if (server.manualAnnouncedTopics.has(topic)) {
        this.manualAnnouncedTopics.add(topic)
      }
    }

    const unannouncing = server._unannounceTopics(server.keyPair)

    server.announcedTopics.clear()
    server.manualAnnouncedTopics.clear()

    return unannouncing
  }

  async _announceTopics () {
    for (const topic of this.announcedTopics) {
      if (this.signaling._server !== this || this.closed || this.rtc.closing || this.rtc.closed) {
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

      if (this.signaling._server === this && !this.announcedTopics.has(topic) && !this.unannouncedTopics.has(topic)) {
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

        let joined = this.joinedPeers.get(target)

        if (!joined) {
          joined = {
            publicKey: peer.publicKey,
            topics: new Set(),
            connection: null,
            timeout: null,
            attempt: 0
          }

          this.joinedPeers.set(target, joined)
        }

        joined.topics.add(topic)

        this._connectJoinedPeer(joined)
      }
    }
  }

  _connectJoinedPeer (peer) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      return
    }

    if (peer.topics.size === 0 || peer.connection || peer.timeout) {
      return
    }

    if (this.rtc.debug) console.log('[Server] connecting to peer', b4a.toString(peer.publicKey, 'hex'))

    let client = null

    try {
      client = new Client(this.rtc, peer.publicKey, { keyPair: this.keyPair })
    } catch (err) {
      if (this.rtc.debug) console.error(err)

      this._retryJoinedPeer(peer)

      return
    }

    const connection = client.connection
    peer.connection = connection

    const onStateChange = () => {
      if (connection.connectionState === 'connected') {
        peer.attempt = 0
      }
    }

    connection.addEventListener('connectionstatechange', onStateChange)

    client.peer.once('close', () => {
      connection.removeEventListener('connectionstatechange', onStateChange)

      peer.connection = null

      if (peer.topics.size === 0) {
        this.joinedPeers.delete(b4a.toString(peer.publicKey, 'hex'))
        return
      }

      this._retryJoinedPeer(peer)
    })

    this.emit('connection', connection)
  }

  _retryJoinedPeer (peer) {
    if (this.closed || this.rtc.closing || this.rtc.closed) {
      return
    }

    if (peer.topics.size === 0 || peer.connection || peer.timeout) {
      return
    }

    const delays = [1000, 5000, 15000, 10 * 60 * 1000]
    const jitter = this.jitter ? 0.5 + Math.random() : 1
    const delay = Math.round(delays[Math.min(peer.attempt++, delays.length - 1)] * jitter)

    if (this.rtc.debug) console.log('[Server] retrying peer', b4a.toString(peer.publicKey, 'hex'), 'in', delay, 'ms')

    peer.timeout = setTimeout(() => {
      peer.timeout = null

      this._connectJoinedPeer(peer)
    }, delay)
  }

  async _unannounceTopics (keyPair, signaling = this.signaling) {
    for (const topic of Array.from(this.announcedTopics)) {
      const server = signaling._server

      if (server && server !== this && server.announcedTopics.has(topic)) {
        continue
      }

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

    const wasListening = this.signaling._server === this

    if (wasListening) {
      this.signaling._server = null
    }

    if (this._announceInterval) {
      clearInterval(this._announceInterval)
      this._announceInterval = null
    }

    if (this._lookupInterval) {
      clearInterval(this._lookupInterval)
      this._lookupInterval = null
    }

    this.joinedTopics.clear()

    this.signaling._clearIncoming(this)

    // Close peers before unannounce/signaling teardown. This prevents late
    // offers/candidates from racing with server shutdown and creating orphan
    // RTCPeerConnections.
    const peers = Array.from(this.peers)

    for (const peer of peers) {
      try {
        await peer.connection.close()
      } catch (err) {
        if (this.rtc.debug) console.error(err)
      }
    }

    const joinedPeers = Array.from(this.joinedPeers.values())

    for (const peer of joinedPeers) {
      if (peer.timeout) {
        clearTimeout(peer.timeout)
      }

      if (!peer.connection) {
        continue
      }

      try {
        await peer.connection.close()
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
      wasListening &&
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
      await this.signaling._release(this)
    } catch (err) {
      if (this.rtc.debug) console.error(err)
    }

    this.rtc.servers.delete(this)
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

  _createIncomingPeer (req) {
    const token = req.body.token
    const peer = this.rtc._createPeer(this.signaling, false, req.body.remotePublicKey, token)

    this.peers.add(peer)

    peer.once('close', () => {
      this.peers.delete(peer)
    })

    this.emit('connection', peer.connection)

    return peer
  }
}

function maybeDefaultModule (mod) {
  return mod.default ? mod.default : mod
}
