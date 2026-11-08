const EventEmitter = require('events')
const { RTCPeerConnection, RTCIceCandidate } = require('webrtc-runtime')

module.exports = class Peer extends EventEmitter {
  constructor (opts = {}) {
    super()

    this.RTCPeerConnection = opts.RTCPeerConnection || RTCPeerConnection
    this.RTCIceCandidate = opts.RTCIceCandidate || RTCIceCandidate

    this.connection = new this.RTCPeerConnection({
      iceServers: opts.iceServers || [],
      iceTransportPolicy: opts.iceTransportPolicy || 'all'
    })

    this.polite = !!opts.polite
    this.trickle = opts.trickle !== false
    this.debug = !!opts.debug

    this.closed = false
    this.closing = false

    this.connectTimeout = opts.connectTimeout ?? 30000
    this.disconnectTimeout = opts.disconnectTimeout ?? 120000
    this.iceDisconnectedRestartDelay = opts.iceDisconnectedRestartDelay ?? 5000

    this._operations = Promise.resolve()
    this._negotiationQueued = false
    this._restart = false
    this._repairing = false
    this._connected = false
    this._ignoreOffer = false
    this._rollingBack = false
    this._candidates = []
    this._remoteUfrags = new Set()
    this._retiredUfrags = new Set()
    this._listeners = new Map()
    this._gathering = null
    this._connectTimer = null
    this._disconnectTimer = null
    this._restartTimer = null
    this._rawClose = this.connection.close.bind(this.connection)

    this.connection.close = this.close.bind(this)

    this._listen('negotiationneeded', () => {
      // Rollback briefly becomes stable before the remote offer is installed.
      if (!this._rollingBack) this._requestNegotiation()
    })
    this._listen('icecandidate', event => {
      if (this.trickle) this.emit('signal', { candidate: candidateToJSON(event.candidate) })
    })
    this._listen('icegatheringstatechange', () => {
      if (this.connection.iceGatheringState === 'complete' && this._gathering) this._gathering()
    })
    this._listen('signalingstatechange', () => {
      if (this._restart && this.connection.signalingState === 'stable') this._requestNegotiation()
    })
    this._listen('connectionstatechange', () => this._updateConnectionState())
    this._listen('iceconnectionstatechange', () => this._updateConnectionState())

    this.on('error', noop)
  }

  signal (data) {
    this._signal(data).catch(err => this.emit('error', err))
  }

  close () {
    if (this.closing) return

    this.closing = true
    this._clearTimers()
    if (this._gathering) this._gathering()

    for (const [name, listener] of this._listeners) {
      this.connection.removeEventListener(name, listener)
    }

    this._listeners.clear()
    this._candidates.length = 0
    this._remoteUfrags.clear()
    this._retiredUfrags.clear()

    try {
      this._rawClose()
    } finally {
      this.closed = true
      this.emit('close')
    }
  }

  async _signal (data) {
    this._assertOpen()

    if (!data || typeof data !== 'object') {
      throw signalError('Expected a description or candidate')
    }

    if (data.description) {
      const description = data.description

      if (!['offer', 'answer'].includes(description.type) || typeof description.sdp !== 'string') {
        throw signalError('Invalid session description')
      }

      return this._enqueue(() => this._receiveDescription(description))
    }

    const candidate = data.candidate

    if (candidate !== null && (!candidate || typeof candidate.candidate !== 'string')) {
      throw signalError('Invalid ICE candidate')
    }

    return this._enqueue(() => this._receiveCandidate(candidate))
  }

  _enqueue (operation) {
    const run = this._operations.then(() => {
      this._assertOpen()
      return operation()
    })

    this._operations = run.catch(noop)

    return run
  }

  _restartIce () {
    this._assertOpen()

    this._restart = true

    if (typeof this.connection.restartIce === 'function') {
      this.connection.restartIce()
    }

    return this._requestNegotiation()
  }

  _requestNegotiation () {
    if (this.closing || this._negotiationQueued) return this._operations

    this._negotiationQueued = true

    const run = this._enqueue(async () => {
      this._negotiationQueued = false
      await this._negotiate()
    })

    run.catch(err => this._fail(err))

    return run
  }

  async _negotiate () {
    if (this.connection.signalingState !== 'stable') return

    this._startConnectTimer()

    const restart = this._restart
    this._restart = false
    const offer = await this.connection.createOffer(restart ? { iceRestart: true } : {})

    if (this.closing) return

    await this._sendDescription(offer)
  }

  async _receiveDescription (description) {
    if (description.type === 'answer' && this.connection.signalingState !== 'have-local-offer') {
      return
    }

    const collision = description.type === 'offer' && this.connection.signalingState !== 'stable'

    this._ignoreOffer = collision && !this.polite

    if (this._ignoreOffer) {
      return false
    }

    this._startConnectTimer()

    this._rollingBack = collision

    try {
      if (collision) {
        await this.connection.setLocalDescription({ type: 'rollback' })

        if (this.closing) return

        this.emit('rollback')
      }

      await this.connection.setRemoteDescription(description)
    } finally {
      this._rollingBack = false
    }

    if (this.closing) return

    const ufrags = iceUfrags(description)

    for (const ufrag of this._remoteUfrags) {
      if (!ufrags.has(ufrag)) {
        this._retiredUfrags.add(ufrag)
      }
    }

    this._remoteUfrags = ufrags

    // Keep candidates for a future description, including an answer after glare.
    const candidates = this._candidates
    this._candidates = []

    for (const candidate of candidates) {
      if (this.closing) return

      await this._receiveCandidate(candidate)
    }

    if (description.type === 'offer') {
      const answer = await this.connection.createAnswer()

      if (this.closing) return

      await this._sendDescription(answer)
    }
  }

  async _receiveCandidate (candidate) {
    const ufrag = candidate && (candidate.usernameFragment || candidate.candidate.match(/\bufrag ([^ ]+)/)?.[1])

    if (ufrag && this._retiredUfrags.has(ufrag) && !this._remoteUfrags.has(ufrag)) {
      return
    }

    if (this._ignoreOffer || !this.connection.remoteDescription || (ufrag && !this._remoteUfrags.has(ufrag))) {
      if (this._candidates.length >= 1024) {
        throw signalError('Too many pending ICE candidates')
      }
      this._candidates.push(candidate)
      return
    }

    await this.connection.addIceCandidate(candidate === null ? null : new this.RTCIceCandidate(candidate))
  }

  async _sendDescription (description) {
    await this.connection.setLocalDescription(description)

    if (this.closing) return

    if (!this.trickle && this.connection.iceGatheringState !== 'complete') {
      await new Promise(resolve => { this._gathering = resolve })
      this._gathering = null
    }

    if (this.closing) return

    const { type, sdp } = this.connection.localDescription

    if (this.debug) console.log('[Peer] local description', type)

    this.emit('signal', { description: { type, sdp } })
  }

  _updateConnectionState () {
    const state = this.connection.connectionState
    const ice = this.connection.iceConnectionState

    if (this.debug) console.log('[Peer] connection state', state, ice)

    if (state === 'closed' || ice === 'closed') {
      this._tryClose()
      return
    }

    if (state === 'connected' || (!state && (ice === 'connected' || ice === 'completed'))) {
      this._connected = true
      this._repairing = false
      this._clearTimers()
      return
    }

    if (!this._iceNeedsRepair()) return

    if (this._connected && !this._disconnectTimer && this.disconnectTimeout > 0) {
      this._disconnectTimer = setTimeout(() => {
        this._tryClose()
      }, this.disconnectTimeout)
    }

    if (this._repairing) return

    if (state === 'failed' || ice === 'failed') {
      clearTimeout(this._restartTimer)
      this._restartTimer = null
      this._repairing = true
      this._restartIce()
    } else if (!this._restartTimer) {
      this._restartTimer = setTimeout(() => {
        this._restartTimer = null
        if (!this._iceNeedsRepair()) {
          return
        }
        this._repairing = true
        this._restartIce()
      }, this.iceDisconnectedRestartDelay)
    }
  }

  _iceNeedsRepair () {
    if (this.connection.connectionState === 'connected') return false

    if (this.connection.connectionState === 'failed' || this.connection.connectionState === 'disconnected') return true
    if (this.connection.iceConnectionState === 'failed' || this.connection.iceConnectionState === 'disconnected') return true

    return false
  }

  _startConnectTimer () {
    if (this._connected || this._connectTimer || this.connectTimeout <= 0) return

    this._connectTimer = setTimeout(() => {
      const err = new Error('Timed out connecting to peer')
      err.code = 'PEER_CONNECT_TIMEOUT'
      this._fail(err)
    }, this.connectTimeout)
  }

  _clearTimers () {
    clearTimeout(this._connectTimer)
    clearTimeout(this._disconnectTimer)
    clearTimeout(this._restartTimer)

    this._connectTimer = null
    this._disconnectTimer = null
    this._restartTimer = null
  }

  _listen (name, listener) {
    const wrapped = event => {
      if (!this.closing) listener(event)
    }

    this._listeners.set(name, wrapped)
    this.connection.addEventListener(name, wrapped)
  }

  _assertOpen () {
    if (!this.closing) return

    const err = new Error('Peer is closed')
    err.code = 'PEER_CLOSED'
    throw err
  }

  _fail (err) {
    if (this.closing) return

    this.emit('error', err)
    this._tryClose()
  }

  _tryClose () {
    try {
      this.close()
    } catch (err) {
      this.emit('error', err)
    }
  }
}

function candidateToJSON (candidate) {
  if (!candidate) return null

  const { candidate: value, sdpMid, sdpMLineIndex, usernameFragment } = candidate
  return { candidate: value, sdpMid, sdpMLineIndex, usernameFragment }
}

function iceUfrags (description) {
  return new Set(Array.from(description.sdp.matchAll(/^a=ice-ufrag:(.+)$/gm), match => match[1].trim()))
}

function signalError (message) {
  const err = new Error(message)
  err.code = 'INVALID_SIGNAL'
  return err
}

function noop () {}
