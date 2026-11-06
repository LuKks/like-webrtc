// NOTE: I wrote the old code by hand, it worked but it was not behaving perfectly
// I ended up enhancing this file with AI, not ideal for business-critical code
// Mostly for the ICE negotiation restarts, network switches, edge cases, etc
// It saved time as this is a quick one week thing
// TODO: Needs a proper re-write and tests for edge cases

const EventEmitter = require('events')
const { RTCPeerConnection, RTCIceCandidate } = require('webrtc-runtime')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')

// TODO: Peer should be more decoupled to allow more use cases without party signaling

// Don't use arrays for urls
const ICE_SERVERS = [
  { urls: 'stun:turn.leet.ar:3478' },
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun.cloudflare.com:3478' },

  // TODO: If you use werift, it doesn't support TURNS
  // { urls: 'turns:turn.leet.ar:5349?transport=udp', username: 'us3rn4m3', credential: 'p4$$w0rd' },
  // { urls: 'turns:turn.leet.ar:5349?transport=tcp', username: 'us3rn4m3', credential: 'p4$$w0rd' },

  { urls: 'turn:turn.leet.ar:3478?transport=udp', username: 'us3rn4m3', credential: 'p4$$w0rd' },
  { urls: 'turn:turn.leet.ar:3478?transport=tcp', username: 'us3rn4m3', credential: 'p4$$w0rd' }
]

module.exports = class Peer extends EventEmitter {
  constructor (opts = {}) {
    super()

    this.signaling = opts.signaling
    this.token = opts.token || b4a.toString(crypto.randomBytes(32), 'hex')

    this.publicKey = opts.publicKey
    this.remotePublicKey = opts.remotePublicKey

    // Deterministic perfect-negotiation glare role.
    this.polite = !!opts.polite

    // Routing side only. This is not the SDP offerer role.
    this.isInitiator = typeof opts.isInitiator === 'boolean' ? opts.isInitiator : !this.polite

    this.debug = !!opts.debug
    this.trickle = opts.trickle !== false

    this.RTCPeerConnection = opts.RTCPeerConnection || RTCPeerConnection
    this.RTCIceCandidate = opts.RTCIceCandidate || RTCIceCandidate

    this.pc = new this.RTCPeerConnection({
      iceServers: opts.iceServers || ICE_SERVERS,
      iceTransportPolicy: opts.iceTransportPolicy || 'all'
    })

    this.id = (this.isInitiator ? 'initiator' : 'responder') + ':' + String(this.token).slice(0, 8)

    this.closed = false
    this.closing = false
    this.closePromise = null

    this.makingOffer = false
    this.ignoreOffer = false
    this.isSettingRemoteAnswerPending = false

    this.needsNegotiation = false
    this.iceRestartPending = false
    this.iceRestarting = false

    this.negotiationChain = Promise.resolve()
    this.pendingCandidates = []
    this.pendingSignalingMessages = new Map()
    this.receivedSignalingMessages = new Set()
    this.lastReceivedSignalingMessage = null
    this.signalingMessageSeq = 0
    this.flushingSignaling = false

    this.localOfferSeq = 0
    this.remoteOfferSeq = 0
    this.localAnswerSeq = 0
    this.remoteAnswerSeq = 0
    this.negotiationCompleted = 0
    this.iceRestartCompleted = 0
    this._operationSeq = 0

    this.connectTimer = null
    this.disconnectTimer = null
    this.iceRestartTimer = null

    this.connectTimeout = opts.connectTimeout || 30 * 1000
    this.disconnectTimeout = opts.disconnectTimeout || 120 * 1000
    this.iceDisconnectedRestartDelay = opts.iceDisconnectedRestartDelay || 5000
    this.closeTimeout = opts.closeTimeout || 5000
    this.dataChannelCloseTimeout = opts.dataChannelCloseTimeout || 1000

    this.dataChannelsCreated = 0
    this.dataChannels = new Set()
    this.ondatachannel = null
    this.ontrack = null

    this._pcDisposers = []
    this._channelDisposers = new Map()
    this._onSignalingOnline = this._handleSignalingOnline.bind(this)
    this._onSignalingOffline = this._handleSignalingOffline.bind(this)

    this._patchConnection()
    this._setupPeerConnection()

    if (this.signaling && !this.signaling._users && typeof this.signaling.on === 'function') {
      this.signaling.on('online', this._onSignalingOnline)
      this.signaling.on('offline', this._onSignalingOffline)
    }

    this._startInitialConnectionTimer()
  }

  get connection () {
    return this.pc
  }

  get connectionState () {
    return this.pc.connectionState
  }

  getStats (...args) {
    return this.pc.getStats(...args)
  }

  createDataChannel (label, options) {
    this._assertOpen()

    const hadApplicationSection = this._hasApplicationMediaSection()
    const channel = this._rawCreateDataChannel(label, options)

    this.dataChannelsCreated++
    this._trackDataChannel(channel)

    this._log('createDataChannel', {
      label,
      hadApplicationSection,
      channelId: channel && channel.id,
      dataChannelsCreated: this.dataChannelsCreated,
      dataChannelsOpen: this.dataChannels.size
    })

    // Important:
    // Once SCTP exists, new data channels are in-band and do not need SDP.
    // Creating offers for every later data channel is what caused late offers,
    // extra responder peers, and leaked wrtc handles.
    if (!hadApplicationSection) {
      this.requestNegotiation('initial data channel')
    }

    if (this._iceNeedsRepair()) {
      this.restartIce('createDataChannel while ICE unhealthy')
    }

    return channel
  }

  addTrack (...args) {
    this._assertOpen()
    this._log('addTrack start', this._mediaArgs(args))

    if (!this._rawAddTrack) {
      throw new Error('RTCPeerConnection.addTrack is not supported')
    }

    const sender = this._rawAddTrack(...args)
    this._log('addTrack done', { sender: this._objectId(sender) })

    this.requestNegotiation('addTrack')

    return sender
  }

  addTransceiver (...args) {
    this._assertOpen()
    this._log('addTransceiver start', this._mediaArgs(args))

    if (!this._rawAddTransceiver) {
      throw new Error('RTCPeerConnection.addTransceiver is not supported')
    }

    const transceiver = this._rawAddTransceiver(...args)
    this._log('addTransceiver done', { transceiver: this._objectId(transceiver) })

    this.requestNegotiation('addTransceiver')

    return transceiver
  }

  removeTrack (...args) {
    this._assertOpen()
    this._log('removeTrack start', this._mediaArgs(args))

    if (!this._rawRemoveTrack) {
      throw new Error('RTCPeerConnection.removeTrack is not supported')
    }

    const result = this._rawRemoveTrack(...args)
    this._log('removeTrack done')

    this.requestNegotiation('removeTrack')

    return result
  }

  restartIce (reason) {
    if (this._isClosing()) return this.negotiationChain

    if (this.iceRestartPending) {
      this._log('restartIce already pending:', reason)
      return this.negotiationChain
    }

    if (this.iceRestarting && this.pc.signalingState !== 'stable') {
      this._resendLocalOffer('restartIce while restart offer pending')
      return this.negotiationChain
    }

    this._log('restartIce requested:', reason)

    this.iceRestartPending = true

    try {
      if (typeof this.pc.restartIce === 'function') {
        this.pc.restartIce()
      }
    } catch (err) {
      this._log('native restartIce failed', err)
    }

    return this.requestNegotiation('restartIce')
  }

  requestNegotiation (reason) {
    if (this._isClosing()) return this.negotiationChain

    this.needsNegotiation = true

    if (!this._canSignal()) return this.negotiationChain

    this._log('negotiation requested:', reason)

    const run = this._enqueue(() => this._maybeNegotiate())

    run.catch(err => {
      this._log('negotiation failed', err)
    })

    return run
  }

  signal (data) {
    try {
      if (!data || typeof data !== 'object' || !data.body || typeof data.body !== 'object') {
        throw new Error('Invalid signal')
      }
    } catch (err) {
      this._log('signal failed', this._errorInfo(err))
      return
    }

    let operation = null

    if (data.name === 'webrtc-negotiation') {
      operation = this.handleDescription(data.body.description, data.body.messageId)
    } else if (data.name === 'webrtc-ice') {
      operation = this.handleCandidate(data.body.candidate, data.body.messageId)
    } else if (data.name === 'webrtc-ack') {
      this.handleAck(data.body.ack)
      return
    } else {
      this._log('signal failed', this._errorInfo(new Error('Unknown signal: ' + data.name)))
      return
    }

    operation.then(() => this.sendAck(data.body.messageId)).catch(err => {
      this._log('signal failed', this._errorInfo(err))
    })
  }

  async handleDescription (description, messageId) {
    return this._enqueue(async () => {
      if (this._isClosing()) {
        this._log('handleDescription skipped: closing')
        return
      }
      if (!description || !description.type) {
        this._log('handleDescription skipped: malformed', this._descriptionInfo(description))
        return
      }

      if (messageId && this.receivedSignalingMessages.has(messageId)) return

      if (description.type !== 'offer' && description.type !== 'answer') {
        this._log('handleDescription skipped: unsupported type', description.type)
        return
      }

      const pc = this.pc

      this._log('handleDescription start', {
        type: description.type,
        ufrag: this._sdpIceUfrag(description)
      })

      if (
        description.type === 'answer' &&
        pc.signalingState !== 'have-local-offer'
      ) {
        this._log('drop stale answer in state:', pc.signalingState)
        return
      }

      const readyForOffer =
        !this.makingOffer &&
        (
          pc.signalingState === 'stable' ||
          this.isSettingRemoteAnswerPending
        )

      const offerCollision =
        description.type === 'offer' &&
        !readyForOffer

      this.ignoreOffer = !this.polite && offerCollision

      if (this.ignoreOffer) {
        if (messageId) this._rememberSignalingMessage(messageId)
        this._log('ignore colliding offer because impolite')
        return
      }

      try {
        this.isSettingRemoteAnswerPending = description.type === 'answer'

        if (offerCollision) {
          await Promise.all([
            this._rollback(),
            pc.setRemoteDescription(description)
          ])
        } else {
          await pc.setRemoteDescription(description)
        }
      } finally {
        this.isSettingRemoteAnswerPending = false
      }

      if (this._isClosing()) return

      this._log('setRemoteDescription done', {
        type: description.type,
        ufrag: this._sdpIceUfrag(description)
      })

      await this._flushPendingCandidates()

      if (this._isClosing()) return

      if (description.type === 'offer') {
        await this._answerOffer(description)
      } else {
        this._acceptAnswer(description)
      }

      if (messageId) this._rememberSignalingMessage(messageId)
    })
  }

  async handleCandidate (candidate, messageId) {
    return this._enqueue(async () => {
      this._log('handleCandidate start', this._candidateInfo(candidate))
      if (this._isClosing() || !this.trickle) {
        this._log('handleCandidate skipped', { closing: this._isClosing(), trickle: this.trickle, ignoreOffer: this.ignoreOffer })
        return
      }

      if (messageId && this.receivedSignalingMessages.has(messageId)) return
      if (messageId) this._rememberSignalingMessage(messageId)

      if (candidate && !candidate.candidate) {
        this._log('ignore malformed ICE candidate', candidate)
        return
      }

      const ice = candidate ? new this.RTCIceCandidate(candidate) : null

      if (this.ignoreOffer || !this.pc.remoteDescription) {
        this.pendingCandidates.push(ice)
        this._log('candidate queued until remoteDescription', { pending: this.pendingCandidates.length })
        return
      }

      const remoteUfrag = this._sdpIceUfrag(this.pc.remoteDescription)
      if (
        candidate &&
        candidate.usernameFragment &&
        remoteUfrag &&
        candidate.usernameFragment !== remoteUfrag
      ) {
        this._log('ignore stale ICE candidate', {
          candidateUfrag: candidate.usernameFragment,
          remoteUfrag
        })
        return
      }

      try {
        await this.pc.addIceCandidate(ice)
        this._log('addIceCandidate done', this._candidateInfo(candidate))
      } catch (err) {
        this._log('addIceCandidate failed', err)
      }
    })
  }

  close () {
    if (this.closePromise) return this.closePromise
    if (this.closed) return Promise.resolve()

    this.closing = true
    this.closePromise = this._close()

    return this.closePromise
  }

  async _close () {
    if (this.closed) return

    this.closing = true
    this._log('close start')
    this.needsNegotiation = false
    this.iceRestartPending = false
    this.iceRestarting = false
    this.ignoreOffer = true
    this.pendingCandidates.length = 0
    this.pendingSignalingMessages.clear()

    this._clearConnectTimer()
    this._clearDisconnectTimer()
    this._clearIceRestartTimer()
    this._removeSignalingOnlineListener()
    this._disposePeerConnectionListeners()

    try {
      await this._closeDataChannels()

      const closeWaiter = this._createPeerConnectionCloseWaiter()

      try {
        const result = this._rawClose()

        closeWaiter.check()

        if (result && typeof result.then === 'function') {
          await result.catch(err => {
            this._log('native close failed', err)
          })

          closeWaiter.check()
        }

        await closeWaiter.wait
      } finally {
        closeWaiter.finish()
      }
    } finally {
      this._disposeDataChannelListeners()

      this.closed = true
      this.closing = false

      this._log('close done')
      this.emit('close')
    }
  }

  async _maybeNegotiate () {
    if (this._isClosing()) return
    if (!this.needsNegotiation && !this.iceRestartPending) return
    if (!this._canSignal()) return
    if (this.makingOffer) return

    const pc = this.pc

    if (pc.signalingState !== 'stable') {
      this._log('negotiation deferred because not stable')
      return
    }

    const restartingIce = this.iceRestartPending

    this.makingOffer = true
    let committed = false

    try {
      const options = restartingIce ? { iceRestart: true } : {}

      this._log('createOffer start', options)

      const offer = await pc.createOffer(options)

      if (this._isClosing()) return

      this._log('createOffer done', {
        iceRestart: restartingIce,
        ufrag: this._sdpIceUfrag(offer)
      })

      if (pc.signalingState !== 'stable') {
        this.needsNegotiation = true
        this._log('offer aborted because signaling state changed')
        return
      }

      await pc.setLocalDescription(offer)

      if (this._isClosing()) return

      await this._waitForIceGathering()

      if (this._isClosing()) return

      if (
        !pc.localDescription ||
        pc.localDescription.type !== 'offer'
      ) {
        return
      }

      this.localOfferSeq++
      this._log('setLocalDescription offer done', {
        iceRestart: restartingIce,
        ufrag: this._sdpIceUfrag(pc.localDescription)
      })

      this._sendDescription(pc.localDescription)
      committed = true

      this.needsNegotiation = false
      if (restartingIce) {
        this.iceRestartPending = false
        this.iceRestarting = true
      }
    } finally {
      if (!committed && !this._isClosing()) {
        this.needsNegotiation = true
        if (restartingIce) this.iceRestartPending = true
      }

      this.makingOffer = false
    }
  }

  async _answerOffer (description) {
    const pc = this.pc

    this.remoteOfferSeq++
    this.needsNegotiation = false

    const answer = await pc.createAnswer()

    if (this._isClosing()) return

    this._log('createAnswer done', {
      ufrag: this._sdpIceUfrag(answer)
    })

    await pc.setLocalDescription(answer)

    if (this._isClosing()) return

    await this._waitForIceGathering()

    if (this._isClosing()) return

    this.localAnswerSeq++
    this.negotiationCompleted++

    this._log('setLocalDescription answer done', {
      ufrag: this._sdpIceUfrag(pc.localDescription),
      negotiationCompleted: this.negotiationCompleted
    })

    this._sendDescription(pc.localDescription)

    if (this.dataChannelsCreated > 0 && !this._hasApplicationMediaSection()) this.needsNegotiation = true
    if (this.needsNegotiation || this.iceRestartPending) this.requestNegotiation('pending after answering')
  }

  _acceptAnswer (description) {
    this.remoteAnswerSeq++

    const completedIceRestart = this.iceRestarting

    this.iceRestarting = false
    this.negotiationCompleted++

    if (completedIceRestart) {
      this.iceRestartCompleted++
    }

    this._log('answer accepted', {
      completedIceRestart,
      negotiationCompleted: this.negotiationCompleted,
      iceRestartCompleted: this.iceRestartCompleted,
      ufrag: this._sdpIceUfrag(description)
    })

    if (
      (this.needsNegotiation || this.iceRestartPending) &&
      this.pc.signalingState === 'stable'
    ) {
      this.requestNegotiation('pending after answer')
    }
  }

  async _rollback () {
    const pc = this.pc

    if (pc.signalingState === 'stable') return

    this._log('rollback local description')
    this.pendingSignalingMessages.delete('description:offer')
    for (const [key, message] of this.pendingSignalingMessages) {
      if (message.kind === 'candidate') this.pendingSignalingMessages.delete(key)
    }

    await pc.setLocalDescription({ type: 'rollback' })
  }

  async _flushPendingCandidates () {
    while (
      !this._isClosing() &&
      this.pendingCandidates.length &&
      this.pc.remoteDescription
    ) {
      const candidate = this.pendingCandidates.shift()

      const candidateUfrag = candidate && candidate.usernameFragment
      const remoteUfrag = this._sdpIceUfrag(this.pc.remoteDescription)
      if (candidateUfrag && remoteUfrag && candidateUfrag !== remoteUfrag) {
        this._log('ignore stale pending ICE candidate', {
          candidateUfrag,
          remoteUfrag
        })
        continue
      }

      try {
        await this.pc.addIceCandidate(candidate)
        this._log('pending candidate added')
      } catch (err) {
        this._log('pending candidate failed', err)
      }
    }
  }

  _sendDescription (description) {
    if (!description) {
      this._log('sendDescription skipped: empty')
      return
    }
    if (this._isClosing()) {
      this._log('sendDescription skipped: closing', this._descriptionInfo(description))
      return
    }
    let message = this.pendingSignalingMessages.get('description:' + description.type)
    if (!message || message.description !== description) {
      message = {
        kind: 'description',
        messageId: this._nextSignalingMessageId(),
        description,
        sent: false
      }
      this.pendingSignalingMessages.set('description:' + description.type, message)
    }

    if (!this._canSignal()) {
      this._log('sendDescription skipped: signaling offline', this._descriptionInfo(description))
      return
    }

    this._log('sendDescription', {
      type: description.type,
      isInitiator: this.isInitiator,
      ufrag: this._sdpIceUfrag(description)
    })

    try {
      const delivered = this._sendSignal('webrtc-negotiation', {
        messageId: message.messageId,
        token: this.token,
        isInitiator: this.isInitiator,
        remotePublicKey: this.remotePublicKey,
        description: {
          type: description.type,
          sdp: description.sdp
        }
      })
      if (delivered === false) {
        this._log('sendDescription not delivered and queued', this._descriptionInfo(description))
        return
      }
      message.sent = true
      this._log('sendDescription done', this._descriptionInfo(description))
    } catch (err) {
      this._log('sendDescription failed and queued', this._errorInfo(err))
    }
  }

  _sendCandidate (candidate) {
    if (!this.trickle) {
      this._log('sendCandidate skipped: trickle disabled', this._candidateInfo(candidate))
      return
    }
    if (this._isClosing()) {
      this._log('sendCandidate skipped: closing', this._candidateInfo(candidate))
      return
    }
    let message = this.pendingSignalingMessages.get('candidate:' + this._candidateKey(candidate))
    if (!message) {
      message = {
        kind: 'candidate',
        messageId: this._nextSignalingMessageId(),
        candidate,
        sent: false
      }
      this.pendingSignalingMessages.set('candidate:' + this._candidateKey(candidate), message)
    }

    if (!this._canSignal()) {
      this._log('sendCandidate skipped: signaling offline', this._candidateInfo(candidate))
      return
    }

    try {
      const delivered = this._sendSignal('webrtc-ice', {
        messageId: message.messageId,
        token: this.token,
        isInitiator: this.isInitiator,
        remotePublicKey: this.remotePublicKey,
        candidate: candidate
          ? {
              candidate: candidate.candidate,
              sdpMLineIndex: candidate.sdpMLineIndex,
              sdpMid: candidate.sdpMid,
              usernameFragment: candidate.usernameFragment
            }
          : null
      })
      if (delivered === false) {
        this._log('sendCandidate not delivered and queued', this._candidateInfo(candidate))
        return
      }
      message.sent = true
      this._log('sendCandidate done', this._candidateInfo(candidate))
    } catch (err) {
      this._log('sendCandidate failed and queued', this._errorInfo(err))
    }
  }

  sendAck (messageId) {
    if (!messageId || this._isClosing() || !this._canSignal()) return

    try {
      this._sendSignal('webrtc-ack', {
        token: this.token,
        isInitiator: this.isInitiator,
        remotePublicKey: this.remotePublicKey,
        ack: messageId
      })
    } catch (err) {
      this._log('send acknowledgment failed', this._errorInfo(err))
    }
  }

  handleAck (messageId) {
    if (!messageId) return

    for (const [key, message] of this.pendingSignalingMessages) {
      if (message.messageId === messageId) {
        this.pendingSignalingMessages.delete(key)
        this._log('signaling message acknowledged', { messageId })
        return
      }
    }
  }

  _nextSignalingMessageId () {
    return this.id + ':' + (++this.signalingMessageSeq)
  }

  _rememberSignalingMessage (messageId) {
    this.lastReceivedSignalingMessage = messageId
    this.receivedSignalingMessages.add(messageId)

    if (this.receivedSignalingMessages.size > 512) {
      this.receivedSignalingMessages.delete(this.receivedSignalingMessages.values().next().value)
    }
  }

  _candidateKey (candidate) {
    if (!candidate) return 'end'
    return [candidate.candidate, candidate.sdpMid, candidate.sdpMLineIndex, candidate.usernameFragment].join('|')
  }

  _enqueue (fn) {
    const operation = ++this._operationSeq
    const run = this.negotiationChain
      .catch(() => {})
      .then(() => {
        this._log('queue start', { operation })
        if (this._isClosing()) return
        return fn()
      })
      .then(result => {
        this._log('queue done', { operation })
        return result
      })

    this.negotiationChain = run.catch(err => {
      this._log('queued operation failed', err)
    })

    return run
  }

  _patchConnection () {
    const pc = this.pc

    this._rawClose = pc.close.bind(pc)
    pc.close = () => this.close()

    this._rawCreateDataChannel = pc.createDataChannel.bind(pc)
    pc.createDataChannel = (label, options) => this.createDataChannel(label, options)

    if (typeof pc.addTrack === 'function') {
      this._rawAddTrack = pc.addTrack.bind(pc)
      pc.addTrack = (...args) => this.addTrack(...args)
    }

    if (typeof pc.addTransceiver === 'function') {
      this._rawAddTransceiver = pc.addTransceiver.bind(pc)
      pc.addTransceiver = (...args) => this.addTransceiver(...args)
    }

    if (typeof pc.removeTrack === 'function') {
      this._rawRemoveTrack = pc.removeTrack.bind(pc)
      pc.removeTrack = (...args) => this.removeTrack(...args)
    }
  }

  _setupPeerConnection () {
    const pc = this.pc

    this._addPeerConnectionListener('negotiationneeded', () => {
      if (this.makingOffer) {
        this._log('ignore negotiationneeded while making offer')
        return
      }

      this.requestNegotiation('onnegotiationneeded')
    })

    this._addPeerConnectionListener('icecandidate', event => {
      this._sendCandidate(event.candidate || null)
    })

    this._addPeerConnectionListener('icecandidateerror', event => {
      this._log('icecandidateerror', {
        address: event.address,
        port: event.port,
        url: event.url,
        errorCode: event.errorCode,
        errorText: event.errorText
      })
    })

    this._addPeerConnectionListener('datachannel', event => {
      const channel = event && event.channel

      this._trackDataChannel(channel)

      this._log('datachannel', channel && channel.label)

      this.emit('datachannel', channel, event)

      if (typeof this.ondatachannel === 'function') {
        this.ondatachannel(event)
      }
    })

    this._addPeerConnectionListener('track', event => {
      this._log('track', this._trackInfo(event))
      this.emit('track', event)

      if (typeof this.ontrack === 'function') {
        this.ontrack(event)
      }
    })

    this._addPeerConnectionListener('iceconnectionstatechange', () => {
      this._updateConnectionState('ice')
    })

    this._addPeerConnectionListener('connectionstatechange', () => {
      this._updateConnectionState('connection')
    })

    this._addPeerConnectionListener('signalingstatechange', () => {
      this._log('signalingState:', pc.signalingState)

      if (
        pc.signalingState === 'stable' &&
        (this.needsNegotiation || this.iceRestartPending)
      ) {
        this.requestNegotiation('stable with pending work')
      }
    })
  }

  _updateConnectionState (source) {
    const state = this.pc.connectionState
    const iceState = this.pc.iceConnectionState

    this._log(source + ' state:', state, iceState)

    if (state === 'closed' || iceState === 'closed') {
      this.close()
      return
    }

    if (this._isConnected()) {
      this._clearConnectTimer()
      this._clearDisconnectTimer()
      this._clearIceRestartTimer()
      return
    }

    if (state === 'connecting' || iceState === 'checking') {
      this._clearDisconnectTimer()
      return
    }

    if (this._iceNeedsRepair()) {
      if (state === 'failed' || iceState === 'failed') {
        this.restartIce(source + ' failed')
      } else {
        this._scheduleIceRestart(source + ' disconnected')
      }

      this._startDisconnectTimer()
    }
  }

  _addPeerConnectionListener (name, listener) {
    const pc = this.pc

    const wrapped = event => {
      this._log('peer connection event', { name, event: this._eventInfo(event) })
      if (this._isClosing()) return
      listener(event)
    }

    let dispose = noop

    if (typeof pc.addEventListener === 'function') {
      pc.addEventListener(name, wrapped)

      dispose = () => {
        if (typeof pc.removeEventListener === 'function') {
          pc.removeEventListener(name, wrapped)
        }
      }
    } else {
      const prop = 'on' + name
      const prev = pc[prop]

      const onevent = event => {
        if (typeof prev === 'function') {
          prev.call(pc, event)
        }

        wrapped(event)
      }

      pc[prop] = onevent

      dispose = () => {
        if (pc[prop] === onevent) {
          pc[prop] = prev
        }
      }
    }

    this._pcDisposers.push(dispose)
  }

  _handleSignalingOnline () {
    if (this._isClosing()) return

    this._log('signaling online')
    // Re-acknowledge on the new socket so the relay refreshes this side's route.
    this.sendAck(this.lastReceivedSignalingMessage)
    this._flushSignalingQueue()

    if (this.pc.signalingState === 'have-local-offer') {
      if (!this.makingOffer) {
        this._resendLocalOffer('signaling reconnected')
      }

      return
    }

    if (this.pc.signalingState !== 'stable') {
      return
    }

    if (this._iceNeedsRepair()) {
      this.restartIce('signaling online with unhealthy ICE')
      return
    }

    if (this.needsNegotiation || this.iceRestartPending) {
      this.requestNegotiation('signaling online with pending work')
    }
  }

  _handleSignalingOffline () {
    this._log('signaling offline')
  }

  _flushSignalingQueue () {
    if (this.flushingSignaling || !this._canSignal() || this._isClosing()) return

    this.flushingSignaling = true

    try {
      for (const message of this.pendingSignalingMessages.values()) {
        if (message.kind === 'description') this._sendDescription(message.description)
        else this._sendCandidate(message.candidate)
      }
    } finally {
      this.flushingSignaling = false
    }
  }

  _resendLocalOffer (reason) {
    const message = this.pendingSignalingMessages.get('description:offer')
    const description = message ? message.description : this.pc.localDescription

    if (
      !description ||
      description.type !== 'offer'
    ) {
      return
    }

    this._log('resend local offer:', reason)

    this._sendDescription(description)
  }

  _scheduleIceRestart (reason) {
    if (this._isClosing()) return
    if (this.iceRestartTimer) return
    if (this.iceRestartPending) return

    this._log('schedule ICE restart:', reason)

    this.iceRestartTimer = setTimeout(() => {
      this.iceRestartTimer = null

      if (this._isClosing()) return

      if (this._iceNeedsRepair()) {
        this.restartIce(reason)
      }
    }, this.iceDisconnectedRestartDelay)
  }

  _startInitialConnectionTimer () {
    this._clearConnectTimer()

    this.connectTimer = setTimeout(() => {
      this.connectTimer = null

      if (this._isClosing()) return

      if (this._isConnected()) {
        return
      }

      this._log('initial connection timeout')
      this.close()
    }, this.connectTimeout)
  }

  _startDisconnectTimer () {
    if (this._isClosing()) return
    if (this.disconnectTimer) return

    this.disconnectTimer = setTimeout(() => {
      this.disconnectTimer = null

      if (this._isClosing()) return

      if (!this._iceNeedsRepair()) {
        return
      }

      if (this._hasLiveDataChannel()) {
        this.restartIce('disconnect timeout with live data channel')
        return
      }

      this._log('disconnect timeout without live channels; closing')
      this.close()
    }, this.disconnectTimeout)
  }

  _clearConnectTimer () {
    if (!this.connectTimer) return

    clearTimeout(this.connectTimer)
    this.connectTimer = null
  }

  _clearDisconnectTimer () {
    if (!this.disconnectTimer) return

    clearTimeout(this.disconnectTimer)
    this.disconnectTimer = null
  }

  _clearIceRestartTimer () {
    if (!this.iceRestartTimer) return

    clearTimeout(this.iceRestartTimer)
    this.iceRestartTimer = null
  }

  _removeSignalingOnlineListener () {
    if (
      this.signaling &&
      typeof this.signaling.removeListener === 'function' &&
      this._onSignalingOnline
    ) {
      this.signaling.removeListener('online', this._onSignalingOnline)
    }

    if (
      this.signaling &&
      typeof this.signaling.removeListener === 'function' &&
      this._onSignalingOffline
    ) {
      this.signaling.removeListener('offline', this._onSignalingOffline)
    }
  }

  _disposePeerConnectionListeners () {
    while (this._pcDisposers.length) {
      const dispose = this._pcDisposers.pop()

      try {
        dispose()
      } catch (err) {
        this._log('dispose pc listener failed', err)
      }
    }
  }

  async _closeDataChannels () {
    const waits = []

    for (const channel of Array.from(this.dataChannels)) {
      waits.push(this._waitDataChannelClosed(channel))

      try {
        if (
          channel &&
          typeof channel.close === 'function' &&
          channel.readyState !== 'closed'
        ) {
          channel.close()
        }
      } catch (err) {
        this._log('data channel close failed', err)
      }
    }

    if (waits.length) {
      await Promise.all(waits)
    }
  }

  _waitDataChannelClosed (channel) {
    if (!channel || channel.readyState === 'closed') {
      return Promise.resolve()
    }

    return new Promise(resolve => {
      let done = false
      let timeout = null
      let dispose = noop

      const finish = () => {
        if (done) return

        done = true

        if (timeout) {
          clearTimeout(timeout)
          timeout = null
        }

        dispose()
        resolve()
      }

      dispose = addListener(channel, 'close', finish)
      timeout = setTimeout(finish, this.dataChannelCloseTimeout)
    })
  }

  _disposeDataChannelListeners () {
    for (const channel of Array.from(this.dataChannels)) {
      this._forgetDataChannel(channel)
    }

    this.dataChannels.clear()
    this._channelDisposers.clear()
  }

  _trackDataChannel (channel) {
    if (!channel) return
    if (this.dataChannels.has(channel)) return

    this.dataChannels.add(channel)

    const disposers = []

    this._log('data channel tracked', this._channelInfo(channel))

    disposers.push(addListener(channel, 'open', () => {
      this._log('data channel open', this._channelInfo(channel))
    }))

    disposers.push(addListener(channel, 'close', () => {
      this._log('data channel close', this._channelInfo(channel))
      this._forgetDataChannel(channel)
    }))

    disposers.push(addListener(channel, 'message', event => {
      this._log('data channel message', {
        ...this._channelInfo(channel),
        message: this._messageInfo(event && event.data)
      })
    }))

    disposers.push(addListener(channel, 'error', err => {
      this._log('data channel error', { ...this._channelInfo(channel), error: this._errorInfo(err) })
    }))

    this._channelDisposers.set(channel, disposers)
  }

  _forgetDataChannel (channel) {
    const disposers = this._channelDisposers.get(channel)

    if (disposers) {
      while (disposers.length) {
        const dispose = disposers.pop()

        try {
          dispose()
        } catch (err) {
          this._log('dispose data channel listener failed', err)
        }
      }
    }

    this._channelDisposers.delete(channel)
    this.dataChannels.delete(channel)
  }

  _createPeerConnectionCloseWaiter () {
    let done = false
    let settleTimer = null
    let timeout = null
    let resolveWait = null

    const finish = () => {
      if (done) return

      done = true

      if (settleTimer) {
        clearTimeout(settleTimer)
        settleTimer = null
      }

      if (timeout) {
        clearTimeout(timeout)
        timeout = null
      }

      resolveWait()
    }

    const check = () => {
      if (done) return
      if (!this._isPeerConnectionClosed()) return
      if (settleTimer) return

      settleTimer = setTimeout(finish, 25)
    }

    const wait = new Promise(resolve => {
      resolveWait = resolve
      timeout = setTimeout(finish, this.closeTimeout)
    })

    return { wait, check, finish }
  }

  _isPeerConnectionClosed () {
    return (
      this.pc.connectionState === 'closed' ||
      this.pc.iceConnectionState === 'closed' ||
      this.pc.signalingState === 'closed'
    )
  }

  _isConnected () {
    return (
      this.pc.connectionState === 'connected' ||
      this.pc.iceConnectionState === 'connected' ||
      this.pc.iceConnectionState === 'completed'
    )
  }

  _iceNeedsRepair () {
    if (this.pc.connectionState === 'connected') {
      return false
    }

    return (
      this.pc.connectionState === 'failed' ||
      this.pc.connectionState === 'disconnected' ||
      this.pc.iceConnectionState === 'failed' ||
      this.pc.iceConnectionState === 'disconnected'
    )
  }

  _hasLiveDataChannel () {
    for (const channel of this.dataChannels) {
      if (
        channel &&
        (
          channel.readyState === 'open' ||
          channel.readyState === 'connecting'
        )
      ) {
        return true
      }
    }

    return false
  }

  _hasApplicationMediaSection () {
    const descriptions = [
      this.pc.localDescription,
      this.pc.remoteDescription,
      this.pc.currentLocalDescription,
      this.pc.currentRemoteDescription,
      this.pc.pendingLocalDescription,
      this.pc.pendingRemoteDescription
    ]

    for (const description of descriptions) {
      if (
        description &&
        typeof description.sdp === 'string' &&
        /^m=application /m.test(description.sdp)
      ) {
        return true
      }
    }

    return false
  }

  _sdpIceUfrag (description) {
    if (!description || typeof description.sdp !== 'string') {
      return null
    }

    const match = description.sdp.match(/^a=ice-ufrag:(.+)$/m)

    return match ? match[1] : null
  }

  _waitForIceGathering () {
    if (this.trickle || this.pc.iceGatheringState === 'complete') {
      return Promise.resolve()
    }

    this._log('wait for ICE gathering')

    return new Promise(resolve => {
      let dispose = noop
      let timeout = null

      const finish = () => {
        clearTimeout(timeout)
        dispose()
        this._log('ICE gathering wait done', this.pc.iceGatheringState)
        resolve()
      }

      timeout = setTimeout(finish, this.closeTimeout)

      dispose = addListener(this.pc, 'icegatheringstatechange', () => {
        if (this.pc.iceGatheringState === 'complete') finish()
      })

      if (this.pc.iceGatheringState === 'complete') finish()
    })
  }

  _sendSignal (name, body) {
    if (this.signaling) {
      return this.signaling.event(name, body)
    }

    return this.emit('signal', { name, body })
  }

  _canSignal () {
    if (this.signaling) {
      return !this.signaling.closed
    }

    return true
  }

  _assertOpen () {
    if (
      this.closed ||
      this.closing ||
      this.pc.signalingState === 'closed' ||
      this.pc.connectionState === 'closed'
    ) {
      throw new Error('Peer is closed')
    }
  }

  _isClosing () {
    return this.closed || this.closing
  }

  _state () {
    return {
      signalingState: this.pc.signalingState,
      iceConnectionState: this.pc.iceConnectionState,
      connectionState: this.pc.connectionState,
      iceGatheringState: this.pc.iceGatheringState,

      closing: this.closing,
      closed: this.closed,

      makingOffer: this.makingOffer,
      ignoreOffer: this.ignoreOffer,
      needsNegotiation: this.needsNegotiation,
      iceRestartPending: this.iceRestartPending,
      iceRestarting: this.iceRestarting,

      polite: this.polite,

      localOfferSeq: this.localOfferSeq,
      remoteOfferSeq: this.remoteOfferSeq,
      localAnswerSeq: this.localAnswerSeq,
      remoteAnswerSeq: this.remoteAnswerSeq,
      negotiationCompleted: this.negotiationCompleted,
      iceRestartCompleted: this.iceRestartCompleted,

      dataChannelsCreated: this.dataChannelsCreated,
      dataChannelsOpen: this.dataChannels.size
    }
  }

  _descriptionInfo (description) {
    if (!description) return null

    return {
      type: description.type,
      sdpLength: typeof description.sdp === 'string' ? description.sdp.length : 0,
      ufrag: this._sdpIceUfrag(description)
    }
  }

  _candidateInfo (candidate) {
    if (!candidate) return { end: true }

    return {
      candidate: typeof candidate.candidate === 'string' ? candidate.candidate.slice(0, 80) : null,
      mid: candidate.sdpMid,
      mline: candidate.sdpMLineIndex,
      ufrag: candidate.usernameFragment
    }
  }

  _channelInfo (channel) {
    if (!channel) return null

    return {
      label: channel.label,
      id: channel.id,
      protocol: channel.protocol,
      readyState: channel.readyState,
      bufferedAmount: channel.bufferedAmount
    }
  }

  _trackInfo (event) {
    return {
      track: this._objectId(event && event.track),
      kind: event && event.track && event.track.kind,
      streams: event && Array.isArray(event.streams) ? event.streams.length : undefined,
      transceiver: this._objectId(event && event.transceiver),
      receiver: this._objectId(event && event.receiver)
    }
  }

  _messageInfo (data) {
    if (typeof data === 'string') return { type: 'string', length: data.length }
    if (data && typeof data.byteLength === 'number') return { type: data.constructor && data.constructor.name, length: data.byteLength }
    if (data && typeof data.length === 'number') return { type: data.constructor && data.constructor.name, length: data.length }
    return { type: typeof data }
  }

  _eventInfo (event) {
    if (!event) return null

    return {
      type: event.type,
      state: event.state,
      candidate: event.candidate ? this._candidateInfo(event.candidate) : undefined,
      channel: event.channel ? this._channelInfo(event.channel) : undefined,
      track: event.track ? this._trackInfo(event) : undefined,
      errorCode: event.errorCode,
      errorText: event.errorText
    }
  }

  _objectId (value) {
    if (!value || typeof value !== 'object') return null
    return value.id || value.label || value.mid || value.kind || (value.constructor && value.constructor.name)
  }

  _mediaArgs (args) {
    return args.map(arg => this._objectId(arg))
  }

  _errorInfo (err) {
    if (!err) return null
    return { name: err.name, message: err.message, code: err.code }
  }

  _log (...args) {
    if (!this.debug) return

    console.log('[Peer %s]', this.id, ...args, this._state())
  }
}

function addListener (target, name, listener) {
  if (!target) return noop

  if (typeof target.addEventListener === 'function') {
    target.addEventListener(name, listener)

    return () => {
      if (typeof target.removeEventListener === 'function') {
        target.removeEventListener(name, listener)
      }
    }
  }

  if (typeof target.on === 'function') {
    target.on(name, listener)

    return () => {
      if (typeof target.removeListener === 'function') {
        target.removeListener(name, listener)
      } else if (typeof target.off === 'function') {
        target.off(name, listener)
      }
    }
  }

  const prop = 'on' + name
  const prev = target[prop]

  const onevent = function (event) {
    if (typeof prev === 'function') {
      prev.call(target, event)
    }

    listener(event)
  }

  target[prop] = onevent

  return () => {
    if (target[prop] === onevent) {
      target[prop] = prev
    }
  }
}

function noop () {}
