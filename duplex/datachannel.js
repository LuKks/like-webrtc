const { Duplex, getStreamError } = require('streamx')
const b4a = require('b4a')

module.exports = class DataChannelDuplex extends Duplex {
  constructor (dc, opts = {}) {
    super({
      mapWritable,
      highWaterMark: opts.highWaterMark || 16 * 1024
    })

    this.dc = dc
    this.debug = !!opts.debug

    this.maxBufferedAmount = opts.maxBufferedAmount || 64 * 1024
    this.lowBufferedAmount = opts.lowBufferedAmount || Math.floor(this.maxBufferedAmount / 4)
    this.maxMessageSize = opts.maxMessageSize || 64 * 1024

    this._opening = null
    this._writeCallback = null
    this._finalCallback = null
    this._flushTimer = null
    this._closeTimer = null
    this._drainTimer = null
    this._pendingBytes = 0

    this.closeDelay = opts.closeDelay === undefined ? 250 : opts.closeDelay
    this.flushInterval = opts.flushInterval || 5
    this.drainInterval = opts.drainInterval || 16

    dc.binaryType = 'arraybuffer'
    dc.bufferedAmountLowThreshold = this.lowBufferedAmount

    this._onopen = this._onopen.bind(this)
    this._onmessage = this._onmessage.bind(this)
    this._onerror = this._onerror.bind(this)
    this._onclose = this._onclose.bind(this)
    this._ondrain = this._ondrain.bind(this)

    dc.addEventListener('open', this._onopen)
    dc.addEventListener('message', this._onmessage)
    dc.addEventListener('error', this._onerror)
    dc.addEventListener('close', this._onclose)
    dc.addEventListener('bufferedamountlow', this._ondrain)

    this.resume().pause()
  }

  _open (cb) {
    const readyState = this.dc.readyState

    if (isConnecting(readyState)) {
      this._opening = cb
      return
    }

    if (isOpen(readyState)) {
      queueMicrotask(() => this.emit('connect'))
      cb(null)
      return
    }

    cb(new Error('DataChannel closed'))
  }

  _write (data, cb) {
    if (this.debug) {
      console.log('[dc write]', this.dc.label, this.dc.id, this.dc.readyState, data.length)
    }

    if (this.destroying) {
      cb(new Error('Destroyed'))
      return
    }

    try {
      if (!isOpen(this.dc.readyState)) {
        cb(new Error('DataChannel not open'))
        return
      }

      if (data.byteLength > this.maxMessageSize) {
        cb(new Error('DataChannel message too large'))
        return
      }

      this.dc.send(toArrayBuffer(data))
      this._pendingBytes += byteLength(data)

      if (this._hasPendingData()) {
        this._writeCallback = cb
        this._scheduleDrainCheck()
        return
      }

      this._completeWrite(cb)
    } catch (err) {
      cb(err)
    }
  }

  _ondrain () {
    this._markDrained()
    this._maybeDrain()

    this._flushAndClose()
  }

  _onmessage (e) {
    if (this.debug) {
      console.log('DataChannelDuplex _onmessage', this.dc.label, this.dc.id, this.dc.readyState, e.data)
    }

    if (this.destroying) return

    this.push(toBuffer(e.data))
  }

  _onopen () {
    const cb = this._opening
    this._opening = null

    if (cb) cb(null)
    this.emit('connect')
  }

  _onerror (e) {
    this.destroy(e instanceof Error ? e : new Error('DataChannel error'))
  }

  _onclose () {
    this._finishFinal(null)

    if (!this.destroying) {
      this.push(null)
      this.destroy()
    }
  }

  _final (cb) {
    this._finalCallback = cb
    this._flushAndClose()
  }

  _predestroy () {
    const err = getStreamError(this)
    const destroyError = err || new Error('Destroyed')

    this._clearFlushTimer()
    this._clearDrainTimer()

    if (err && !isPrematureClose(err)) {
      this._closeDataChannelNow()
    } else {
      this._closeDataChannel()
    }

    const cb = this._writeCallback
    this._writeCallback = null

    if (cb) cb(destroyError)

    this._finishFinal(destroyError)
  }

  _destroy (cb) {
    this._clearFlushTimer()
    this._clearDrainTimer()

    this.dc.removeEventListener('open', this._onopen)
    this.dc.removeEventListener('message', this._onmessage)
    this.dc.removeEventListener('error', this._onerror)
    this.dc.removeEventListener('close', this._onclose)
    this.dc.removeEventListener('bufferedamountlow', this._ondrain)

    cb(null)
  }

  _flushAndClose () {
    if (!this._finalCallback) return
    if (this._writeCallback) return

    if (isClosed(this.dc.readyState)) {
      this._finishFinal(null)
      return
    }

    if (!isOpen(this.dc.readyState)) {
      this._scheduleFlushCheck()
      return
    }

    if (this._pendingBytes > 0) {
      this._scheduleDrainCheck()
      this._scheduleFlushCheck()
      return
    }

    if (this.dc.bufferedAmount > 0) {
      this._scheduleFlushCheck()
      return
    }

    try {
      this._closeDataChannel()
      this._finishFinal(null)
    } catch (err) {
      this._finishFinal(err)
    }
  }

  _hasPendingData () {
    return (
      this._pendingBytes >= this.maxBufferedAmount ||
      this._bufferedAmount() >= this.maxBufferedAmount
    )
  }

  _markDrained () {
    if (this._pendingBytes === 0) return

    const bufferedAmount = this._bufferedAmount()

    if (bufferedAmount >= this.maxBufferedAmount) {
      return
    }

    if (bufferedAmount > 0) {
      this._pendingBytes = Math.min(
        this._pendingBytes,
        bufferedAmount
      )

      return
    }

    this._pendingBytes = Math.max(
      0,
      this._pendingBytes - this.lowBufferedAmount
    )
  }

  _bufferedAmount () {
    return typeof this.dc.bufferedAmount === 'number'
      ? this.dc.bufferedAmount
      : 0
  }

  _maybeDrain () {
    const cb = this._writeCallback

    if (!cb) return

    if (this._hasPendingData()) {
      this._scheduleDrainCheck()
      return
    }

    this._writeCallback = null
    this._completeWrite(cb)
  }

  _completeWrite (cb) {
    defer(() => cb(null))
  }

  _scheduleDrainCheck () {
    if (this._drainTimer) return

    this._drainTimer = setTimeout(() => {
      this._drainTimer = null
      this._markDrained()
      this._maybeDrain()
      this._flushAndClose()
    }, this.drainInterval)
  }

  _clearDrainTimer () {
    if (!this._drainTimer) return

    clearTimeout(this._drainTimer)
    this._drainTimer = null
  }

  _scheduleFlushCheck () {
    if (this._flushTimer) return

    this._flushTimer = setTimeout(() => {
      this._flushTimer = null
      this._flushAndClose()
    }, this.flushInterval)
  }

  _clearFlushTimer () {
    if (!this._flushTimer) return

    clearTimeout(this._flushTimer)
    this._flushTimer = null
  }

  _closeDataChannel () {
    if (isClosing(this.dc.readyState) || isClosed(this.dc.readyState)) {
      return
    }

    if (this._closeTimer) return

    this._closeTimer = setTimeout(() => {
      this._closeTimer = null
      this._closeDataChannelNow()
    }, this.closeDelay)
  }

  _closeDataChannelNow () {
    if (this._closeTimer) {
      clearTimeout(this._closeTimer)
      this._closeTimer = null
    }

    if (isClosing(this.dc.readyState) || isClosed(this.dc.readyState)) {
      return
    }

    try {
      this.dc.close()
    } catch {}
  }

  _finishFinal (err) {
    const cb = this._finalCallback
    this._finalCallback = null

    this._clearFlushTimer()

    if (cb) cb(err)
  }
}

function mapWritable (data) {
  return toBuffer(data)
}

function toBuffer (data) {
  if (b4a.isBuffer(data)) {
    return data
  }

  return b4a.from(data)
}

function toArrayBuffer (data) {
  return data.buffer.slice(
    data.byteOffset,
    data.byteOffset + data.byteLength
  )
}

function byteLength (data) {
  return data.byteLength || data.length || 0
}

function defer (fn) {
  if (typeof setImmediate === 'function') {
    setImmediate(fn)
    return
  }

  setTimeout(fn, 0)
}

function isOpen (state) {
  return state === 'open' || state === 1
}

function isConnecting (state) {
  return state === 'connecting' || state === 0
}

function isClosed (state) {
  return state === 'closed' || state === 3
}

function isClosing (state) {
  return state === 'closing' || state === 2
}

function isPrematureClose (err) {
  return !!err && (
    err.code === 'ERR_STREAM_PREMATURE_CLOSE' ||
    err.message === 'Premature close' ||
    err.message === 'Writable stream closed prematurely'
  )
}
