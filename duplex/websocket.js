const { Duplex, getStreamError } = require('streamx')
const b4a = require('b4a')

const WS_CONNECTING = 0
const WS_OPEN = 1

module.exports = class WebSocketDuplex extends Duplex {
  constructor (ws, opts = {}) {
    super({
      mapWritable,
      highWaterMark: opts.highWaterMark || 64 * 1024
    })

    this.ws = ws
    this.ws.binaryType = 'arraybuffer'
    this.maxBufferedAmount = opts.maxBufferedAmount || 8 * 1024 * 1024
    this.drainInterval = opts.drainInterval || 4

    this._opening = null
    this._writeCallback = null
    this._closed = false
    this.debug = typeof opts.debug === 'function' ? opts.debug : null

    this._onopen = this._onopen.bind(this)
    this._onmessage = this._onmessage.bind(this)
    this._onerror = this._onerror.bind(this)
    this._onclose = this._onclose.bind(this)

    ws.addEventListener('open', this._onopen)
    ws.addEventListener('message', this._onmessage)
    ws.addEventListener('error', this._onerror)
    ws.addEventListener('close', this._onclose)

    this.resume().pause()
  }

  _open (cb) {
    switch (this.ws.readyState) {
      case WS_CONNECTING:
        this._opening = cb
        return

      case WS_OPEN:
        cb(null)
        queueMicrotask(() => this.emit('connect'))
        return

      default:
        cb(new Error('WebSocket is closed'))
    }
  }

  _onopen () {
    this._debug('open')

    const cb = this._opening

    this._opening = null

    if (cb) {
      cb(null)
    }

    this.emit('connect')
  }

  _onmessage (event) {
    if (this.destroyed) {
      return
    }

    const data = event.data

    if (data instanceof ArrayBuffer) {
      this.push(b4a.from(data))
      return
    }

    if (typeof Blob !== 'undefined' && data instanceof Blob) {
      data.arrayBuffer()
        .then(buf => {
          if (!this.destroyed) {
            this.push(b4a.from(buf))
          }
        })
        .catch(err => {
          this.destroy(err)
        })

      return
    }

    this.push(b4a.from(data))
  }

  _onerror (event) {
    let err

    if (event instanceof Error) {
      err = event
    } else {
      const msg = event.message || 'WebSocket error'

      err = new Error(msg)

      if (event.code) {
        err.code = event.code
      }
    }

    this._debug('error', err.message)

    const cb = this._opening

    this._opening = null

    if (cb) {
      cb(err)
    }

    this.destroy(err)
  }

  _onclose () {
    const opening = this._opening

    this._debug('close')

    this._closed = true

    this._opening = null

    if (opening) {
      opening(new Error('WebSocket closed before open'))
    }

    if (!this.readableEnded) {
      this.push(null)
    }

    this._continueWrite(new Error('Socket closed'))

    if (this.destroying || this.destroyed) {
      return
    }

    this.destroy()
  }

  _write (data, cb) {
    if (this._closed || this.ws.readyState !== WS_OPEN) {
      cb(new Error('Socket closed'))
      return
    }

    try {
      this.ws.send(data)
    } catch (err) {
      cb(err)
      return
    }

    if (this.ws.bufferedAmount >= this.maxBufferedAmount) {
      this._writeCallback = cb
      this._drain()
      return
    }

    cb(null)
  }

  // Poll because WebSocket doesn't expose a drain event.
  _drain () {
    if (this.destroyed || !this._writeCallback) {
      return
    }

    if (this.ws.readyState !== WS_OPEN) {
      this._continueWrite(new Error('Socket closed'))
      return
    }

    if (this.ws.bufferedAmount < this.maxBufferedAmount) {
      this._continueWrite(null)
      return
    }

    setTimeout(() => this._drain(), this.drainInterval)
  }

  _continueWrite (err) {
    const cb = this._writeCallback

    this._writeCallback = null

    if (cb) {
      cb(err)
    }
  }

  _final (cb) {
    const flush = () => {
      if (this.destroyed || this._closed) {
        cb(null)
        return
      }

      if (this.ws.readyState !== WS_OPEN) {
        cb(null)
        return
      }

      if (this.ws.bufferedAmount === 0) {
        try {
          this.ws.close()
        } catch {}

        cb(null)
        return
      }

      setTimeout(flush, 4)
    }

    flush()
  }

  _predestroy () {
    this._debug('predestroy')

    this._continueWrite(getStreamError(this) || new Error('Stream destroyed'))

    if (this.ws.readyState === WS_OPEN || this.ws.readyState === WS_CONNECTING) {
      try {
        this.ws.close()
      } catch {}
    }
  }

  _destroy (cb) {
    this._debug('destroy')

    this.ws.removeEventListener('open', this._onopen)
    this.ws.removeEventListener('message', this._onmessage)
    this.ws.removeEventListener('error', this._onerror)
    this.ws.removeEventListener('close', this._onclose)

    cb(null)
  }

  _debug (...args) {
    if (this.debug) {
      this.debug(...args)
    }
  }
}

function mapWritable (data) {
  return b4a.from(data)
}
