const { Duplex, getStreamError } = require('streamx')
const b4a = require('b4a')

module.exports = class TCPDuplex extends Duplex {
  constructor (socket, opts = {}) {
    super({
      mapWritable,
      eagerOpen: true
    })

    this.socket = socket
    this.debug = !!opts.debug
    this._opening = null
    this._writeCallback = null

    this._remoteEnded = false
    this._localEnded = false

    this._onConnect = this._onConnect.bind(this)
    this._onData = this._onData.bind(this)
    this._onDrain = this._onDrain.bind(this)
    this._onEnd = this._onEnd.bind(this)
    this._onError = this._onError.bind(this)
    this._onClose = this._onClose.bind(this)

    socket.on('connect', this._onConnect)
    socket.on('data', this._onData)
    socket.on('drain', this._onDrain)
    socket.on('end', this._onEnd)
    socket.on('error', this._onError)
    socket.on('close', this._onClose)

    // Force streamx to call _open immediately
    this.resume().pause()

    if (socket.readyState === 'open') {
      queueMicrotask(this._onConnect)
    }
  }

  get remoteAddress () {
    return this.socket.remoteAddress
  }

  get remotePort () {
    return this.socket.remotePort
  }

  setTimeout (ms, cb) {
    this.socket.setTimeout(ms, cb)
    return this
  }

  _open (cb) {
    if (this.debug) {
      console.log('TCPDuplex _open', this.socket.readyState)
    }

    if (this.socket.readyState === 'open') {
      return cb(null)
    }

    if (this.socket.readyState === 'closed') {
      return cb(new Error('Socket is closed'))
    }

    this._opening = cb
  }

  _predestroy () {
    if (this.debug) {
      console.log('TCPDuplex _predestroy')
    }

    const err = getStreamError(this) || new Error('Stream destroyed')

    if (this._writeCallback) {
      const cb = this._writeCallback
      this._writeCallback = null
      cb(err)
    }

    try {
      this.socket.destroy()
    } catch {}
  }

  _destroy (cb) {
    if (this.debug) {
      console.log('TCPDuplex _destroy')
    }

    this.socket.off('connect', this._onConnect)
    this.socket.off('data', this._onData)
    this.socket.off('drain', this._onDrain)
    this.socket.off('end', this._onEnd)
    this.socket.off('error', this._onError)
    this.socket.off('close', this._onClose)

    cb(null)
  }

  _read (cb) {
    try {
      this.socket.resume()
      cb(null)
    } catch (err) {
      cb(err)
    }
  }

  _onData (data) {
    if (this.destroying) {
      return
    }

    const buf = b4a.isBuffer(data) ? data : b4a.from(data)

    if (this.push(buf) === false) {
      this.socket.pause()
    }
  }

  _onEnd () {
    if (this.debug) {
      console.log('TCPDuplex remote end')
    }

    if (this._remoteEnded) {
      return
    }

    this._remoteEnded = true

    this.push(null)
  }

  _write (data, cb) {
    if (this.destroying) {
      return cb(getStreamError(this) || new Error('Stream destroyed'))
    }

    // Copy the view for react-native-tcp-socket's JSI path
    const buf = b4a.from(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength))

    try {
      const flushed = this.socket.write(buf)

      if (flushed === true) {
        return cb(null)
      }

      this._writeCallback = cb
    } catch (err) {
      cb(err)
    }
  }

  _onDrain () {
    const cb = this._writeCallback
    this._writeCallback = null

    if (cb) {
      cb(null)
    }
  }

  _final (cb) {
    if (this.debug) {
      console.log('TCPDuplex _final')
    }

    if (this._localEnded) {
      return cb(null)
    }

    this._localEnded = true

    if (this._writeCallback) {
      const prev = this._writeCallback

      this._writeCallback = err => {
        prev(err)

        try {
          this.socket.end()
          cb(null)
        } catch (err) {
          cb(err)
        }
      }

      return
    }

    try {
      this.socket.end()
      cb(null)
    } catch (err) {
      cb(err)
    }
  }

  _onConnect () {
    if (this.debug) {
      console.log('TCPDuplex connected')
    }

    const cb = this._opening
    this._opening = null

    if (cb) {
      cb(null)
    }

    this.emit('connect')
  }

  _onError (err) {
    if (this.debug) {
      console.log('TCPDuplex socket error', err)
    }

    if (this._opening) {
      const cb = this._opening
      this._opening = null
      cb(err)
    }

    this.destroy(err)
  }

  _onClose () {
    if (this.debug) {
      console.log('TCPDuplex socket close')
    }

    if (this.destroyed || this.destroying) {
      return
    }

    this.destroy()
  }
}

function mapWritable (data) {
  return b4a.from(data)
}
