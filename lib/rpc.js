const EventEmitter = require('events')
const promiseWithResolvers = require('promise-resolvers')
const compact = require('compact-encoding')
const { getStreamError } = require('streamx')

// TODO: Should create a ExpressRPC for servers to avoid re-creating responder handlers etc

class ErrorRPC extends Error {
  constructor (code, message) {
    super(message)

    this.code = code
  }

  get name () {
    return 'ErrorRPC'
  }
}

module.exports = class RPC extends EventEmitter {
  constructor (stream, opts = {}) {
    super()

    this._opening = promiseWithResolvers()
    this._closing = promiseWithResolvers()

    this.opened = false
    this.closed = false

    this._opening.promise.catch(noop)

    this._id = 1
    this._requests = new Map()
    this._responds = new Map()

    this.stream = stream

    this.stream.on('open', this._onStreamOpen.bind(this))
    this.stream.on('data', this._onStreamData.bind(this))
    this.stream.on('error', this._onStreamError.bind(this))
    this.stream.on('close', this._onStreamClose.bind(this))

    this.on('error', noop)

    this.ready().catch(noop)
  }

  static Error = ErrorRPC

  get publicKey () {
    return this.stream.publicKey
  }

  async ready () {
    await this._opening.promise
  }

  async close () {
    this.destroy()

    await this._closing.promise
  }

  destroy () {
    this.closed = true

    this.stream.destroy()
  }

  _onStreamOpen () {
    this.opened = true

    this._opening.resolve()
  }

  async _onStreamData (data) {
    try {
      const message = compact.decode(compact.any, data)

      if (message.type === 'request') {
        await this._onRequest(message)
      } else if (message.type === 'response') {
        this._onResponse(message)
      }
    } catch (err) {
      this.stream.destroy(err)
    }
  }

  _onStreamError (err) {
    this._opening.reject(err)

    this.emit('error', err)
  }

  _onStreamClose () {
    this.closed = true

    const error = getStreamError(this.stream) || new Error('RPC closed' + (this.tag ? ' (' + this.tag + ')' : ''))

    for (const [, req] of this._requests) {
      req.reject(error)

      if (req.timeout) {
        clearTimeout(req.timeout)
      }
    }

    this._requests.clear()

    this._opening.resolve()
    this._closing.resolve()

    this.emit('close')
  }

  respond (method, callback) {
    this._responds.set(method, { callback })
  }

  unrespond (method) {
    this._responds.delete(method)
  }

  async request (method, body) {
    // if (!this.opened) await this._opening.promise
    if (this.closed) throw new Error('RPC is closing')

    const id = this._nextId()
    const req = new Request(id)

    this._requests.set(id, req)

    req.timeout = setTimeout(() => {
      const req = this._requests.get(id)

      if (req) {
        this._requests.delete(id)

        req.reject(new Error('Timed out'))
      }
    }, 60000)

    this.stream.write(compact.encode(compact.any, { type: 'request', id, method, body }))

    return req.promise
  }

  event (method, body) {
    this.stream.write(compact.encode(compact.any, { type: 'request', id: 0, method, body }))
  }

  async _onRequest (req) {
    const responder = this._responds.get(req.method)

    if (!responder) {
      if (req.id) {
        this.stream.write(compact.encode(compact.any, { type: 'response', id: req.id, error: { message: 'Method not found', code: 'METHOD_NOT_FOUND' }, result: null }))
      }

      return
    }

    try {
      const result = await responder.callback(req)

      if (req.id) {
        this.stream.write(compact.encode(compact.any, { type: 'response', id: req.id, error: null, result }))
      }
    } catch (err) {
      // TODO: Double check all 'error' events, why did it not produce the error log on emit?
      // console.error(err)

      // TODO: RPC.Error
      const validation = err.name === 'ValidationError'
      const critical = !(err.name === 'ErrorHTTP' || err.name === 'ErrorRPC' || validation)

      if (critical) {
        this.emit('error', err)
      }

      if (req.id) {
        const cause = {
          message: critical ? 'Internal error' : err.message
        }

        const code = critical ? 'INTERNAL' : (validation ? 'VALIDATION' : err.code)

        if (code) cause.code = code

        // TODO: Compat
        // if (err.status) cause.status = err.status
        // if (validation) cause.validation = { path: err.path, type: err.type }

        const error = {
          message: 'Request failed',
          code: 'RPC_REQUEST_FAILED',
          cause
        }

        this.stream.write(compact.encode(compact.any, { type: 'response', id: req.id, error, result: null }))
      }
    }
  }

  _onResponse (res) {
    const req = this._requests.get(res.id)

    if (!req) {
      return
    }

    this._requests.delete(res.id)

    if (req.timeout) {
      clearTimeout(req.timeout)
    }

    if (res.error) {
      const error = new Error(res.error.message, { cause: res.error.cause })

      if (res.error.code) error.code = res.error.code

      // TODO: Compat
      // if (res.error.status) error.status = res.error.status
      // if (res.error.validation) error.validation = res.error.validation

      req.reject(error)

      return
    }

    req.resolve(res.result)
  }

  _nextId () {
    if (this._id === 0xffffffff) {
      this._id = 1
    }

    return this._id++
  }
}

class Request {
  constructor (id) {
    this.id = id
    this.timeout = null

    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
  }
}

function noop () {}
