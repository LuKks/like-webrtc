// sodium-javascript patch for the error "Noise handshake failed"

const sodium = require('sodium-universal')

const ED25519_BYTES = 32
const ED25519_SCALAR_MASK = (1n << 255n) - 1n

if (!sodium.crypto_scalarmult_ed25519_BYTES) {
  sodium.crypto_scalarmult_ed25519_BYTES = ED25519_BYTES
}

if (!sodium.crypto_scalarmult_ed25519_SCALARBYTES) {
  sodium.crypto_scalarmult_ed25519_SCALARBYTES = ED25519_BYTES
}

if (!sodium.crypto_scalarmult_ed25519_noclamp) {
  const { ed25519 } = require('@noble/curves/ed25519')

  sodium.crypto_scalarmult_ed25519_noclamp = function (output, scalar, publicKey) {
    const q = asBytes(output, 'q', 'crypto_scalarmult_ed25519_BYTES')
    const n = asBytes(scalar, 'n', 'crypto_scalarmult_ed25519_SCALARBYTES')
    const p = asBytes(publicKey, 'p', 'crypto_scalarmult_ed25519_BYTES')

    let point = null

    try {
      point = ed25519.Point.fromBytes(p)
    } catch (err) {
      throw new Error('status: -1', { cause: err })
    }

    if (point.isSmallOrder() || !point.isTorsionFree()) {
      throw new Error('status: -1')
    }

    const reduced = ed25519.Point.Fn.create(bytesToNumberLE(n) & ED25519_SCALAR_MASK)

    if (reduced === 0n) {
      q.set(ed25519.Point.ZERO.toBytes())

      throw new Error('status: -1')
    }

    q.set(point.multiply(reduced).toBytes())
  }
}

module.exports = sodium

function asBytes (view, name, size) {
  if (!ArrayBuffer.isView(view)) throw errorAssertion(`${name} must be a typed array`)
  if (view.byteLength !== ED25519_BYTES) throw errorAssertion(`${name} must be '${size}' bytes`)

  return new Uint8Array(view.buffer, view.byteOffset, view.byteLength)
}

function bytesToNumberLE (bytes) {
  let n = 0n

  for (let i = bytes.length - 1; i >= 0; i--) {
    n = (n << 8n) | BigInt(bytes[i])
  }

  return n
}

function errorAssertion (message) {
  const err = new Error(message)

  err.name = 'AssertionError'
  err.code = 'ERR_ASSERTION'

  return err
}
