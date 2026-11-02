// sodium-javascript patch for the error "Noise handshake failed"

const sodium = require('sodium-universal')

const ED25519_BYTES = 32

if (!sodium.crypto_scalarmult_ed25519_BYTES) {
  sodium.crypto_scalarmult_ed25519_BYTES = ED25519_BYTES
}

if (typeof sodium.crypto_scalarmult_ed25519_noclamp !== 'function') {
  const { ed25519 } = require('@noble/curves/ed25519')

  sodium.crypto_scalarmult_ed25519_noclamp = function (output, scalar, publicKey) {
    if (
      output.byteLength !== ED25519_BYTES ||
      scalar.byteLength !== ED25519_BYTES ||
      publicKey.byteLength !== ED25519_BYTES
    ) {
      throw new Error('Invalid Ed25519 scalar multiplication input')
    }

    const point = ed25519.Point.fromBytes(publicKey)

    if (!point.isTorsionFree() || point.isSmallOrder()) {
      throw new Error('Invalid Ed25519 point')
    }

    const n = ed25519.Point.Fn.create(bytesToNumberLE(scalar))

    if (n === 0n) {
      throw new Error('Invalid Ed25519 scalar')
    }

    output.set(point.multiply(n).toBytes())

    return 0
  }
}

module.exports = sodium

function bytesToNumberLE (bytes) {
  let n = 0n

  for (let i = bytes.length - 1; i >= 0; i--) {
    n = (n << 8n) | BigInt(bytes[i])
  }

  return n
}
