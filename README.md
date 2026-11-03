# like-webrtc

Peer-to-peer WebRTC connections with built-in signaling.

Works across browsers, Node.js, and React Native.

```
npm i like-webrtc
```

Warning: this is experimental.

Live demo: https://web.leet.ar (P2P Discord clone built in 48 hours)

Need support with WebRTC? Open a GitHub issue :)

## Usage

#### Direct peer-to-peer connections

A peer can create a server listening on a public key as address.

Another peer can use that address to connect to the server.

```js
import RTC from 'like-webrtc'

const rtc = new RTC()

const server = rtc.createServer(peer => {
  const channel = peer.createDataChannel('wire', { negotiated: true, id: 0 })

  channel.addEventListener('open', () => channel.send('Hello from the server'))
  channel.addEventListener('message', e => console.log('Server received:', e.data))
})

await server.listen()
```

```js
const rtc2 = new RTC()

const peer = rtc2.connect(server.publicKey)

const channel = peer.createDataChannel('wire', { negotiated: true, id: 0 })

channel.addEventListener('open', () => channel.send('Hello from the client'))
channel.addEventListener('message', e => console.log('Client received:', e.data))
```

A familiar API like Node.js `http`, but peer-to-peer and running directly in your browser.

#### Group peers

<details>
<summary>Click to see example</summary>

Peers can create servers with random key pairs.

You can announce or lookup any server under the same shared topic.

```js
import RTC from 'like-webrtc'

const rtc = new RTC()

const server = rtc.createServer(peer => {
  console.log('New peer connection')
  // ...
})

await server.listen()
await server.announce('random-group')
```

```js
const rtc2 = new RTC()

const results = await rtc2.lookup('random-group')

const peer = rtc2.connect(results[0].publicKey)
// ...
```
</details>

#### Swarm peers

<details>
<summary>Click to see example</summary>

Each peer runs the same code to join the swarm.

Other peers will find you.

```js
import RTC from 'like-webrtc'

const rtc = new RTC()

const server = rtc.createServer(peer => {
  console.log('New swarm connection')
  // ...
})

await server.listen()
await server.join('random-swarm')
```
</details>

## SIGNALING

Single signaling server:

- `wss://party.leet.ar:41337`

It's a WebSocket server with a RPC protocol.

- E2E noise encryption: meaning HTTPS/WSS is redundant.
- Announce / Unannounce: act as a server e.g. listen/close.
- Lookup: act as a client by finding peers.
- Relay: bidirectional data for WebRTC SDP/ICE/ACK.

## ICE

3 STUN servers:

- `stun:turn.leet.ar:3478`
- `stun:stun.l.google.com:19302`
- `stun:stun.cloudflare.com:3478`

1 TURN server with multiple transports:

- `turn:turn.leet.ar:3478?transport=udp`
- `turn:turn.leet.ar:3478?transport=tcp`

## API

### `rtc = new RTC([options])`

Creates a WebRTC instance.

Options:

```js
{
  signaling, // Defaults to 'wss://party.leet.ar:41337'.
  keyPair, // Defaults to a key pair generated from seed or random bytes.
  seed, // Seed used to generate the key pair.
  iceServers, // Defaults to the package's STUN and TURN servers.
  iceTransportPolicy, // Defaults to 'all'.
  trickle, // Defaults to true.
  RTCPeerConnection, // Optional WebRTC implementation override.
  RTCIceCandidate // Optional ICE candidate implementation override.
}
```

#### `rtc.publicKey`

Returns the RTC instance's public key as a buffer.

#### `rtc.address`

Returns the RTC instance's public key as a Base58 string.

### `peer = rtc.connect(publicKey[, options])`

Creates a P2P connection and returns a `RTCPeerConnection`.

Add a data channel, track, or transceiver to begin WebRTC negotiation.

Options:

```js
{
  keyPair // Defaults to rtc.keyPair.
}
```

### `server = rtc.createServer([options][, handler])`

Creates a P2P server that accepts WebRTC connections.

The handler receives each incoming peer connection.

Options:

```js
{
  keyPair, // Defaults to rtc.keyPair.
  firewall // Function or array of allowed public keys. Allows all peers by default.
}
```

#### `server.on('connection', peer)`

Emitted on each incoming `RTCPeerConnection`, same as the handler.

#### `server.publicKey`

Returns the server's public key as a buffer.

#### `server.address`

Returns the server's public key as a Base58 string.

#### `await server.listen([keyPair])`

Starts listening for connections under the server's public key so peers can connect.

#### `await server.announce(topic)`

Announces the listening server under `topic` and refreshes the announcement while the server listens.

The server removes the announcement when it closes.

#### `await server.unannounce(topic)`

Removes the server's announcement for `topic`.

The server keeps listening for direct connections.

If the server has joined `topic`, it will keep looking up peers.

The `topic` stays unannounced until you call `server.announce(topic)` or leave and join again.

#### `await server.join(topic)`

Finds peers that announce `topic` and connects to them.

If the server is listening, it also announces itself under that topic.

The server emits a `connection` event for each discovered connection.

Closed connections are retried after 1 second, 5 seconds, 15 seconds, then every 10 minutes.

#### `await server.leave(topic)`

Stops finding peers under `topic`.

If only `server.join(topic)` announced the `topic`, it also removes that announcement.

An announcement made from `server.announce(topic)` stays active.

Existing peer connections stay open.

Cancels retries unless the peer was also discovered under another joined topic.

#### `await server.close()`

Closes the server's peer connections, removes its topic announcements, and closes its signaling connection.

### `await rtc.announce(topic[, options])`

Signs and sends the announcement with `keyPair`.

Options:

```js
{
  keyPair // Defaults to rtc.keyPair.
}
```

The announcement expires unless you refresh or remove it.

### `await rtc.unannounce(topic[, options])`

Signs and sends a request to remove the announcement with `keyPair`.

Options:

```js
{
  keyPair // Defaults to rtc.keyPair.
}
```

### `peers = await rtc.lookup(topic)`

Returns peers announced under `topic`:

```js
[
  { publicKey }
]
```

An announcement can remain even after a peer abruptly disconnects, unless closed properly.

The peer can stay discoverable while reconnecting after a brief signaling drop or page refresh.

A lookup during this gap can return a peer that is still offline.

#### `await rtc.close()`

Closes the RTC instance, its servers, its peers, and its signaling.

If you called `rtc.announce()` directly, call `rtc.unannounce()` to remove those announcements.

If you do not remove these manual announcements, they remain available until expiration.

#### `keyPair = RTC.keyPair([seed])`

Creates a key pair. Pass `seed` to create the same key pair each time.

#### `bytes = RTC.randomBytes(size)`

Returns random bytes.

#### `stream = RTC.WebStream(dataChannelOrWebSocket)`

Wraps a WebRTC data channel or a WebSocket in a duplex stream.

Useful for interacting with common Node.js streams.

## Ideas

WhatsApp relays and stores messages for 30d if the recipient is offline.

Telegram uses WebRTC with double encryption to avoid trusting SRTP/DTLS.

Discord forwards WebRTC media with SFUs so peers don't connect together.

`like-webrtc` can do the same for a perfect P2P experience including the browser.

- RPC relay messages must be E2E encrypted between peers to avoid learning from connections or metadata
- Create a tiny SFU topology from scratch using WebRTC e.g. 3 people speaking and 3000 listening
- Signaling could be a HTTPS server with EventSource/SSE instead of a WebSocket server
- Make deploying TURN servers easy so people can use their own and allow app creators to pay for bandwidth
- Payments is outside of the scope of this module but it's important for peers to be able to exchange value
- More cryptographic safety around verifying announcement signatures locally
- WebRTC-HTTP ingestion protocol (WHIP instead of RTMP?)
- WebRTC-HTTP egress protocol (WHEP instead of HLS/CDN?)
- Reusable module for WebRTC audio, video, screen, etc so that creating frontend apps is easy
- HTTPS server that helps connect to a WebRTC signaling peer to avoid TCP/HTTPS/WebSocket traffic

## Notes

This was a one-week kind of thing. I built it while using two React Native instances and it worked but I was having issues with ICE restarts, negotiations, opening channels on demand, etc. At that point, I had everything written by hand but due those issues I ended up trying AI to enhance the code (especially the Peer class). Later on, I ran the code in Node.js and the browser, and I realized a bit too late that the original issues were only present in React Native due old unstable WebRTC builds. So most of the AI code i.e. changes in lib/peer.js are probably mostly unnecessary and could be simplified, but I didn't want to revert the changes due several added features. Summary: Peer needs a rewrite to get back to simplicity.

## License

MIT. Copyright (c) Lucas Barrena
