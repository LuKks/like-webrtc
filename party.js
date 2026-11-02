const fs = require('fs')
const http = require('http')
const https = require('https')
const minimist = require('minimist')
const { WebSocketServer } = require('ws')
const PartyServer = require('./lib/party.js')
const WebSocketDuplex = require('./duplex/websocket.js')

const argv = minimist(process.argv.slice(2))

const ssl = {
  cert: argv.cert ? fs.readFileSync(argv.cert) : null, // E.g. fullchain.pem
  key: argv.key ? fs.readFileSync(argv.key) : null // E.g. privkey.pem
}

const serverInsecure = http.createServer()
const serverSecure = ssl.cert && ssl.key ? https.createServer({ ...ssl }) : http.createServer()

const wsi = new WebSocketServer({ server: serverInsecure })
const wss = new WebSocketServer({ server: serverSecure })

const party = new PartyServer()

wsi.on('connection', function (ws, req) {
  ws.setMaxListeners(0)

  const rawStream = new WebSocketDuplex(ws)

  party.handle(rawStream)
})

wss.on('connection', function (ws, req) {
  ws.setMaxListeners(0)

  const rawStream = new WebSocketDuplex(ws)

  party.handle(rawStream)
})

serverInsecure.on('listening', function () {
  console.log('Party is ready without HTTPS', serverInsecure.address())
})

serverSecure.on('listening', function () {
  console.log('Party is ready with HTTPS', serverSecure.address())
})

serverInsecure.listen(41336, '0.0.0.0')
serverSecure.listen(41337, '0.0.0.0')
