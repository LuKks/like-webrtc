const RTC = require('./index.js')

main()

async function main () {
  // Server
  const rtc = new RTC()

  const server = rtc.createServer(peer => {
    const stream = new RTC.WebStream(peer.createDataChannel('wire', { negotiated: true, id: 0 }))

    process.stdin.pipe(stream).pipe(process.stdout)
  })

  await server.listen()

  // Client
  const rtc2 = new RTC()

  const peer = rtc2.connect(server.publicKey)

  const stream = new RTC.WebStream(peer.createDataChannel('wire', { negotiated: true, id: 0 }))

  process.stdin.pipe(stream).pipe(process.stdout)
}
