// create a basic node server

import http from 'http'
import { WebSocketServer } from 'ws'
import IORedis from 'ioredis'

import fs from 'fs/promises'
import path from 'path'

const HTTP_SERVER = http.createServer(async (req, res) => {
    try {
        const filePath = path.resolve('index.html')
        const content = await fs.readFile(filePath, 'utf-8')
        res.writeHead(200, { 'Content-Type': 'text/html' })
        res.end(content)
    } catch (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain' })
        res.end('Internal Server Error')
    }
})

const REDIS_CHANNEL = 'ws-messages'
const redisPublish = new IORedis({ maxRetriesPerRequest: 1 })
const redisSubscribe = new IORedis({ maxRetriesPerRequest: 1 })

let useRedisFallback = true

// Error handlers to avoid unhandled crash / excessive spam
redisPublish.on('error', (err) => {
    if (useRedisFallback) {
        console.warn('⚠️ Redis Publish client connection failed. Operating in local-only fallback mode.')
        useRedisFallback = false
    }
})

redisSubscribe.on('error', (err) => {
    // Suppress connection spam
})

redisPublish.on('connect', () => {
    console.log('Connected to Redis (Publish)')
    useRedisFallback = true
})

redisSubscribe.on('connect', () => {
    console.log('Connected to Redis (Subscribe)')
    redisSubscribe.subscribe(REDIS_CHANNEL)
})

// Subscribe once globally instead of per-connection to avoid duplicate listeners
redisSubscribe.on('message', (channel, message) => {
    if (channel === REDIS_CHANNEL) {
        wsServer.clients.forEach(client => {
            client.send(message.toString())
        })
    }
})

const wsServer = new WebSocketServer({ server: HTTP_SERVER })

wsServer.on('connection', (socket) => {
    console.log('A new client connected')

    socket.on('message', (data) => {
        const messageStr = data.toString()
        console.log('Received message:', messageStr)

        if (useRedisFallback) {
            console.log('Broadcasting message via Redis broker')
            redisPublish.publish(REDIS_CHANNEL, messageStr).catch(err => {
                // If publish fails mid-way, fall back to in-memory broadcast
                broadcastLocally(messageStr)
            })
        } else {
            console.log('Broadcasting message directly (in-memory)')
            broadcastLocally(messageStr)
        }
    })

    socket.on('close', () => {
        console.log('Connection is closed')
    })
})

function broadcastLocally(message) {
    wsServer.clients.forEach(client => {
        client.send(message)
    })
}

const PORT = process.env.PORT || 3000
HTTP_SERVER.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`)
})
