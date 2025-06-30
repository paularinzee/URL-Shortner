require('dotenv').config();
const express = require('express');
const redis = require('redis');
const shortid = require('shortid');

const app = express();
app.use(express.json());

// Create Redis clients
const redisClients = [
    redis.createClient({ host: process.env.REDIS_HOST1, port: process.env.REDIS_PORT1 }),
    redis.createClient({ host: process.env.REDIS_HOST2, port: process.env.REDIS_PORT2 }),
    redis.createClient({ host: process.env.REDIS_HOST3, port: process.env.REDIS_PORT3 }),
];

// Connect Redis clients
redisClients.forEach((client, index) => {
    client.on('error', (err) => console.error(`Redis ${index + 1} Error:`, err));
    client.connect().catch((err) => console.error(`Redis ${index + 1} Connect Error:`, err));
});

// Hash function to pick a client
function getRedisClient(key) {
    const hash = key.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
    return redisClients[hash % redisClients.length];
}

// POST /shorten - shorten a URL
app.post('/shorten', async (req, res) => {
    const { url, ttl } = req.body;
    if (!url) return res.status(400).send('URL is required');

    const shortId = shortid.generate();
    const client = getRedisClient(shortId);

    try {
        await client.set(shortId, url, {
            EX: ttl || 3600,
        });
        res.json({ shortUrl: `http://localhost:${process.env.PORT}/${shortId}` });
    } catch (err) {
        console.error('Redis SET error:', err);
        res.status(500).send('Server error');
    }
});

// GET /:shortId - retrieve original URL
app.get('/:shortId', async (req, res) => {
    const { shortId } = req.params;
    const client = getRedisClient(shortId);

    try {
        const url = await client.get(shortId);
        if (!url) return res.status(404).send('URL not found');

        res.redirect(url);
    } catch (err) {
        console.error('Redis GET error:', err);
        res.status(500).send('Server error');
    }
});

app.listen(process.env.PORT, () => {
    console.log(`Server running on port ${process.env.PORT}`);
});
