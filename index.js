require('dotenv').config();
const express = require('express');
const redis = require('redis');
const { nanoid } = require('nanoid');
const crypto = require('crypto');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');

// -----------------------------
// Configuration & validation
// -----------------------------
const requiredEnvVars = ['REDIS_HOST1', 'REDIS_PORT1', 'REDIS_HOST2', 'REDIS_PORT2', 'REDIS_HOST3', 'REDIS_PORT3', 'PORT'];
for (const envVar of requiredEnvVars) {
  if (!process.env[envVar]) {
    console.error(`Missing required environment variable: ${envVar}`);
    process.exit(1);
  }
}

const app = express();
app.use(express.json());
app.use(helmet());

// Basic rate limiter to help prevent abuse
const limiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 60, // max 60 requests per IP per minute
  standardHeaders: true,
  legacyHeaders: false,
});
app.use(limiter);

// -----------------------------
// Redis v4 clients (correct API)
// -----------------------------
function makeRedisClient(host, port) {
  return redis.createClient({
    url: `redis://${host}:${port}`,
    socket: {
      // reconnect strategy: retries -> delay in ms
      reconnectStrategy: (retries) => Math.min(retries * 100, 3000),
    },
  });
}

const redisClients = [
  makeRedisClient(process.env.REDIS_HOST1, process.env.REDIS_PORT1),
  makeRedisClient(process.env.REDIS_HOST2, process.env.REDIS_PORT2),
  makeRedisClient(process.env.REDIS_HOST3, process.env.REDIS_PORT3),
];

// Connect all clients before accepting traffic
(async () => {
  try {
    await Promise.all(redisClients.map((c, i) => c.connect()));
    console.log('✅ All Redis clients connected');
  } catch (err) {
    console.error('❌ Error connecting Redis clients:', err);
    process.exit(1);
  }
})();

redisClients.forEach((client, i) => {
  client.on('error', (err) => console.error(`Redis ${i + 1} error:`, err.message));
  client.on('connect', () => console.log(`Redis ${i + 1} connected`));
  client.on('ready', () => console.log(`Redis ${i + 1} ready`));
});

// -----------------------------
// Hashing (MD5 -> integer) to pick client
// -----------------------------
function getRedisClient(key) {
  const hash = crypto.createHash('md5').update(key).digest('hex');
  const idx = parseInt(hash.substring(0, 8), 16) % redisClients.length;
  return redisClients[idx];
}

// -----------------------------
// Helpers
// -----------------------------
function isValidUrl(string) {
  try {
    const url = new URL(string);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch (_) {
    return false;
  }
}

function isSelfUrl(urlString) {
  try {
    const u = new URL(urlString);
    const port = process.env.PORT || '3000';
    const host = `localhost:${port}`;
    // check localhost with the configured port or the host header domain
    return (u.hostname === 'localhost' && (u.port === '' || u.port === port)) || u.host === host;
  } catch (_) {
    return false;
  }
}

// -----------------------------
// Endpoints
// -----------------------------
app.get('/health', async (req, res) => {
  const status = { status: 'ok', redis: [] };
  for (let i = 0; i < redisClients.length; i++) {
    try {
      await redisClients[i].ping();
      status.redis.push({ client: i + 1, status: 'connected' });
    } catch (err) {
      status.redis.push({ client: i + 1, status: 'disconnected', error: err.message });
      status.status = 'degraded';
    }
  }
  res.status(status.status === 'ok' ? 200 : 503).json(status);
});

// POST /shorten
app.post('/shorten', async (req, res) => {
  const { url, ttl, customAlias } = req.body;

  if (!url) return res.status(400).json({ error: 'URL is required', code: 'MISSING_URL' });
  if (!isValidUrl(url)) return res.status(400).json({ error: 'Invalid URL', code: 'INVALID_URL' });
  if (isSelfUrl(url)) return res.status(400).json({ error: 'Shortening self-referential URLs is not allowed', code: 'SELF_URL' });

  const parsedTtl = ttl ? parseInt(ttl, 10) : 3600;
  if (isNaN(parsedTtl) || parsedTtl <= 0) return res.status(400).json({ error: 'TTL must be positive integer', code: 'INVALID_TTL' });

  let shortId = customAlias;
  if (customAlias) {
    if (!/^[a-zA-Z0-9_-]+$/.test(customAlias) || customAlias.length > 50) {
      return res.status(400).json({ error: 'Invalid custom alias', code: 'INVALID_ALIAS' });
    }
  } else {
    shortId = nanoid(8);
  }

  const client = getRedisClient(shortId);
  const key = `short:${shortId}`;
  const analyticsKey = `analytics:${shortId}`;

  try {
    // If custom alias, ensure it doesn't exist
    if (customAlias) {
      const exists = await client.exists(key);
      if (exists) return res.status(409).json({ error: 'Alias already exists', code: 'ALIAS_EXISTS' });
    }

    // Store main record as a JSON string (single source of truth)
    const payload = JSON.stringify({ originalUrl: url, createdAt: new Date().toISOString(), ttl: parsedTtl });
    await client.set(key, payload, { EX: parsedTtl });

    // Store analytics separately as a hash (so we can increment atomically)
    await client.hSet(analyticsKey, { clicks: '0' });
    await client.expire(analyticsKey, parsedTtl);

    res.json({
      shortId,
      shortUrl: `http://localhost:${process.env.PORT}/${shortId}`,
      originalUrl: url,
      ttl: parsedTtl,
      expiresAt: new Date(Date.now() + parsedTtl * 1000).toISOString(),
    });
  } catch (err) {
    console.error('Error creating short URL:', err);
    res.status(500).json({ error: 'Server error', code: 'REDIS_ERROR' });
  }
});

// GET /:shortId (redirect)
app.get('/:shortId', async (req, res) => {
  const { shortId } = req.params;
  if (!shortId || shortId.length > 50) return res.status(400).json({ error: 'Invalid short id', code: 'INVALID_SHORT_ID' });

  const client = getRedisClient(shortId);
  const key = `short:${shortId}`;
  const analyticsKey = `analytics:${shortId}`;

  try {
    const data = await client.get(key);
    if (!data) return res.status(404).json({ error: 'URL not found', code: 'URL_NOT_FOUND' });

    const parsed = JSON.parse(data);

    // increment clicks (fire & forget safe) but ensure analytics exists
    client.hIncrBy(analyticsKey, 'clicks', 1).catch((e) => console.warn('hIncrBy failed:', e.message));

    // Standard redirect
    return res.redirect(parsed.originalUrl);
  } catch (err) {
    console.error('Error retrieving short URL:', err);
    res.status(500).json({ error: 'Server error', code: 'REDIS_ERROR' });
  }
});

// GET analytics
app.get('/analytics/:shortId', async (req, res) => {
  const { shortId } = req.params;
  if (!shortId || shortId.length > 50) return res.status(400).json({ error: 'Invalid short id', code: 'INVALID_SHORT_ID' });

  const client = getRedisClient(shortId);
  const key = `short:${shortId}`;
  const analyticsKey = `analytics:${shortId}`;

  try {
    const data = await client.get(key);
    if (!data) return res.status(404).json({ error: 'Short URL not found', code: 'URL_NOT_FOUND' });

    const meta = JSON.parse(data);
    const analytics = await client.hGetAll(analyticsKey);

    res.json({
      shortId,
      originalUrl: meta.originalUrl,
      createdAt: meta.createdAt,
      ttl: parseInt(meta.ttl, 10) || 0,
      clicks: parseInt(analytics.clicks || '0', 10),
      isActive: true,
    });
  } catch (err) {
    console.error('Error fetching analytics:', err);
    res.status(500).json({ error: 'Server error', code: 'REDIS_ERROR' });
  }
});

// DELETE
app.delete('/:shortId', async (req, res) => {
  const { shortId } = req.params;
  if (!shortId || shortId.length > 50) return res.status(400).json({ error: 'Invalid short id', code: 'INVALID_SHORT_ID' });

  const client = getRedisClient(shortId);
  const key = `short:${shortId}`;
  const analyticsKey = `analytics:${shortId}`;

  try {
    const del = await client.del(key);
    await client.del(analyticsKey);

    if (del === 0) return res.status(404).json({ error: 'Short URL not found', code: 'URL_NOT_FOUND' });

    res.json({ message: 'Deleted', shortId });
  } catch (err) {
    console.error('Error deleting short URL:', err);
    res.status(500).json({ error: 'Server error', code: 'REDIS_ERROR' });
  }
});

// Graceful shutdown
const gracefulShutdown = async () => {
  console.log('Shutting down...');
  for (let i = 0; i < redisClients.length; i++) {
    try {
      await redisClients[i].quit();
      console.log(`Redis ${i + 1} disconnected`);
    } catch (e) {
      console.error(`Error disconnecting Redis ${i + 1}:`, e.message);
    }
  }
  process.exit(0);
};
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// Global error handler
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
});

// Start server
const server = app.listen(process.env.PORT, () => {
  console.log(`URL Shortener (refactor) running on port ${process.env.PORT}`);
  console.log(`Health: http://localhost:${process.env.PORT}/health`);
});

server.on('close', () => console.log('Server closed'));
