// ─────────────────────────────────────────────────────────────────────────────
// Camille Core — WhatsApp API Gateway
// Remplace WAHA Plus : multi-sessions, envoi audio/vidéo/doc sans limite,
// QR code accessible via HTTP + WebSocket, anti-ban intégré.
// ─────────────────────────────────────────────────────────────────────────────

'use strict';

const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const QRCode   = require('qrcode');
const axios    = require('axios');
const path     = require('path');
const fs       = require('fs');

// ── Config ────────────────────────────────────────────────────────────────────

const PORT          = process.env.PORT          || 3000;
const API_KEY       = process.env.API_KEY       || 'camille-core-secret';
const N8N_WEBHOOK   = process.env.N8N_WEBHOOK_URL || '';
const SESSIONS_DIR  = process.env.SESSIONS_DIR  || './sessions';

// ── Serveur HTTP + Socket.io ──────────────────────────────────────────────────

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.json({ limit: '100mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ── Middleware auth ───────────────────────────────────────────────────────────

const auth = (req, res, next) => {
  const key = req.headers['x-api-key'] || req.query.apiKey;
  if (key !== API_KEY) return res.status(401).json({ error: 'Unauthorized — X-Api-Key invalide' });
  next();
};

// ── Session Manager ───────────────────────────────────────────────────────────
//
//  Chaque "session" = 1 numéro WhatsApp = 1 instance Client whatsapp-web.js
//  sessions Map : name → { name, status, qrBase64, client }
//

const sessions = new Map();

function createSession(name) {
  if (sessions.has(name)) return sessions.get(name);

  const data = { name, status: 'INITIALIZING', qrBase64: null, client: null };
  sessions.set(name, data);

  const client = new Client({
    authStrategy: new LocalAuth({
      clientId:  name,
      dataPath:  SESSIONS_DIR,
    }),
    puppeteer: {
      headless: true,
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-gpu',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-zygote',
        '--single-process',
      ],
    },
  });

  data.client = client;

  // ── Événements WhatsApp ───────────────────────────────────────────────────

  client.on('qr', async (qr) => {
    data.status   = 'QR_READY';
    data.qrBase64 = await QRCode.toDataURL(qr);
    io.emit('session:update', { name, status: data.status, qr: data.qrBase64 });
    console.log(`[${name}] 📱 QR Code prêt — scannez depuis le dashboard`);
  });

  client.on('authenticated', () => {
    data.status = 'AUTHENTICATED';
    io.emit('session:update', { name, status: data.status });
    console.log(`[${name}] 🔐 Authentifié`);
  });

  client.on('ready', () => {
    data.status   = 'CONNECTED';
    data.qrBase64 = null;
    io.emit('session:update', { name, status: data.status });
    console.log(`[${name}] ✅ Connecté et prêt`);
  });

  client.on('auth_failure', (msg) => {
    data.status = 'AUTH_FAILURE';
    io.emit('session:update', { name, status: data.status });
    console.error(`[${name}] ❌ Échec auth: ${msg}`);
  });

  client.on('disconnected', (reason) => {
    data.status = 'DISCONNECTED';
    io.emit('session:update', { name, status: data.status });
    console.log(`[${name}] 🔌 Déconnecté: ${reason}`);
    // Tenter reconnexion après 10 s
    setTimeout(() => {
      console.log(`[${name}] 🔄 Tentative reconnexion...`);
      client.initialize().catch(e => console.error(`[${name}] Reconnexion failed:`, e.message));
    }, 10_000);
  });

  // ── Réception messages → forward n8n ─────────────────────────────────────

  client.on('message', async (msg) => {
    if (msg.fromMe) return;

    // Support webhook par session : N8N_WEBHOOK_MONSESSION ou N8N_WEBHOOK_URL global
    const webhookUrl = process.env[`N8N_WEBHOOK_${name.toUpperCase()}`] || N8N_WEBHOOK;
    if (!webhookUrl) return;

    try {
      await axios.post(webhookUrl, {
        event:   'message',
        session: name,
        payload: {
          id:         msg.id._serialized,
          from:       msg.from,
          fromMe:     false,
          body:       msg.body,
          type:       msg.type,
          timestamp:  msg.timestamp,
          notifyName: msg._data?.notifyName || '',
        },
      }, { timeout: 8000 });
    } catch (err) {
      console.error(`[${name}] Webhook error:`, err.message);
    }
  });

  // ── Init ──────────────────────────────────────────────────────────────────

  client.initialize().catch(err => {
    data.status = 'ERROR';
    console.error(`[${name}] Init error:`, err.message);
  });

  return data;
}

async function stopSession(name) {
  const s = sessions.get(name);
  if (!s) return false;
  try { await s.client.destroy(); } catch {}
  sessions.delete(name);
  io.emit('session:removed', { name });
  return true;
}

// Auto-démarrage des sessions persistées sur disque
function autoStartSessions() {
  if (!fs.existsSync(SESSIONS_DIR)) return;
  fs.readdirSync(SESSIONS_DIR, { withFileTypes: true })
    .filter(d => d.isDirectory() && d.name.startsWith('session-'))
    .forEach(d => {
      const name = d.name.replace(/^session-/, '');
      if (name) {
        console.log(`[Auto-start] Reprise session: ${name}`);
        createSession(name);
      }
    });
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const formatChatId = (id) => {
  if (!id) return null;
  if (id.includes('@')) return id;
  return `${id.replace(/[^0-9]/g, '')}@c.us`;
};

const randomDelay = (min, max) =>
  new Promise(r => setTimeout(r, Math.floor(Math.random() * (max - min + 1)) + min));

const getClient = (session) => {
  const s = sessions.get(session);
  if (!s)                      throw new Error(`Session "${session}" introuvable`);
  if (s.status !== 'CONNECTED') throw new Error(`Session "${session}" non connectée (${s.status})`);
  return s.client;
};

// ── Routes ────────────────────────────────────────────────────────────────────

// Dashboard (page HTML)
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// Health check (sans auth)
app.get('/health', (_req, res) => res.json({ ok: true, sessions: sessions.size }));

// ── Sessions ──────────────────────────────────────────────────────────────────

app.get('/api/sessions', auth, (_req, res) => {
  res.json({
    sessions: [...sessions.values()].map(s => ({
      name:   s.name,
      status: s.status,
      hasQr:  !!s.qrBase64,
    })),
  });
});

app.post('/api/sessions/start', auth, (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'name requis' });
  const s = createSession(name);
  res.json({ success: true, name: s.name, status: s.status });
});

app.delete('/api/sessions/:name/stop', auth, async (req, res) => {
  const ok = await stopSession(req.params.name);
  res.json({ success: ok });
});

app.get('/api/sessions/:name/status', auth, (req, res) => {
  const s = sessions.get(req.params.name);
  if (!s) return res.status(404).json({ error: 'Session introuvable' });
  res.json({ name: s.name, status: s.status });
});

app.get('/api/sessions/:name/qr', auth, (req, res) => {
  const s = sessions.get(req.params.name);
  if (!s)                return res.status(404).json({ error: 'Session introuvable' });
  if (s.status === 'CONNECTED') return res.json({ status: s.status, message: 'Déjà connecté' });
  if (!s.qrBase64)       return res.json({ status: s.status, message: 'QR pas encore disponible, patientez…' });
  res.json({ status: s.status, qrCodeBase64: s.qrBase64 });
});

// Alias court : GET /api/qr?session=xxx  (compatible intégrations simples)
app.get('/api/qr', auth, (req, res) => {
  const name = req.query.session || 'default';
  const s = sessions.get(name);
  if (!s)                       return res.status(404).json({ error: 'Session introuvable' });
  if (s.status === 'CONNECTED') return res.json({ status: s.status });
  if (!s.qrBase64)              return res.json({ status: s.status, message: 'QR pas encore disponible' });
  res.json({ status: s.status, qrCodeBase64: s.qrBase64 });
});

// ── Envoi de messages (compatibles WAHA) ──────────────────────────────────────

// POST /api/sendText   { chatId, text, session }
app.post('/api/sendText', auth, async (req, res) => {
  let { chatId, text, session = 'default' } = req.body;
  if (!chatId || !text) return res.status(400).json({ error: 'chatId et text requis' });

  try {
    const cl  = getClient(session);
    const id  = formatChatId(chatId);
    const chat = await cl.getChatById(id);

    await chat.sendStateTyping();
    await randomDelay(1200, 2500);
    await cl.sendMessage(id, text);
    await chat.clearState();

    res.json({ success: true });
  } catch (err) {
    console.error('[sendText]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/sendVoice  { chatId, session, file: { url } }
app.post('/api/sendVoice', auth, async (req, res) => {
  let { chatId, session = 'default', file } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId requis' });
  if (!file?.url) return res.json({ success: true, skipped: true, reason: 'Aucune URL audio configurée' });

  try {
    const cl   = getClient(session);
    const id   = formatChatId(chatId);
    const chat = await cl.getChatById(id);
    const media = await MessageMedia.fromUrl(file.url, { unsafeMime: true });
    // Forcer le rendu "note vocale" dans WhatsApp
    media.mimetype = 'audio/ogg; codecs=opus';

    await chat.sendStateRecording();
    await randomDelay(1500, 3000);
    await cl.sendMessage(id, media, { sendAudioAsVoice: true });
    await chat.clearState();

    res.json({ success: true });
  } catch (err) {
    console.error('[sendVoice]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/sendVideo  { chatId, session, file: { url }, caption }
app.post('/api/sendVideo', auth, async (req, res) => {
  let { chatId, session = 'default', file, caption = '' } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId requis' });
  if (!file?.url) return res.json({ success: true, skipped: true, reason: 'Aucune URL vidéo configurée' });

  try {
    const cl    = getClient(session);
    const id    = formatChatId(chatId);
    const media = await MessageMedia.fromUrl(file.url, { unsafeMime: true });

    await randomDelay(500, 1500);
    await cl.sendMessage(id, media, { caption });

    res.json({ success: true });
  } catch (err) {
    console.error('[sendVideo]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/sendFile   { chatId, session, file: { url, name }, caption }
app.post('/api/sendFile', auth, async (req, res) => {
  let { chatId, session = 'default', file, caption = '' } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId requis' });
  if (!file?.url) return res.json({ success: true, skipped: true, reason: 'Aucune URL fichier configurée' });

  try {
    const cl    = getClient(session);
    const id    = formatChatId(chatId);
    const media = await MessageMedia.fromUrl(file.url, { unsafeMime: true });
    if (file.name) media.filename = file.name;

    await randomDelay(500, 1500);
    await cl.sendMessage(id, media, { caption });

    res.json({ success: true });
  } catch (err) {
    console.error('[sendFile]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/startTyping  { chatId, session }
app.post('/api/startTyping', auth, async (req, res) => {
  let { chatId, session = 'default' } = req.body;
  try {
    const cl   = getClient(session);
    const chat = await cl.getChatById(formatChatId(chatId));
    await chat.sendStateTyping();
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/stopTyping   { chatId, session }
app.post('/api/stopTyping', auth, async (req, res) => {
  let { chatId, session = 'default' } = req.body;
  try {
    const cl   = getClient(session);
    const chat = await cl.getChatById(formatChatId(chatId));
    await chat.clearState();
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Socket.io ─────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  // Envoyer l'état actuel au nouveau client web
  socket.emit('init', [...sessions.values()].map(s => ({
    name:   s.name,
    status: s.status,
    qr:     s.qrBase64,
  })));
});

// ── Démarrage ─────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`\n🔥 Camille Core → http://localhost:${PORT}`);
  console.log(`🔑 API Key      → ${API_KEY}`);
  console.log(`📡 Webhook n8n  → ${N8N_WEBHOOK || '(non configuré)'}\n`);
  autoStartSessions();
});
