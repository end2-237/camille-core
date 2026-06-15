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
const MEDIA_DIR     = path.join(__dirname, 'public', 'media');
const VERSION       = '1.1.0';
const START_TIME    = Date.now();

// ── Stabilité : watchdog & reconnexion ────────────────────────────────────────
const WATCHDOG_INTERVAL_MS = Number(process.env.WATCHDOG_INTERVAL_MS) || 60_000;  // sonde getState() toutes les 60s
const RECONNECT_BASE_MS    = Number(process.env.RECONNECT_BASE_MS)    || 5_000;   // backoff initial
const RECONNECT_MAX_MS     = 5 * 60_000;                                          // backoff plafonné à 5 min
const MAX_RECONNECT_TRIES  = Number(process.env.MAX_RECONNECT_TRIES)  || 10;      // au-delà → garde le QR/erreur, stoppe la boucle
// Version WhatsApp Web épinglée : évite que le store casse lors d'une MAJ WA côté serveur.
// OPT-IN : actif seulement si WWEB_VERSION est défini (sinon comportement natif de la lib).
// Pinner une version inexistante casserait la connexion → on ne force RIEN par défaut.
const WWEB_VERSION = process.env.WWEB_VERSION || '';

// Créer le dossier media au démarrage
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

// ── Webhook config (persisted in webhooks.json) ───────────────────────────────
const WEBHOOKS_FILE = path.join(__dirname, 'webhooks.json');

function loadWebhookConfig() {
  try { if (fs.existsSync(WEBHOOKS_FILE)) return JSON.parse(fs.readFileSync(WEBHOOKS_FILE, 'utf8')); } catch {}
  return { global: N8N_WEBHOOK, sessions: {} };
}

function saveWebhookConfig(cfg) {
  try { fs.writeFileSync(WEBHOOKS_FILE, JSON.stringify(cfg, null, 2)); } catch (e) { console.error('webhooks.json write error:', e.message); }
}

let webhookConfig = loadWebhookConfig();
if (!webhookConfig.sessions) webhookConfig.sessions = {};
if (!webhookConfig.global && N8N_WEBHOOK) webhookConfig.global = N8N_WEBHOOK;

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

function cleanLockFiles(name) {
  // Cherche et supprime tous les SingletonLock/Cookie/Socket récursivement
  // Ces fichiers sont laissés par l'ancien container Docker et bloquent Chromium
  const sessionDir = path.join(SESSIONS_DIR, `session-${name}`);
  if (!fs.existsSync(sessionDir)) return;

  const LOCK_FILES = ['SingletonLock', 'SingletonCookie', 'SingletonSocket'];

  function removeLocks(dir) {
    let entries;
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (LOCK_FILES.includes(entry.name)) {
        try {
          fs.rmSync(fullPath, { force: true });
          console.log(`[${name}] 🧹 Lock supprimé: ${fullPath}`);
        } catch (e) {
          console.warn(`[${name}] ⚠️  Impossible de supprimer ${fullPath}: ${e.message}`);
        }
      } else if (entry.isDirectory()) {
        removeLocks(fullPath);
      }
    }
  }

  removeLocks(sessionDir);
}

function createSession(name) {
  if (sessions.has(name)) return sessions.get(name);

  const data = {
    name, status: 'INITIALIZING', qrBase64: null, client: null, phone: null,
    // ── État interne de stabilité ──
    reconnecting:    false,  // garde anti-concurrence pour la reconnexion/recréation
    stopped:         false,  // session arrêtée volontairement → ne pas reconnecter
    watchdogTimer:   null,   // setInterval du watchdog
    reconnectTimer:  null,   // setTimeout de la reconnexion programmée
    // ── Métriques de monitoring ──
    metrics: {
      createdAt:        Date.now(),
      statusChangedAt:  Date.now(),
      lastMessageAt:    null,   // dernier message REÇU d'un contact
      messageCount:     0,      // total messages reçus
      lastWebhookOkAt:  null,   // dernier forward n8n réussi
      reconnectCount:   0,      // nombre de tentatives de reconnexion
      lastDisconnect:   null,   // { reason, at }
      webhookErrors:    0,
      mediaErrors:      0,
      lastError:        null,   // { msg, at }
      emptyBodyCount:   0,      // messages reçus avec body vide (symptôme zombie)
      zombieKills:      0,      // nombre de fois où le watchdog a recréé un client zombie
      lastWatchdogAt:   null,   // dernière sonde getState() réussie
    },
  };
  sessions.set(name, data);

  spawnClient(data);
  startWatchdog(data);
  return data;
}

// ── spawnClient : construit un client NEUF et l'attache à `data` ──────────────
// Utilisé au démarrage ET à chaque reconnexion/recréation. Ne touche jamais aux
// métriques cumulées (elles vivent dans data.metrics), seulement à data.client.
function spawnClient(data) {
  const name = data.name;
  cleanLockFiles(name);   // purge les SingletonLock laissés par un Chromium mort

  // Épinglage de version OPT-IN : seulement si WWEB_VERSION est défini.
  const versionOpts = WWEB_VERSION ? {
    webVersion: WWEB_VERSION,
    webVersionCache: {
      type: 'remote',
      remotePath: `https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/${WWEB_VERSION}.html`,
    },
  } : {};

  const client = new Client({
    authStrategy: new LocalAuth({ clientId: name, dataPath: SESSIONS_DIR }),
    ...versionOpts,
    restartOnAuthFail: true,   // relance proprement plutôt que de rester bloqué sur auth_failure
    puppeteer: {
      headless: true,
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      // NB : --single-process RETIRÉ — c'était la cause n°1 des crashs renderer silencieux.
      // --no-zygote retiré aussi (n'a de sens qu'avec --single-process).
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-gpu',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
      ],
    },
  });

  data.client = client;

  const setStatus = (s) => { data.status = s; data.metrics.statusChangedAt = Date.now(); };

  client.on('qr', async (qr) => {
    setStatus('QR_READY');
    data.qrBase64 = await QRCode.toDataURL(qr);
    io.emit('session:update', { name, status: data.status, qr: data.qrBase64 });
    console.log(`[${name}] 📱 QR Code prêt — scannez depuis le dashboard`);
  });

  client.on('authenticated', () => {
    setStatus('AUTHENTICATED');
    io.emit('session:update', { name, status: data.status });
    console.log(`[${name}] 🔐 Authentifié`);
  });

  client.on('ready', async () => {
    setStatus('CONNECTED');
    data.qrBase64 = null;
    data.reconnecting = false;        // succès → libère la garde
    data.metrics.reconnectCount = 0;  // remet le backoff à zéro
    try {
      const info = await client.getInfo();
      data.phone = info?.wid?.user || null;
    } catch {}
    io.emit('session:update', { name, status: data.status });
    console.log(`[${name}] ✅ Connecté et prêt${data.phone ? ' — ' + data.phone : ''}`);
  });

  client.on('auth_failure', (msg) => {
    setStatus('AUTH_FAILURE');
    data.metrics.lastError = { msg: `auth_failure: ${msg}`, at: Date.now() };
    io.emit('session:update', { name, status: data.status });
    console.error(`[${name}] ❌ Échec auth: ${msg}`);
  });

  client.on('disconnected', (reason) => {
    setStatus('DISCONNECTED');
    data.metrics.lastDisconnect = { reason: String(reason), at: Date.now() };
    io.emit('session:update', { name, status: data.status });
    console.log(`[${name}] 🔌 Déconnecté: ${reason}`);
    // Reconnexion correcte : destroy() du client mort PUIS client neuf (jamais re-init du zombie)
    scheduleReconnect(name, `disconnected: ${reason}`);
  });

  // ── Réception messages → forward n8n ─────────────────────────────────────
  client.on('message', async (msg) => {
    if (msg.fromMe) return;

    data.metrics.lastMessageAt = Date.now();
    data.metrics.messageCount += 1;
    if ((!msg.body || msg.body.trim() === '') && msg.type === 'chat') {
      data.metrics.emptyBodyCount += 1;
      console.warn(`[${name}] ⚠️  Message body vide (type=chat) — symptôme zombie possible (#${data.metrics.emptyBodyCount})`);
    }

    const webhookUrl = webhookConfig.sessions[name]
      || process.env[`N8N_WEBHOOK_${name.toUpperCase()}`]
      || webhookConfig.global
      || N8N_WEBHOOK;
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
      data.metrics.lastWebhookOkAt = Date.now();
    } catch (err) {
      data.metrics.webhookErrors += 1;
      data.metrics.lastError = { msg: `webhook: ${err.message}`, at: Date.now() };
      console.error(`[${name}] Webhook error:`, err.message);
    }
  });

  client.initialize().catch(err => {
    data.status = 'ERROR';
    data.metrics.lastError = { msg: `init: ${err.message}`, at: Date.now() };
    console.error(`[${name}] Init error:`, err.message);
    // Plan B : si l'init échoue (souvent fetch version distante KO), on reprogramme une reconnexion
    scheduleReconnect(name, `init error: ${err.message}`);
  });

  return client;
}

// ── scheduleReconnect : détruit proprement le client mort puis en recrée un ───
// Garde anti-concurrence (data.reconnecting) + backoff exponentiel plafonné.
function scheduleReconnect(name, reason) {
  const data = sessions.get(name);
  if (!data || data.stopped) return;          // session supprimée/arrêtée → on n'insiste pas
  if (data.reconnecting) return;              // déjà une reconnexion en cours
  data.reconnecting = true;

  if (data.metrics.reconnectCount >= MAX_RECONNECT_TRIES) {
    data.metrics.lastError = { msg: `abandon reconnexion après ${MAX_RECONNECT_TRIES} essais (${reason})`, at: Date.now() };
    console.error(`[${name}] 🛑 Reconnexion abandonnée après ${MAX_RECONNECT_TRIES} essais. Reset manuel requis.`);
    data.reconnecting = false;
    return;
  }

  const tries = data.metrics.reconnectCount;
  const delay = Math.min(RECONNECT_BASE_MS * Math.pow(2, tries), RECONNECT_MAX_MS);
  console.log(`[${name}] 🔄 Reconnexion programmée dans ${Math.round(delay/1000)}s (essai #${tries + 1}) — ${reason}`);

  clearTimeout(data.reconnectTimer);
  data.reconnectTimer = setTimeout(async () => {
    data.metrics.reconnectCount += 1;
    // 1) Détruire proprement l'ancien client zombie (best-effort, jamais bloquant)
    try {
      if (data.client) await data.client.destroy();
    } catch (e) {
      console.warn(`[${name}] destroy() pendant reconnexion: ${e.message}`);
    }
    // 2) Recréer un client NEUF
    try {
      console.log(`[${name}] 🔁 Recréation d'un client neuf...`);
      spawnClient(data);
      data.reconnecting = false;  // sera reconfirmé sur 'ready'
    } catch (e) {
      data.reconnecting = false;
      data.metrics.lastError = { msg: `recréation: ${e.message}`, at: Date.now() };
      console.error(`[${name}] Recréation échouée:`, e.message);
      scheduleReconnect(name, `retry après échec recréation`);
    }
  }, delay);
}

// ── Watchdog : sonde getState() régulièrement pour détecter les zombies ───────
// Le cas vicieux : le renderer Chromium meurt SANS émettre 'disconnected'.
// getState() lève alors une exception ou renvoie ≠ CONNECTED → on force la recréation.
function startWatchdog(data) {
  const name = data.name;
  clearInterval(data.watchdogTimer);
  data.watchdogTimer = setInterval(async () => {
    if (data.stopped || data.reconnecting) return;
    // On ne sonde que les sessions censées être connectées
    if (data.status !== 'CONNECTED') return;
    try {
      const state = await Promise.race([
        data.client.getState(),
        new Promise((_, rej) => setTimeout(() => rej(new Error('getState timeout')), 15_000)),
      ]);
      data.metrics.lastWatchdogAt = Date.now();
      if (state !== 'CONNECTED') {
        data.metrics.zombieKills += 1;
        console.warn(`[${name}] 🧟 Watchdog : état=${state} ≠ CONNECTED → recréation`);
        scheduleReconnect(name, `watchdog state=${state}`);
      }
    } catch (e) {
      // getState() qui throw = renderer mort = zombie silencieux : le cas qu'on cible
      data.metrics.zombieKills += 1;
      data.metrics.lastError = { msg: `watchdog: ${e.message}`, at: Date.now() };
      console.warn(`[${name}] 🧟 Watchdog : getState() a échoué (${e.message}) → zombie présumé, recréation`);
      scheduleReconnect(name, `watchdog exception: ${e.message}`);
    }
  }, WATCHDOG_INTERVAL_MS);
}

async function stopSession(name) {
  const s = sessions.get(name);
  if (!s) return false;
  s.stopped = true;                       // bloque toute reconnexion en vol
  clearInterval(s.watchdogTimer);
  clearTimeout(s.reconnectTimer);
  try { if (s.client) await s.client.destroy(); } catch {}
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

// DELETE /api/sessions/:name/reset — arrête la session ET supprime les fichiers d'auth
// Utilisé par le dashboard Camille lors d'une déconnexion volontaire (changer de numéro)
app.delete('/api/sessions/:name/reset', auth, async (req, res) => {
  const name = req.params.name;
  await stopSession(name);

  // Supprimer les fichiers d'auth LocalAuth
  const sessionDir = path.join(SESSIONS_DIR, `session-${name}`);
  try {
    if (fs.existsSync(sessionDir)) {
      fs.rmSync(sessionDir, { recursive: true, force: true });
      console.log(`[${name}] 🗑️  Auth supprimée: ${sessionDir}`);
    }
  } catch (e) {
    console.warn(`[${name}] ⚠️  Impossible de supprimer auth: ${e.message}`);
  }

  res.json({ success: true });
});

app.get('/api/sessions/:name/status', auth, (req, res) => {
  const s = sessions.get(req.params.name);
  if (!s) return res.status(404).json({ error: 'Session introuvable' });
  res.json({ name: s.name, status: s.status, phone: s.phone || null });
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

// ── Helper : résout une URL en MessageMedia (local si possible) ───────────────
// Détecte automatiquement les fichiers locaux via le pattern /media/filename
// sans dépendance à CORE_PUBLIC_URL
function resolveMedia(url) {
  const mediaMatch = url.match(/\/media\/([^/?#]+)/);
  if (mediaMatch) {
    const filename = mediaMatch[1];
    const filePath = path.join(MEDIA_DIR, filename);
    if (fs.existsSync(filePath)) {
      console.log('[media] lecture locale:', filePath);
      return MessageMedia.fromFilePath(filePath);
    }
  }
  console.log('[media] téléchargement URL:', url);
  return MessageMedia.fromUrl(url, { unsafeMime: true });
}

// ── Helper : normalise @lid → @c.us pour les envois media ────────────────────
// whatsapp-web.js supporte @lid pour le texte mais pas pour les médias
function normalizeMediaId(id) {
  if (!id) return id;
  return id.replace(/@lid$/, '@c.us');
}

// POST /api/sendVoice  { chatId, session, file: { url } }
app.post('/api/sendVoice', auth, async (req, res) => {
  let { chatId, session = 'default', file } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId requis' });
  if (!file?.url) return res.json({ success: true, skipped: true, reason: 'Aucune URL audio configurée' });

  try {
    const cl    = getClient(session);
    const rawId = formatChatId(chatId);
    console.log('[sendVoice] chatId:', rawId, 'url:', file.url);

    // Pour @lid : récupérer le vrai numéro de téléphone via le contact
    let sendId = rawId;
    if (rawId.endsWith('@lid')) {
      try {
        const contact = await cl.getContactById(rawId);
        // Log complet pour diagnostic
        console.log('[sendVoice] contact keys:', Object.keys(contact));
        console.log('[sendVoice] contact.id:', JSON.stringify(contact.id));
        console.log('[sendVoice] contact.number:', contact.number);
        console.log('[sendVoice] contact.pushname:', contact.pushname);

        // Essayer toutes les sources possibles du vrai numéro
        const cus = contact.id && contact.id._serialized;
        if (cus && cus.endsWith('@c.us')) {
          sendId = cus;
          console.log('[sendVoice] sendId via contact.id._serialized:', sendId);
        } else if (contact.number && contact.number.length > 5) {
          sendId = `${contact.number}@c.us`;
          console.log('[sendVoice] sendId via contact.number:', sendId);
        } else {
          // Dernière tentative : récupérer depuis le chat
          const chat = await cl.getChatById(rawId);
          const chatContact = await chat.getContact();
          console.log('[sendVoice] chatContact.id:', JSON.stringify(chatContact.id));
          console.log('[sendVoice] chatContact.number:', chatContact.number);
          if (chatContact.number && chatContact.number.length > 5) {
            sendId = `${chatContact.number}@c.us`;
            console.log('[sendVoice] sendId via chatContact.number:', sendId);
          }
        }
      } catch (lidErr) {
        const lidMsg = (lidErr && typeof lidErr === 'object') ? lidErr.message : String(lidErr);
        console.warn('[sendVoice] Résolution LID échouée:', lidMsg);
      }
    }

    const chat = await cl.getChatById(rawId);
    await chat.sendStateRecording();
    await randomDelay(1500, 3000);

    const mediaPtt = await resolveMedia(file.url);
    mediaPtt.mimetype = 'audio/ogg; codecs=opus';
    const dataLen = mediaPtt.data ? mediaPtt.data.length : 0;
    console.log('[sendVoice] envoi PTT vers:', sendId, 'data length:', dataLen);
    if (dataLen < 500) {
      throw new Error(`Fichier audio invalide ou introuvable (${dataLen} bytes base64) — vérifiez que le fichier est bien uploadé`);
    }

    try {
      await cl.sendMessage(sendId, mediaPtt, { sendAudioAsVoice: true });
      console.log('[sendVoice] PTT envoyé ✓');
    } catch (pttErr) {
      const pttMsg = (pttErr && typeof pttErr === 'object') ? pttErr.message : String(pttErr);
      console.warn('[sendVoice] PTT échoué:', pttMsg, '→ audio normal');
      const media = await resolveMedia(file.url);
      try {
        await cl.sendMessage(sendId, media);
        console.log('[sendVoice] audio normal envoyé ✓');
      } catch (audioErr) {
        const audioMsg = (audioErr && typeof audioErr === 'object') ? audioErr.message : String(audioErr);
        console.error('[sendVoice] audio normal échoué:', audioMsg);
        throw new Error(audioMsg);
      }
    }

    await chat.clearState();
    res.json({ success: true });
  } catch (err) {
    const msg = (err && typeof err === 'object') ? err.message : String(err);
    const sd = sessions.get(session);
    if (sd?.metrics) { sd.metrics.mediaErrors += 1; sd.metrics.lastError = { msg: `sendVoice: ${msg}`, at: Date.now() }; }
    console.error('[sendVoice] ERREUR FINALE:', msg);
    res.status(500).json({ success: false, error: msg });
  }
});

// POST /api/sendVideo  { chatId, session, file: { url }, caption }
app.post('/api/sendVideo', auth, async (req, res) => {
  let { chatId, session = 'default', file, caption = '' } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId requis' });
  if (!file?.url) return res.json({ success: true, skipped: true, reason: 'Aucune URL vidéo configurée' });

  try {
    const cl    = getClient(session);
    const rawId = formatChatId(chatId);
    console.log('[sendVideo] rawId:', rawId, 'url:', file.url);

    // Résolution @lid → @c.us (même logique que sendVoice)
    let sendId = rawId;
    if (rawId.endsWith('@lid')) {
      try {
        const contact = await cl.getContactById(rawId);
        const cus = contact.id && contact.id._serialized;
        if (cus && cus.endsWith('@c.us')) {
          sendId = cus;
        } else if (contact.number && contact.number.length > 5) {
          sendId = `${contact.number}@c.us`;
        } else {
          const chat = await cl.getChatById(rawId);
          const chatContact = await chat.getContact();
          if (chatContact.number && chatContact.number.length > 5) {
            sendId = `${chatContact.number}@c.us`;
          }
        }
        console.log('[sendVideo] sendId résolu:', sendId);
      } catch (lidErr) {
        console.warn('[sendVideo] Résolution LID échouée:', lidErr.message);
      }
    }

    const media = await resolveMedia(file.url);
    await randomDelay(500, 1500);
    await cl.sendMessage(sendId, media, { caption });
    console.log('[sendVideo] envoyé ✓');

    res.json({ success: true });
  } catch (err) {
    const sd = sessions.get(session);
    if (sd?.metrics) { sd.metrics.mediaErrors += 1; sd.metrics.lastError = { msg: `sendVideo: ${err.message}`, at: Date.now() }; }
    console.error('[sendVideo] ERREUR FINALE:', err.message);
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

// ── Server info ───────────────────────────────────────────────────────────────

app.get('/api/server/info', auth, (_req, res) => {
  res.json({
    version:  VERSION,
    uptime:   Math.floor((Date.now() - START_TIME) / 1000),
    sessions: sessions.size,
    connected: [...sessions.values()].filter(s => s.status === 'CONNECTED').length,
  });
});

// ── Monitoring détaillé ─────────────────────────────────────────────────────────
// GET /api/health/detailed — état santé par session + ressources process
// Utilisé par la section "Monitoring" du dashboard.

const ZOMBIE_SILENCE_MS = 30 * 60 * 1000; // 30 min sans message = suspect si trafic habituel

function sessionHealth(s) {
  const m = s.metrics || {};
  const now = Date.now();
  const issues = [];
  let health = 'ok'; // ok | warn | critical

  if (s.status !== 'CONNECTED') {
    health = s.status === 'DISCONNECTED' || s.status === 'AUTH_FAILURE' || s.status === 'ERROR' ? 'critical' : 'warn';
    issues.push(`Statut: ${s.status}`);
  }

  // Zombie suspect : connecté mais bodies vides récents
  if (s.status === 'CONNECTED' && m.emptyBodyCount > 0) {
    health = 'critical';
    issues.push(`${m.emptyBodyCount} message(s) à body vide — session zombie probable`);
  }

  // Reconnexions répétées
  if (m.reconnectCount >= 3) {
    if (health === 'ok') health = 'warn';
    issues.push(`${m.reconnectCount} reconnexions`);
  }

  // Erreurs webhook
  if (m.webhookErrors > 0) {
    if (health === 'ok') health = 'warn';
    issues.push(`${m.webhookErrors} erreur(s) webhook n8n`);
  }

  // Zombies récupérés par le watchdog
  if (m.zombieKills > 0) {
    if (health === 'ok') health = 'warn';
    issues.push(`${m.zombieKills} zombie(s) récupéré(s) par le watchdog`);
  }

  // Silence prolongé (info seulement)
  const silenceMs = m.lastMessageAt ? now - m.lastMessageAt : null;
  if (s.status === 'CONNECTED' && silenceMs !== null && silenceMs > ZOMBIE_SILENCE_MS && m.messageCount > 0) {
    if (health === 'ok') health = 'warn';
    issues.push(`Aucun message depuis ${Math.round(silenceMs / 60000)} min`);
  }

  return {
    name:             s.name,
    status:           s.status,
    phone:            s.phone || null,
    health,
    issues,
    messageCount:     m.messageCount || 0,
    emptyBodyCount:   m.emptyBodyCount || 0,
    reconnectCount:   m.reconnectCount || 0,
    webhookErrors:    m.webhookErrors || 0,
    mediaErrors:      m.mediaErrors || 0,
    zombieKills:      m.zombieKills || 0,
    lastWatchdogAt:   m.lastWatchdogAt || null,
    lastMessageAt:    m.lastMessageAt || null,
    lastWebhookOkAt:  m.lastWebhookOkAt || null,
    statusChangedAt:  m.statusChangedAt || null,
    createdAt:        m.createdAt || null,
    lastDisconnect:   m.lastDisconnect || null,
    lastError:        m.lastError || null,
    silenceMs,
  };
}

app.get('/api/health/detailed', auth, (_req, res) => {
  const mem = process.memoryUsage();
  const list = [...sessions.values()].map(sessionHealth);
  res.json({
    now:      Date.now(),
    uptime:   Math.floor((Date.now() - START_TIME) / 1000),
    version:  VERSION,
    process: {
      rssMB:        Math.round(mem.rss / 1048576),
      heapUsedMB:   Math.round(mem.heapUsed / 1048576),
      heapTotalMB:  Math.round(mem.heapTotal / 1048576),
      externalMB:   Math.round((mem.external || 0) / 1048576),
    },
    summary: {
      total:     list.length,
      connected: list.filter(s => s.status === 'CONNECTED').length,
      critical:  list.filter(s => s.health === 'critical').length,
      warn:      list.filter(s => s.health === 'warn').length,
    },
    sessions: list,
  });
});

// ── Webhook config ────────────────────────────────────────────────────────────

app.get('/api/config/webhooks', auth, (_req, res) => {
  res.json({ global: webhookConfig.global || '', sessions: webhookConfig.sessions });
});

app.post('/api/config/webhooks', auth, (req, res) => {
  const { session, url } = req.body;
  if (!session) return res.status(400).json({ error: 'session requis' });
  if (session === '__global__') {
    webhookConfig.global = url || '';
  } else {
    if (url) webhookConfig.sessions[session] = url;
    else delete webhookConfig.sessions[session];
  }
  saveWebhookConfig(webhookConfig);
  res.json({ success: true });
});

// ── Media Storage ─────────────────────────────────────────────────────────────
// POST /api/media/upload  { name, data (base64), mimeType }
// DELETE /api/media/:filename

app.post('/api/media/upload', auth, (req, res) => {
  const { name, data, mimeType } = req.body;
  if (!name || !data) return res.status(400).json({ error: 'name et data (base64) requis' });

  // Assainir le nom de fichier
  const safe = name.replace(/[^a-zA-Z0-9._-]/g, '_').replace(/\.{2,}/g, '_');
  if (!safe) return res.status(400).json({ error: 'nom invalide' });

  try {
    const filePath = path.join(MEDIA_DIR, safe);
    fs.writeFileSync(filePath, Buffer.from(data, 'base64'));
    const baseUrl = (process.env.CORE_PUBLIC_URL || `http://localhost:${PORT}`).replace(/\/$/, '');
    const url = `${baseUrl}/media/${safe}`;
    console.log(`[media] upload → ${safe} (${mimeType || '?'})`);
    res.json({ url, filename: safe });
  } catch (e) {
    console.error('[media] upload error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/media/:filename', auth, (req, res) => {
  const safe = req.params.filename.replace(/[^a-zA-Z0-9._-]/g, '_').replace(/\.{2,}/g, '_');
  const filePath = path.join(MEDIA_DIR, safe);
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    console.log(`[media] delete → ${safe}`);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
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
