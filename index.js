// ─────────────────────────────────────────────────────────────────────────────
// Camille Core — WhatsApp API Gateway (v2 — Baileys)
// Multi-sessions, envoi audio/vidéo/doc sans limite, QR + pairing code,
// reconnexion auto, watchdog, analytics. SANS Chrome/Puppeteer → ultra léger.
// API HTTP & dashboard 100% compatibles avec la v1 (n8n inchangé).
// ─────────────────────────────────────────────────────────────────────────────

'use strict';

const makeWASocket = require('@whiskeysockets/baileys').default;
const {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  DisconnectReason,
} = require('@whiskeysockets/baileys');

const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const QRCode   = require('qrcode');
const axios    = require('axios');
const path     = require('path');
const fs        = require('fs');
const pino     = require('pino');

// ── Config ────────────────────────────────────────────────────────────────────

const PORT          = process.env.PORT          || 3000;
const API_KEY       = process.env.API_KEY       || 'camille-core-secret';
const N8N_WEBHOOK   = process.env.N8N_WEBHOOK_URL || '';
const SESSIONS_DIR  = process.env.SESSIONS_DIR  || './sessions';

// Logger Baileys → fichier baileys.log (niveau 'warn' par défaut : capture les
// stream:error et vraies raisons de déconnexion, sans noyer le CPU). Mettre
// BAILEYS_LOG_LEVEL=silent pour couper, ou =debug pour tout voir.
try { fs.mkdirSync(SESSIONS_DIR, { recursive: true }); } catch {}
const logger = pino(
  { level: process.env.BAILEYS_LOG_LEVEL || 'warn' },
  pino.destination(path.join(SESSIONS_DIR, 'baileys.log'))
);

// Debug applicatif → fichier (docker logs trop lent sur ce serveur)
const debugLog = (msg) => {
  const line = `[${new Date().toISOString()}] ${msg}\n`;
  fs.appendFile(path.join(SESSIONS_DIR, 'debug.log'), line, () => {});
};
const MEDIA_DIR     = path.join(__dirname, 'public', 'media');
const VERSION       = '2.0.0';
const START_TIME    = Date.now();

// ── Stabilité : watchdog & reconnexion ────────────────────────────────────────
const WATCHDOG_INTERVAL_MS = Number(process.env.WATCHDOG_INTERVAL_MS) || 60_000;  // surveillance toutes les 60s
const RECONNECT_BASE_MS    = Number(process.env.RECONNECT_BASE_MS)    || 5_000;   // backoff initial
const RECONNECT_MAX_MS     = 5 * 60_000;                                          // backoff plafonné à 5 min
const MAX_RECONNECT_TRIES  = Number(process.env.MAX_RECONNECT_TRIES)  || 10;      // au-delà → stoppe la boucle
const INIT_TIMEOUT_MS      = Number(process.env.INIT_TIMEOUT_MS)      || 180_000; // bloqué en init > 3 min → recréation forcée
// Plafond de sessions simultanées (anti-surcharge serveur). Baileys est léger,
// mais on garde une limite stricte par sécurité (CPU/RAM du VPS).
const MAX_SESSIONS         = Number(process.env.MAX_SESSIONS)         || 2;

// Créer le dossier media au démarrage
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

// ── Analytics : journal des messages entrants (persistant) ────────────────────
const ANALYTICS_FILE       = path.join(SESSIONS_DIR, 'analytics.jsonl');
const ANALYTICS_RETENTION_DAYS = Number(process.env.ANALYTICS_RETENTION_DAYS) || 90;
let analyticsEvents = [];   // [{ t: ms, s: session, f: from }]

function loadAnalytics() {
  try {
    if (!fs.existsSync(ANALYTICS_FILE)) return;
    const cutoff = Date.now() - ANALYTICS_RETENTION_DAYS * 86400000;
    const lines = fs.readFileSync(ANALYTICS_FILE, 'utf8').split('\n');
    for (const line of lines) {
      if (!line.trim()) continue;
      try { const e = JSON.parse(line); if (e.t >= cutoff) analyticsEvents.push(e); } catch {}
    }
    console.log(`[analytics] ${analyticsEvents.length} événements chargés (rétention ${ANALYTICS_RETENTION_DAYS}j)`);
  } catch (e) { console.warn('[analytics] load error:', e.message); }
}

function recordAnalytics(session, from) {
  const e = { t: Date.now(), s: session, f: from };
  analyticsEvents.push(e);
  fs.appendFile(ANALYTICS_FILE, JSON.stringify(e) + '\n', err => {
    if (err) console.warn('[analytics] append error:', err.message);
  });
}

function compactAnalytics() {
  try {
    const cutoff = Date.now() - ANALYTICS_RETENTION_DAYS * 86400000;
    analyticsEvents = analyticsEvents.filter(e => e.t >= cutoff);
    const tmp = ANALYTICS_FILE + '.tmp';
    fs.writeFileSync(tmp, analyticsEvents.map(e => JSON.stringify(e)).join('\n') + (analyticsEvents.length ? '\n' : ''));
    fs.renameSync(tmp, ANALYTICS_FILE);
    console.log(`[analytics] compacté → ${analyticsEvents.length} événements`);
  } catch (e) { console.warn('[analytics] compact error:', e.message); }
}

// S'assurer que SESSIONS_DIR existe (pour analytics + auth)
if (!fs.existsSync(SESSIONS_DIR)) fs.mkdirSync(SESSIONS_DIR, { recursive: true });
loadAnalytics();
setInterval(compactAnalytics, 24 * 3600 * 1000);

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

// ── Helpers JID (compatibilité n8n : on expose le format @c.us comme la v1) ───
// Baileys utilise @s.whatsapp.net (user), @g.us (groupe), @lid (linked id).
// La v1 (whatsapp-web.js) utilisait @c.us. Pour ne RIEN changer côté n8n,
// on convertit dans les deux sens.

function toJid(chatId) {
  if (!chatId) return null;
  let s = String(chatId).trim();
  if (s.includes('@')) {
    if (s.endsWith('@c.us')) return s.replace(/@c\.us$/, '@s.whatsapp.net');
    return s; // @s.whatsapp.net, @g.us, @lid : conservés
  }
  const num = s.replace(/[^0-9]/g, '');
  return `${num}@s.whatsapp.net`;
}

// Présente un JID Baileys au format hérité @c.us (ce que n8n attend)
function toLegacyId(jid) {
  if (!jid) return jid;
  if (jid.endsWith('@s.whatsapp.net')) return jid.replace(/@s\.whatsapp\.net$/, '@c.us');
  return jid; // @g.us, @lid : laissés tels quels
}

// Extrait le texte d'un message Baileys
function extractBody(m) {
  const msg = m.message || {};
  return msg.conversation
    || msg.extendedTextMessage?.text
    || msg.imageMessage?.caption
    || msg.videoMessage?.caption
    || msg.buttonsResponseMessage?.selectedButtonId
    || msg.listResponseMessage?.singleSelectReply?.selectedRowId
    || msg.templateButtonReplyMessage?.selectedId
    || '';
}

function msgType(m) {
  const msg = m.message || {};
  if (msg.conversation || msg.extendedTextMessage) return 'chat';
  if (msg.imageMessage)    return 'image';
  if (msg.videoMessage)    return 'video';
  if (msg.audioMessage)    return msg.audioMessage.ptt ? 'ptt' : 'audio';
  if (msg.documentMessage) return 'document';
  if (msg.stickerMessage)  return 'sticker';
  if (msg.locationMessage) return 'location';
  if (msg.contactMessage)  return 'vcard';
  return 'unknown';
}

// ── Session Manager ───────────────────────────────────────────────────────────
//  Chaque "session" = 1 numéro WhatsApp = 1 socket Baileys
//  sessions Map : name → { name, status, client(sock), ... }

const sessions = new Map();

function createSession(name) {
  if (sessions.has(name)) return sessions.get(name);

  const data = {
    name, status: 'INITIALIZING', qrBase64: null, client: null, phone: null,
    phoneNumber:  null,   // numéro pour le pairing code (sans +)
    pairingCode:  null,   // code 8 chars généré par requestPairingCode()
    saveCreds:    null,   // fonction de persistance des creds Baileys
    // ── État interne de stabilité ──
    reconnecting:    false,
    stopped:         false,
    watchdogTimer:   null,
    reconnectTimer:  null,
    // ── Métriques de monitoring (identiques v1 pour le dashboard) ──
    metrics: {
      createdAt:        Date.now(),
      statusChangedAt:  Date.now(),
      lastMessageAt:    null,
      messageCount:     0,
      lastWebhookOkAt:  null,
      reconnectCount:   0,
      lastDisconnect:   null,
      webhookErrors:    0,
      mediaErrors:      0,
      lastError:        null,
      emptyBodyCount:   0,
      zombieKills:      0,   // ici = recréations forcées par le watchdog (init bloqué)
      lastWatchdogAt:   null,
    },
  };
  sessions.set(name, data);

  spawnClient(data).catch(err => {
    data.status = 'ERROR';
    data.metrics.lastError = { msg: `spawn: ${err.message}`, at: Date.now() };
    console.error(`[${name}] spawn error:`, err.message);
    scheduleReconnect(name, `spawn error: ${err.message}`);
  });
  startWatchdog(data);
  return data;
}

// ── spawnClient : construit un socket Baileys NEUF et l'attache à `data` ──────
async function spawnClient(data) {
  const name = data.name;
  const authDir = path.join(SESSIONS_DIR, `session-${name}`);

  const { state, saveCreds } = await useMultiFileAuthState(authDir);
  data.saveCreds = saveCreds;

  let version;
  try {
    ({ version } = await fetchLatestBaileysVersion());
  } catch (e) {
    console.warn(`[${name}] fetchLatestBaileysVersion KO (${e.message}) — version par défaut`);
  }

  const sock = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys:  makeCacheableSignalKeyStore(state.keys, logger),
    },
    logger,
    printQRInTerminal: false,
    browser: ['Camille Core', 'Chrome', '2.0'],
    syncFullHistory: false,          // pas d'historique → léger en RAM
    markOnlineOnConnect: false,      // n'apparaît pas "en ligne" en permanence
    generateHighQualityLinkPreview: false,
  });

  data.client = sock;

  const setStatus = (s) => { data.status = s; data.metrics.statusChangedAt = Date.now(); };

  sock.ev.on('creds.update', saveCreds);

  // ── Connexion / QR / pairing / déconnexion ───────────────────────────────
  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (connection) debugLog(`connection.update: ${connection} registered=${sock.authState?.creds?.registered}${qr ? ' (qr)' : ''}`);

    if (qr) {
      setStatus('QR_READY');
      data.pairingCode = null;
      // Mode pairing code : si un numéro est enregistré et qu'on n'est pas
      // encore appairé, on n'affiche pas le QR (le code sera demandé à part).
      if (data.phoneNumber && !sock.authState.creds.registered) {
        data.qrBase64 = null;
        maybeRequestPairing(data);
      } else {
        try {
          data.qrBase64 = await QRCode.toDataURL(qr);
          io.emit('session:update', { name, status: data.status, qr: data.qrBase64 });
          console.log(`[${name}] 📱 QR Code prêt — scannez depuis le dashboard`);
        } catch (e) {
          console.error(`[${name}] QR toDataURL error:`, e.message);
        }
      }
    }

    if (connection === 'open') {
      if (!sock.authState.creds.registered) {
        console.log(`[${name}] ⏳ registered=false — attente 5s pour finalisation...`);
        debugLog(`registered=false après connexion — attente 5s`);
        await new Promise(r => setTimeout(r, 5000));
        if (!sock.authState.creds.registered) {
          console.warn(`[${name}] ⚠️ registered=false → forcé à true (session déjà couplée)`);
          debugLog(`registered=false → forcé à true`);
          sock.authState.creds.registered = true;
        }
        await saveCreds();
      }
      setStatus('CONNECTED');
      data.qrBase64 = null;
      data.pairingCode = null;
      data.reconnecting = false;
      data.metrics.reconnectCount = 0;
      try {
        const id = sock.user?.id || '';
        data.phone = id.split(':')[0].split('@')[0] || null;
      } catch { data.phone = null; }
      io.emit('session:update', { name, status: data.status });
      console.log(`[${name}] ✅ Connecté et prêt${data.phone ? ' — ' + data.phone : ''}`);
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode
                 || lastDisconnect?.error?.output?.payload?.statusCode;
      data.metrics.lastDisconnect = { reason: String(code || 'unknown'), at: Date.now() };
      // Log COMPLET de la raison (pour diagnostiquer les déconnexions/registered:false)
      const errMsg = lastDisconnect?.error?.message || '';
      const errData = lastDisconnect?.error?.output?.payload
        ? JSON.stringify(lastDisconnect.error.output.payload) : '';
      debugLog(`CLOSE code=${code} registered=${sock.authState?.creds?.registered} msg="${errMsg}" payload=${errData}`);
      // Libérer la garde AVANT de reprogrammer (sinon scheduleReconnect refuse)
      data.reconnecting = false;

      if (code === DisconnectReason.loggedOut) {
        setStatus('AUTH_FAILURE');
        data.metrics.lastError = { msg: 'loggedOut — appareil délié, re-couplage requis', at: Date.now() };
        io.emit('session:update', { name, status: data.status });
        console.warn(`[${name}] ❌ Déconnecté (loggedOut) — re-couplage requis`);
        try { fs.rmSync(authDir, { recursive: true, force: true }); } catch {}
        scheduleReconnect(name, 'loggedOut → nouveau couplage');
      } else {
        setStatus('DISCONNECTED');
        io.emit('session:update', { name, status: data.status });
        console.log(`[${name}] 🔌 Déconnecté (code ${code}) — reconnexion programmée`);
        scheduleReconnect(name, `close code ${code}`);
      }
    }
  });

  // ── Réception messages → forward n8n ─────────────────────────────────────
  sock.ev.on('messages.upsert', async (upsert) => {
    debugLog(`messages.upsert reçu: type=${upsert.type} count=${upsert.messages?.length || 0}`);
    const messages = upsert.messages || [];
    // Accepter 'notify' (nouveaux messages) ET 'append' (certaines versions Baileys)
    if (upsert.type === 'append' && messages.every(m => !m.message)) return;

    for (const m of messages) {
      debugLog(`  msg: fromMe=${m.key?.fromMe} jid=${m.key?.remoteJid} hasMessage=${!!m.message} type=${msgType(m)}`);
      if (!m.message) continue;
      if (m.key.fromMe) continue;
      const jid = m.key.remoteJid;
      if (!jid || jid === 'status@broadcast') continue;

      const body = extractBody(m);
      const from = toLegacyId(jid);
      const t    = msgType(m);

      data.metrics.lastMessageAt = Date.now();
      data.metrics.messageCount += 1;
      recordAnalytics(name, from);
      debugLog(`  → message accepté: from=${from} body="${body?.substring(0,50)}" type=${t}`);

      if ((!body || body.trim() === '') && t === 'chat') {
        data.metrics.emptyBodyCount += 1;
      }

      const webhookUrl = webhookConfig.sessions[name]
        || process.env[`N8N_WEBHOOK_${name.toUpperCase()}`]
        || webhookConfig.global
        || N8N_WEBHOOK;
      if (!webhookUrl) { debugLog(`  ✗ pas de webhook configuré`); continue; }

      axios.post(webhookUrl, {
          event:   'message',
          session: name,
          payload: {
            id:         m.key.id,
            from,
            fromMe:     false,
            body,
            type:       t,
            timestamp:  Number(m.messageTimestamp) || Math.floor(Date.now() / 1000),
            notifyName: m.pushName || '',
          },
        }, { timeout: 30000 })
        .then(() => {
          data.metrics.lastWebhookOkAt = Date.now();
          debugLog(`  ✓ webhook OK`);
        })
        .catch((err) => {
          data.metrics.webhookErrors += 1;
          data.metrics.lastError = { msg: `webhook: ${err.message}`, at: Date.now() };
          debugLog(`  ✗ webhook ERROR: ${err.message}`);
        });
    }
  });

  return sock;
}

// ── Demande un pairing code (couplage par numéro, sans QR) ────────────────────
async function maybeRequestPairing(data) {
  const sock = data.client;
  if (!sock || !data.phoneNumber) return;
  if (sock.authState?.creds?.registered) return;
  try {
    const code = await sock.requestPairingCode(data.phoneNumber);
    data.pairingCode = code;
    data.qrBase64 = null;
    console.log(`[${data.name}] 📲 Code de couplage prêt: ${code}`);
    io.emit('session:update', { name: data.name, status: data.status, pairingCode: code });
  } catch (e) {
    console.error(`[${data.name}] requestPairingCode échoué:`, e.message);
    data.metrics.lastError = { msg: `pairing: ${e.message}`, at: Date.now() };
  }
}

// ── Détruit proprement un socket Baileys (sans déclencher de reconnexion) ─────
function destroySocket(sock) {
  if (!sock) return;
  try { sock.ev.removeAllListeners('connection.update'); } catch {}
  try { sock.ev.removeAllListeners('messages.upsert'); } catch {}
  try { sock.ev.removeAllListeners('creds.update'); } catch {}
  try { sock.end(undefined); } catch {}
  try { sock.ws?.close(); } catch {}
}

// ── scheduleReconnect : ferme le socket mort puis en recrée un ────────────────
function scheduleReconnect(name, reason) {
  const data = sessions.get(name);
  if (!data || data.stopped) return;
  if (data.reconnecting) return;
  data.reconnecting = true;

  if (data.metrics.reconnectCount >= MAX_RECONNECT_TRIES) {
    data.metrics.lastError = { msg: `abandon reconnexion après ${MAX_RECONNECT_TRIES} essais (${reason})`, at: Date.now() };
    console.error(`[${name}] 🛑 Reconnexion abandonnée après ${MAX_RECONNECT_TRIES} essais.`);
    data.reconnecting = false;
    return;
  }

  const tries = data.metrics.reconnectCount;
  const delay = Math.min(RECONNECT_BASE_MS * Math.pow(2, tries), RECONNECT_MAX_MS);
  console.log(`[${name}] 🔄 Reconnexion dans ${Math.round(delay/1000)}s (essai #${tries + 1}) — ${reason}`);

  clearTimeout(data.reconnectTimer);
  data.reconnectTimer = setTimeout(async () => {
    data.metrics.reconnectCount += 1;
    destroySocket(data.client);
    data.client = null;
    try {
      console.log(`[${name}] 🔁 Recréation d'un socket neuf...`);
      await spawnClient(data);
      data.reconnecting = false;  // reconfirmé sur 'open'
    } catch (e) {
      data.reconnecting = false;
      data.metrics.lastError = { msg: `recréation: ${e.message}`, at: Date.now() };
      console.error(`[${name}] Recréation échouée:`, e.message);
      scheduleReconnect(name, `retry après échec recréation`);
    }
  }, delay);
}

// ── Watchdog : détecte les blocages d'initialisation ──────────────────────────
// Baileys n'a pas de "zombie Chrome" (pas de navigateur). Mais un socket peut
// rester coincé en INITIALIZING (sync qui ne finit jamais). Au-delà de
// INIT_TIMEOUT_MS, on force une recréation propre.
function startWatchdog(data) {
  const name = data.name;
  clearInterval(data.watchdogTimer);
  data.watchdogTimer = setInterval(async () => {
    if (data.stopped) return;
    data.metrics.lastWatchdogAt = Date.now();

    // Détection blocage init (INITIALIZING/AUTHENTICATED > 3 min)
    if (data.status === 'INITIALIZING' || data.status === 'AUTHENTICATED') {
      const stuckMs = Date.now() - data.metrics.statusChangedAt;
      if (stuckMs > INIT_TIMEOUT_MS) {
        data.metrics.zombieKills += 1;
        data.reconnecting = false; // force-libérer la garde
        data.metrics.lastError = { msg: `init bloqué à ${data.status} depuis ${Math.round(stuckMs/1000)}s`, at: Date.now() };
        console.warn(`[${name}] ⏳ Watchdog : bloqué à ${data.status} → recréation forcée`);
        scheduleReconnect(name, `init stuck ${Math.round(stuckMs/1000)}s`);
      }
    }

    // Détection DISCONNECTED sans reconnexion en cours (garde bloquée)
    if (data.status === 'DISCONNECTED' && !data.reconnecting) {
      const stuckMs = Date.now() - data.metrics.statusChangedAt;
      if (stuckMs > 60_000) { // déconnecté > 1 min sans reconnexion
        data.metrics.zombieKills += 1;
        console.warn(`[${name}] ⏳ Watchdog : DISCONNECTED depuis ${Math.round(stuckMs/1000)}s sans reconnexion → relance`);
        scheduleReconnect(name, `watchdog disconnected stuck ${Math.round(stuckMs/1000)}s`);
      }
    }
  }, WATCHDOG_INTERVAL_MS);
}

async function stopSession(name) {
  const s = sessions.get(name);
  if (!s) return false;
  s.stopped = true;
  clearInterval(s.watchdogTimer);
  clearTimeout(s.reconnectTimer);
  destroySocket(s.client);
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
      if (name && sessions.size < MAX_SESSIONS) {
        console.log(`[Auto-start] Reprise session: ${name}`);
        createSession(name);
      } else if (name) {
        console.warn(`[Auto-start] Plafond ${MAX_SESSIONS} atteint — "${name}" non démarrée`);
      }
    });
}

// ── Helpers d'envoi ───────────────────────────────────────────────────────────

const randomDelay = (min, max) =>
  new Promise(r => setTimeout(r, Math.floor(Math.random() * (max - min + 1)) + min));

const getSession = (session) => {
  const s = sessions.get(session);
  if (!s)                         throw new Error(`Session "${session}" introuvable`);
  if (s.status !== 'CONNECTED')   throw new Error(`Session "${session}" non connectée (${s.status})`);
  return s;
};

// Récupère un média (fichier local /media/... si possible, sinon download) → Buffer
async function fetchMediaBuffer(url) {
  const mediaMatch = url.match(/\/media\/([^/?#]+)/);
  if (mediaMatch) {
    const filePath = path.join(MEDIA_DIR, mediaMatch[1]);
    if (fs.existsSync(filePath)) {
      console.log('[media] lecture locale:', filePath);
      return fs.readFileSync(filePath);
    }
  }
  console.log('[media] téléchargement URL:', url);
  const resp = await axios.get(url, { responseType: 'arraybuffer', timeout: 20000 });
  return Buffer.from(resp.data);
}

// ── Routes ────────────────────────────────────────────────────────────────────

app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.get('/health', (_req, res) => res.json({ ok: true, sessions: sessions.size }));

// ── Sessions ──────────────────────────────────────────────────────────────────

app.get('/api/sessions', auth, (_req, res) => {
  res.json({
    sessions: [...sessions.values()].map(s => ({
      name:        s.name,
      status:      s.status,
      hasQr:       !!s.qrBase64,
      pairingCode: s.pairingCode || null,
    })),
  });
});

app.post('/api/sessions/start', auth, (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'name requis' });
  if (!sessions.has(name) && sessions.size >= MAX_SESSIONS) {
    return res.status(429).json({ error: `Plafond de ${MAX_SESSIONS} sessions atteint (anti-surcharge). Arrêtez-en une d'abord.` });
  }
  const s = createSession(name);
  res.json({ success: true, name: s.name, status: s.status });
});

app.delete('/api/sessions/:name/stop', auth, async (req, res) => {
  const ok = await stopSession(req.params.name);
  res.json({ success: ok });
});

app.delete('/api/sessions/:name/reset', auth, async (req, res) => {
  const name = req.params.name;
  await stopSession(name);
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
  res.json({ name: s.name, status: s.status, phone: s.phone || null, pairingCode: s.pairingCode || null });
});

// POST /api/sessions/:name/pairing-code  { phone }
app.post('/api/sessions/:name/pairing-code', auth, async (req, res) => {
  const { name } = req.params;
  const { phone } = req.body;
  if (!phone) return res.status(400).json({ error: 'phone requis (ex: 22890123456 — sans + ni espaces)' });

  const data = sessions.get(name);
  if (!data) return res.status(404).json({ error: 'Session introuvable' });

  const normalizedPhone = String(phone).replace(/[^0-9]/g, '');
  if (normalizedPhone.length < 7) return res.status(400).json({ error: 'Numéro invalide' });
  data.phoneNumber = normalizedPhone;

  if (data.client && !data.client.authState?.creds?.registered) {
    try {
      const code = await data.client.requestPairingCode(normalizedPhone);
      data.pairingCode = code;
      data.qrBase64 = null;
      console.log(`[${name}] 📲 Pairing code: ${code}`);
      io.emit('session:update', { name, status: data.status, pairingCode: code });
      return res.json({ success: true, code });
    } catch (e) {
      console.error(`[${name}] requestPairingCode error:`, e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  res.json({ success: true, message: 'Numéro enregistré — le code sera généré au prochain cycle' });
});

app.get('/api/sessions/:name/qr', auth, (req, res) => {
  const s = sessions.get(req.params.name);
  if (!s)                       return res.status(404).json({ error: 'Session introuvable' });
  if (s.status === 'CONNECTED') return res.json({ status: s.status, message: 'Déjà connecté' });
  if (!s.qrBase64)              return res.json({ status: s.status, message: 'QR pas encore disponible, patientez…' });
  res.json({ status: s.status, qrCodeBase64: s.qrBase64 });
});

app.get('/api/qr', auth, (req, res) => {
  const name = req.query.session || 'default';
  const s = sessions.get(name);
  if (!s)                       return res.status(404).json({ error: 'Session introuvable' });
  if (s.status === 'CONNECTED') return res.json({ status: s.status });
  if (!s.qrBase64)              return res.json({ status: s.status, message: 'QR pas encore disponible' });
  res.json({ status: s.status, qrCodeBase64: s.qrBase64 });
});

// ── Envoi de messages (compatibles WAHA / v1) ─────────────────────────────────

// POST /api/sendText   { chatId, text, session }
app.post('/api/sendText', auth, async (req, res) => {
  let { chatId, text, session = 'default' } = req.body;
  if (!chatId || !text) return res.status(400).json({ error: 'chatId et text requis' });

  try {
    const s   = getSession(session);
    const jid = toJid(chatId);

    await s.client.sendPresenceUpdate('composing', jid);
    await randomDelay(1200, 2500);
    await s.client.sendMessage(jid, { text });
    await s.client.sendPresenceUpdate('paused', jid);

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
    const s   = getSession(session);
    const jid = toJid(chatId);
    console.log('[sendVoice] jid:', jid, 'url:', file.url);

    const buffer = await fetchMediaBuffer(file.url);
    if (!buffer || buffer.length < 500) {
      throw new Error(`Fichier audio invalide ou introuvable (${buffer ? buffer.length : 0} bytes)`);
    }

    await s.client.sendPresenceUpdate('recording', jid);
    await randomDelay(1500, 3000);
    // ptt: true → note vocale (et non pièce jointe audio)
    await s.client.sendMessage(jid, { audio: buffer, ptt: true, mimetype: 'audio/ogg; codecs=opus' });
    await s.client.sendPresenceUpdate('paused', jid);

    console.log('[sendVoice] PTT envoyé ✓');
    res.json({ success: true });
  } catch (err) {
    const msg = err?.message || String(err);
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
    const s   = getSession(session);
    const jid = toJid(chatId);
    console.log('[sendVideo] jid:', jid, 'url:', file.url);

    const buffer = await fetchMediaBuffer(file.url);
    if (!buffer || buffer.length < 500) {
      throw new Error(`Fichier vidéo invalide ou introuvable (${buffer ? buffer.length : 0} bytes)`);
    }

    await randomDelay(500, 1500);
    await s.client.sendMessage(jid, { video: buffer, caption: caption || undefined });
    console.log('[sendVideo] envoyé ✓');

    res.json({ success: true });
  } catch (err) {
    const msg = err?.message || String(err);
    const sd = sessions.get(session);
    if (sd?.metrics) { sd.metrics.mediaErrors += 1; sd.metrics.lastError = { msg: `sendVideo: ${msg}`, at: Date.now() }; }
    console.error('[sendVideo] ERREUR FINALE:', msg);
    res.status(500).json({ success: false, error: msg });
  }
});

// POST /api/sendFile   { chatId, session, file: { url, name }, caption }
app.post('/api/sendFile', auth, async (req, res) => {
  let { chatId, session = 'default', file, caption = '' } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId requis' });
  if (!file?.url) return res.json({ success: true, skipped: true, reason: 'Aucune URL fichier configurée' });

  try {
    const s   = getSession(session);
    const jid = toJid(chatId);

    const buffer = await fetchMediaBuffer(file.url);
    const fileName = file.name || 'document';
    await randomDelay(500, 1500);
    await s.client.sendMessage(jid, {
      document: buffer,
      fileName,
      caption: caption || undefined,
      mimetype: file.mimeType || 'application/octet-stream',
    });

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
    const s = getSession(session);
    await s.client.sendPresenceUpdate('composing', toJid(chatId));
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/stopTyping   { chatId, session }
app.post('/api/stopTyping', auth, async (req, res) => {
  let { chatId, session = 'default' } = req.body;
  try {
    const s = getSession(session);
    await s.client.sendPresenceUpdate('paused', toJid(chatId));
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Server info ───────────────────────────────────────────────────────────────

app.get('/api/server/info', auth, (_req, res) => {
  res.json({
    version:  VERSION,
    engine:   'baileys',
    uptime:   Math.floor((Date.now() - START_TIME) / 1000),
    sessions: sessions.size,
    maxSessions: MAX_SESSIONS,
    connected: [...sessions.values()].filter(s => s.status === 'CONNECTED').length,
  });
});

// ── Monitoring détaillé ─────────────────────────────────────────────────────────

const ZOMBIE_SILENCE_MS = 30 * 60 * 1000;

function sessionHealth(s) {
  const m = s.metrics || {};
  const now = Date.now();
  const issues = [];
  let health = 'ok';

  if (s.status !== 'CONNECTED') {
    health = s.status === 'DISCONNECTED' || s.status === 'AUTH_FAILURE' || s.status === 'ERROR' ? 'critical' : 'warn';
    issues.push(`Statut: ${s.status}`);
  }

  if (s.status === 'CONNECTED' && m.emptyBodyCount > 0) {
    if (health === 'ok') health = 'warn';
    issues.push(`${m.emptyBodyCount} message(s) à body vide`);
  }

  if (m.reconnectCount >= 3) {
    if (health === 'ok') health = 'warn';
    issues.push(`${m.reconnectCount} reconnexions`);
  }

  if (m.webhookErrors > 0) {
    if (health === 'ok') health = 'warn';
    issues.push(`${m.webhookErrors} erreur(s) webhook n8n`);
  }

  if (m.zombieKills > 0) {
    if (health === 'ok') health = 'warn';
    issues.push(`${m.zombieKills} recréation(s) forcée(s)`);
  }

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

// ── Analytics ─────────────────────────────────────────────────────────────────

function bucketKey(ts, granularity) {
  const d = new Date(ts);
  const p = (n) => String(n).padStart(2, '0');
  switch (granularity) {
    case 'minute': return `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
    case 'hour':   return `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())} ${p(d.getHours())}:00`;
    case 'day':
    default:       return `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())}`;
  }
}

app.get('/api/analytics', auth, (req, res) => {
  const now = Date.now();
  const to   = req.query.to   ? Number(req.query.to)   : now;
  const from = req.query.from ? Number(req.query.from) : now - 7 * 86400000;
  const session = req.query.session && req.query.session !== 'all' ? req.query.session : null;
  const granularity = ['minute', 'hour', 'day'].includes(req.query.granularity) ? req.query.granularity : 'day';

  const filtered = analyticsEvents.filter(e =>
    e.t >= from && e.t <= to && (!session || e.s === session)
  );

  const buckets = new Map();
  const byHour    = Array(24).fill(0);
  const byWeekday = Array(7).fill(0);
  const uniqueContacts = new Set();

  for (const e of filtered) {
    const k = bucketKey(e.t, granularity);
    if (!buckets.has(k)) buckets.set(k, { messages: 0, contacts: new Set() });
    const b = buckets.get(k);
    b.messages += 1;
    b.contacts.add(e.f);
    const d = new Date(e.t);
    byHour[d.getHours()] += 1;
    byWeekday[d.getDay()] += 1;
    uniqueContacts.add(e.f);
  }

  const series = [...buckets.entries()]
    .sort((a, b) => a[0] < b[0] ? -1 : 1)
    .map(([bucket, v]) => ({ bucket, messages: v.messages, conversations: v.contacts.size }));

  const peakHour = byHour.indexOf(Math.max(...byHour));
  const WD = ['Dimanche','Lundi','Mardi','Mercredi','Jeudi','Vendredi','Samedi'];
  const peakWeekday = byWeekday.indexOf(Math.max(...byWeekday));

  res.json({
    range: { from, to, granularity, session: session || 'all' },
    totals: {
      messages:      filtered.length,
      conversations: uniqueContacts.size,
      avgPerConv:    uniqueContacts.size ? +(filtered.length / uniqueContacts.size).toFixed(1) : 0,
      peakHour:      filtered.length ? peakHour : null,
      peakWeekday:   filtered.length ? WD[peakWeekday] : null,
    },
    series,
    byHour,
    byWeekday,
    sessionsList: [...new Set(analyticsEvents.map(e => e.s))],
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

app.post('/api/media/upload', auth, (req, res) => {
  const { name, data, mimeType } = req.body;
  if (!name || !data) return res.status(400).json({ error: 'name et data (base64) requis' });

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
  socket.emit('init', [...sessions.values()].map(s => ({
    name:        s.name,
    status:      s.status,
    qr:          s.qrBase64,
    pairingCode: s.pairingCode || null,
  })));
});

// ── Démarrage ─────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`\n🔥 Camille Core v${VERSION} (Baileys) → http://localhost:${PORT}`);
  console.log(`🔑 API Key      → ${API_KEY}`);
  console.log(`📡 Webhook n8n  → ${N8N_WEBHOOK || '(non configuré)'}`);
  console.log(`🧩 Max sessions → ${MAX_SESSIONS}\n`);
  autoStartSessions();
});

// Sécurité : ne jamais laisser une exception non gérée tuer le process
process.on('unhandledRejection', (reason) => {
  console.error('[unhandledRejection]', reason?.message || reason);
});
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err?.message || err);
});
