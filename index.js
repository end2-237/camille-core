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
  Browsers,
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

// Logger Baileys → fichier baileys.log. Par défaut 'fatal' (quasi silencieux) :
// sur un gros compte, le flot d'erreurs de déchiffrement écrites sur disque
// ajoutait de l'I/O pendant la sync et aggravait le blocage de l'event loop.
// Notre debugLog (léger) suffit pour suivre le cycle de connexion.
// Mettre BAILEYS_LOG_LEVEL=warn (ou debug) pour ré-activer le diagnostic.
try { fs.mkdirSync(SESSIONS_DIR, { recursive: true }); } catch {}
const logger = pino(
  { level: process.env.BAILEYS_LOG_LEVEL || 'fatal' },
  pino.destination(path.join(SESSIONS_DIR, 'baileys.log'))
);

// Debug applicatif → fichier (docker logs trop lent sur ce serveur)
const debugLog = (msg) => {
  const line = `[${new Date().toISOString()}] ${msg}\n`;
  fs.appendFile(path.join(SESSIONS_DIR, 'debug.log'), line, () => {});
};
const MEDIA_DIR     = path.join(__dirname, 'public', 'media');
const VERSION       = '3.0.0';
const START_TIME    = Date.now();

// Version de Baileys réellement installée (pour la page Versioning du dashboard)
let BAILEYS_VERSION = 'inconnue';
try { BAILEYS_VERSION = require('@whiskeysockets/baileys/package.json').version; } catch {}

// ── Journal par session (ring buffer en mémoire) ──────────────────────────────
// Chaque session garde ses N dernières lignes d'événements → affichées en direct
// dans le monitoring, à côté des détails de la session.
const SESSION_LOG_MAX = 250;
const sessionLogRing = new Map(); // name → [{ t, msg }]
function pushSessionLog(name, msg) {
  if (!name) return;
  let ring = sessionLogRing.get(name);
  if (!ring) { ring = []; sessionLogRing.set(name, ring); }
  ring.push({ t: Date.now(), msg });
  if (ring.length > SESSION_LOG_MAX) ring.shift();
}

// ── Stabilité : watchdog & reconnexion ────────────────────────────────────────
const WATCHDOG_INTERVAL_MS = Number(process.env.WATCHDOG_INTERVAL_MS) || 60_000;  // surveillance toutes les 60s
const RECONNECT_BASE_MS    = Number(process.env.RECONNECT_BASE_MS)    || 5_000;   // backoff initial
const RECONNECT_MAX_MS     = 5 * 60_000;                                          // backoff plafonné à 5 min
const MAX_RECONNECT_TRIES  = Number(process.env.MAX_RECONNECT_TRIES)  || 10;      // au-delà → stoppe la boucle
const INIT_TIMEOUT_MS      = Number(process.env.INIT_TIMEOUT_MS)      || 180_000; // bloqué en init > 3 min → recréation forcée
// ── Anti-zombie ───────────────────────────────────────────────────────────────
// Un device lié peut rester CONNECTED (keep-alive OK) alors que WhatsApp ne lui
// livre plus AUCUN message. Le keep-alive ne détecte pas ça. On surveille donc
// le silence du flux : après PROBE on sonde activement la connexion (round-trip
// serveur), après HARD on force une reconnexion propre quoi qu'il arrive.
const ZOMBIE_PROBE_AFTER_MS = Number(process.env.ZOMBIE_PROBE_AFTER_MS) || 5 * 60_000;   // sonde après 5 min de silence
const ZOMBIE_HARD_MS        = Number(process.env.ZOMBIE_HARD_MS)        || 30 * 60_000;  // reco forcée après 30 min de silence
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
// IMPORTANT : dans SESSIONS_DIR (volume Docker) et NON __dirname (/app, éphémère),
// sinon la config des webhooks est perdue à chaque redéploiement/rebuild.
const WEBHOOKS_FILE = path.join(SESSIONS_DIR, 'webhooks.json');

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

// Limite à 30 Mo : un base64 de 100 Mo en RAM × plusieurs uploads simultanés
// pouvait déclencher l'OOM sur le container (1536 Mo). 30 Mo couvre largement
// vidéos/docs WhatsApp (limite WA = 16 Mo média, 100 Mo doc → base64 ~40 Mo,
// mais on passe par fetchMediaBuffer côté URL pour les gros).
app.use(express.json({ limit: '30mb' }));
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

// ── Caches de stabilité ───────────────────────────────────────────────────────
// (a) Messages envoyés : pour getMessage() → permet à Baileys de RE-chiffrer un
//     message quand le destinataire demande un "retry" (sinon "en attente de ce
//     message" côté contact). Borné en mémoire.
const SENT_MSG_CACHE_MAX = 1000;
const sentMessages = new Map(); // id → message
function rememberMessage(id, message) {
  if (!id || !message) return;
  if (sentMessages.has(id)) return;
  sentMessages.set(id, message);
  if (sentMessages.size > SENT_MSG_CACHE_MAX) {
    sentMessages.delete(sentMessages.keys().next().value);
  }
}

// (b) Déduplication des messages entrants : WhatsApp peut re-livrer un message
//     après une reconnexion → sans ça, le webhook part 2× → double réponse du bot.
const DEDUP_MAX = 3000;
const seenMessageIds = new Set();
function alreadySeen(id) {
  if (!id) return false;
  if (seenMessageIds.has(id)) return true;
  seenMessageIds.add(id);
  if (seenMessageIds.size > DEDUP_MAX) {
    seenMessageIds.delete(seenMessageIds.values().next().value);
  }
  return false;
}

// (c) Métadonnées de groupe en cache : évite que Baileys re-interroge le serveur
//     à CHAQUE message de groupe (→ rate-limit WhatsApp → déconnexion).
const groupMetaCache = new Map(); // jid → metadata

// (d) Envoi webhook avec réessais (backoff) : un hoquet de n8n ne fait plus
//     perdre le message.
async function postWebhookWithRetry(url, payload, name) {
  const delays = [0, 2000, 5000]; // 3 tentatives : immédiat, +2s, +5s
  let lastErr;
  for (let i = 0; i < delays.length; i++) {
    if (delays[i]) await new Promise(r => setTimeout(r, delays[i]));
    try {
      await axios.post(url, payload, { timeout: 30000 });
      return true;
    } catch (e) {
      lastErr = e;
      debugLog(`  ⟳ webhook tentative ${i + 1}/${delays.length} KO: ${e.message}`);
    }
  }
  throw lastErr;
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
    pairingRequested: false, // garde : 1 seul code par socket (régénérer invalide le précédent)
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
      lastStreamAt:     Date.now(),   // dernier événement de flux (heartbeat anti-zombie)
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

  // Socket NEUF → tout pairing code précédent est mort. On réautorise une
  // demande (et une seule) pour ce nouveau socket.
  data.pairingRequested = false;

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
    // Identité desktop RÉELLE : un browser custom ('Camille Core') fait rejeter
    // les codes de pairing par WhatsApp (issues #1761/#2370). Browsers.macOS('Chrome')
    // = chaîne standard reconnue → pairing fiable.
    browser: Browsers.macOS('Chrome'),
    // ── Durcissement pour comptes à fort volume (centaines de conversations) ──
    // Les "init queries" (privacy, blocklist, app-state) timeoutaient à ~30s sur
    // les gros comptes → connexion instable. On laisse 60s.
    defaultQueryTimeoutMs: 60_000,
    connectTimeoutMs:      60_000,
    // qrTimeout : délai laissé pour scanner le QR / saisir le pairing code avant
    // que Baileys ne ferme la connexion. 60s par défaut = trop court sur un gros
    // compte lent (l'utilisateur n'a pas le temps). On laisse 5 min.
    qrTimeout:             300_000,
    keepAliveIntervalMs:   25_000,   // ping moins agressif → moins de faux "keep alive failed"
    retryRequestDelayMs:   2_000,
    // ── Synchronisation ───────────────────────────────────────────────────────
    // syncFullHistory: false → on NE télécharge PAS tout l'historique (un compte
    // à centaines de chats = énorme). MAIS on NE désactive PAS la synchro
    // essentielle (shouldSyncHistoryMessage par défaut) : c'est elle qui peuple
    // les mappings LID. Sans ça, WhatsApp se connecte mais ne livre AUCUN message
    // des contacts @lid (cause racine du "connecté mais sourd"). Cf. bug Baileys
    // connu : "syncFullHistory:false + shouldSyncHistoryMessage:false → no LID
    // mapping → no messages despite successful connection".
    syncFullHistory: false,
    markOnlineOnConnect: false,      // n'apparaît pas "en ligne" en permanence
    generateHighQualityLinkPreview: false,
    // getMessage : renvoie l'original depuis notre cache pour satisfaire les
    // demandes de retry de (re)chiffrement de WhatsApp (sinon "en attente de ce
    // message" côté contact). undefined si absent → Baileys gère proprement.
    getMessage: async (key) => sentMessages.get(key?.id),
    // cachedGroupMetadata : sert les métadonnées de groupe depuis le cache local
    // au lieu de ré-interroger le serveur à chaque message → anti rate-limit.
    cachedGroupMetadata: async (jid) => groupMetaCache.get(jid),
  });

  data.client = sock;

  const setStatus = (s) => { data.status = s; data.metrics.statusChangedAt = Date.now(); };

  // Journal de session : écrit dans debug.log (préfixé [name]) ET dans le ring
  // buffer mémoire exposé par /api/sessions/:name/logs.
  const slog = (msg) => { debugLog(`[${name}] ${msg}`); pushSessionLog(name, msg); };

  sock.ev.on('creds.update', saveCreds);

  // ── Connexion / QR / pairing / déconnexion ───────────────────────────────
  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (connection) slog(`connection.update: ${connection} registered=${sock.authState?.creds?.registered}${qr ? ' (qr)' : ''}`);

    if (qr) {
      setStatus('QR_READY');
      data.pairingCode = null;
      // Mode pairing code : si un numéro est enregistré et qu'on n'est pas
      // encore appairé, on n'affiche pas le QR (le code sera demandé à part).
      if (data.phoneNumber && !sock.authState.creds.registered) {
        data.qrBase64 = null;
        // IMPORTANT : ne régénère PAS le code à chaque rotation de QR — un
        // nouveau requestPairingCode invalide le précédent, ce qui fait
        // échouer la saisie côté client. On ne demande qu'une fois par socket.
        if (!data.pairingRequested) maybeRequestPairing(data);
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
      // NB : on ne touche JAMAIS à creds.registered. Si la registration n'a pas
      // fini (init queries lentes), Baileys la finalise tout seul une fois la
      // connexion stable. On se contente de logger l'état pour diagnostic.
      slog(`OPEN registered=${sock.authState?.creds?.registered}`);
      await saveCreds();
      setStatus('CONNECTED');
      data.metrics.lastStreamAt = Date.now();  // repart à neuf : pas de faux zombie juste après reconnexion
      data.qrBase64 = null;
      data.pairingCode = null;
      data.reconnecting = false;
      data.reconnectingSince = null;
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
      slog(`CLOSE code=${code} registered=${sock.authState?.creds?.registered} msg="${errMsg}" payload=${errData}`);
      // Libérer la garde AVANT de reprogrammer (sinon scheduleReconnect refuse)
      data.reconnecting = false;

      // Un "conflict"/"replaced" = deux connexions concurrentes (autre appareil
      // OU deuxième socket/container). Ce n'est PAS un logout : il ne faut SURTOUT
      // pas effacer les creds (sinon on perd le couplage et il faut re-scanner).
      const isConflict = /conflict|replaced/i.test(errMsg)
                      || code === DisconnectReason.connectionReplaced;
      // 515 = Baileys demande un redémarrage du flux, typiquement juste après le
      // couplage. C'est normal → reconnexion IMMÉDIATE pour finaliser la registration.
      const isRestart  = code === DisconnectReason.restartRequired;

      const isUnauthorized = code === 401 || code === DisconnectReason.loggedOut;
      const wasRegistered  = sock.authState?.creds?.registered;
      if (isUnauthorized && !isConflict && !wasRegistered) {
        // 401 + registered=false = enregistrement JAMAIS terminé → les creds sont
        // provisoires et inutilisables. On les efface et on re-couple proprement.
        setStatus('AUTH_FAILURE');
        data.metrics.lastError = { msg: `auth failure (${code}) registration incomplète — re-couplage`, at: Date.now() };
        io.emit('session:update', { name, status: data.status });
        console.warn(`[${name}] ❌ Auth failure code=${code} registered=false — creds provisoires effacés, re-couplage`);
        try { fs.rmSync(authDir, { recursive: true, force: true }); } catch {}
        scheduleReconnect(name, `auth failure ${code} (non enregistré) → nouveau couplage`, { immediate: true });
      } else if (isUnauthorized && !isConflict && wasRegistered) {
        // 401 sur une session DÉJÀ enregistrée : probablement un conflit auto-infligé
        // (2 sockets) byte-identique à un vrai logout. On NE supprime PAS les creds
        // (sinon boucle de re-couplage infinie #2110/#2248). On reconnecte avec les
        // mêmes creds ; si c'est un vrai logout, le couplage WhatsApp restera invalide.
        setStatus('DISCONNECTED');
        data.metrics.lastError = { msg: `401 sur session enregistrée — reconnexion sans effacer creds`, at: Date.now() };
        io.emit('session:update', { name, status: data.status });
        console.warn(`[${name}] ⚠️  401 (registered=true) — conflit probable, reconnexion SANS effacer les creds`);
        scheduleReconnect(name, `401 enregistré (conflit probable)`);
      } else if (isRestart) {
        setStatus('DISCONNECTED');
        console.log(`[${name}] 🔁 Restart required (515) — reconnexion immédiate`);
        scheduleReconnect(name, 'restart required (515)', { immediate: true });
      } else {
        setStatus('DISCONNECTED');
        io.emit('session:update', { name, status: data.status });
        console.log(`[${name}] 🔌 Déconnecté (code ${code}${isConflict ? ' conflict' : ''}) — reconnexion programmée`);
        scheduleReconnect(name, `close code ${code}`);
      }
    }
  });

  // ── Réception messages → forward n8n ─────────────────────────────────────
  sock.ev.on('messages.upsert', async (upsert) => {
    // Tout événement de flux (même fromMe=true / status) prouve que la connexion
    // reçoit encore → sert de battement de cœur pour la détection anti-zombie.
    data.metrics.lastStreamAt = Date.now();
    slog(`messages.upsert reçu: type=${upsert.type} count=${upsert.messages?.length || 0}`);
    const messages = upsert.messages || [];
    // Accepter 'notify' (nouveaux messages) ET 'append' (certaines versions Baileys)
    if (upsert.type === 'append' && messages.every(m => !m.message)) return;

    for (const m of messages) {
      // Mémoriser TOUT message avec contenu (y compris les nôtres, fromMe) pour
      // getMessage() → permet le retry de (re)chiffrement demandé par WhatsApp.
      if (m.message && m.key?.id) rememberMessage(m.key.id, m.message);

      slog(`msg: fromMe=${m.key?.fromMe} jid=${m.key?.remoteJid} hasMessage=${!!m.message} type=${msgType(m)}`);
      if (!m.message) continue;
      if (m.key.fromMe) continue;
      const jid = m.key.remoteJid;
      if (!jid || jid === 'status@broadcast') continue;

      // Anti-doublon : un même message re-livré après reconnexion ne déclenche
      // qu'UN seul webhook.
      if (alreadySeen(m.key.id)) {
        slog(`⊘ doublon ignoré id=${m.key.id}`);
        continue;
      }

      const body = extractBody(m);
      const from = toLegacyId(jid);
      const t    = msgType(m);

      data.metrics.lastMessageAt = Date.now();
      data.metrics.messageCount += 1;
      recordAnalytics(name, from);
      slog(`→ message accepté: from=${from} body="${body?.substring(0,50)}" type=${t}`);

      if ((!body || body.trim() === '') && t === 'chat') {
        data.metrics.emptyBodyCount += 1;
      }

      const webhookUrl = webhookConfig.sessions[name]
        || process.env[`N8N_WEBHOOK_${name.toUpperCase()}`]
        || webhookConfig.global
        || N8N_WEBHOOK;
      if (!webhookUrl) { slog(`✗ pas de webhook configuré`); continue; }

      postWebhookWithRetry(webhookUrl, {
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
        }, name)
        .then(() => {
          data.metrics.lastWebhookOkAt = Date.now();
          slog(`✓ webhook OK (${from})`);
        })
        .catch((err) => {
          data.metrics.webhookErrors += 1;
          data.metrics.lastError = { msg: `webhook: ${err.message}`, at: Date.now() };
          slog(`✗ webhook ERROR (après retries): ${err.message}`);
        });
    }
  });

  // ── Cache des métadonnées de groupe (anti rate-limit) ─────────────────────
  // On rafraîchit le cache quand un groupe change ou que ses participants bougent.
  const refreshGroupMeta = async (jid) => {
    try {
      const meta = await sock.groupMetadata(jid);
      if (meta) groupMetaCache.set(jid, meta);
    } catch (e) { debugLog(`  groupMetadata KO ${jid}: ${e.message}`); }
  };
  sock.ev.on('groups.update', async (updates) => {
    for (const u of updates) if (u.id) await refreshGroupMeta(u.id);
  });
  sock.ev.on('group-participants.update', async (ev) => {
    if (ev.id) await refreshGroupMeta(ev.id);
  });
  sock.ev.on('groups.upsert', async (groups) => {
    for (const g of groups) if (g.id) groupMetaCache.set(g.id, g);
  });

  return sock;
}

// ── Demande un pairing code (couplage par numéro, sans QR) ────────────────────
async function maybeRequestPairing(data) {
  const sock = data.client;
  if (!sock || !data.phoneNumber) return;
  if (sock.authState?.creds?.registered) return;
  if (data.pairingRequested) return;
  data.pairingRequested = true;
  // Laisser le socket finir son init avant de demander le code —
  // trop tôt = WhatsApp rejette silencieusement la demande.
  await new Promise(r => setTimeout(r, 3000));
  if (!data.client || data.client !== sock) return; // socket remplacé entre-temps
  try {
    const code = await sock.requestPairingCode(data.phoneNumber);
    data.pairingCode = code;
    data.qrBase64 = null;
    console.log(`[${data.name}] 📲 Code de couplage prêt: ${code}`);
    io.emit('session:update', { name: data.name, status: data.status, pairingCode: code });
  } catch (e) {
    console.error(`[${data.name}] requestPairingCode échoué:`, e.message);
    data.metrics.lastError = { msg: `pairing: ${e.message}`, at: Date.now() };
    data.pairingRequested = false;  // permettre un retry au prochain QR
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
function scheduleReconnect(name, reason, opts = {}) {
  const data = sessions.get(name);
  if (!data || data.stopped) return;
  if (data.reconnecting) return;
  data.reconnecting = true;
  data.reconnectingSince = Date.now();   // pour détecter une reconnexion coincée
  pushSessionLog(name, `🔄 reconnexion programmée — ${reason}`);

  // Les reconnexions "immédiates" (515 post-couplage) ne comptent pas dans le
  // quota d'essais : c'est une étape normale du protocole, pas un échec.
  if (!opts.immediate && data.metrics.reconnectCount >= MAX_RECONNECT_TRIES) {
    data.metrics.lastError = { msg: `abandon reconnexion après ${MAX_RECONNECT_TRIES} essais (${reason})`, at: Date.now() };
    console.error(`[${name}] 🛑 Reconnexion abandonnée après ${MAX_RECONNECT_TRIES} essais.`);
    data.reconnecting = false;
    return;
  }

  const tries = data.metrics.reconnectCount;
  const delay = opts.immediate
    ? 1500
    : Math.min(RECONNECT_BASE_MS * Math.pow(2, tries), RECONNECT_MAX_MS);
  console.log(`[${name}] 🔄 Reconnexion dans ${Math.round(delay/1000)}s (essai #${tries + 1}) — ${reason}`);

  clearTimeout(data.reconnectTimer);
  data.reconnectTimer = setTimeout(async () => {
    if (!opts.immediate) data.metrics.reconnectCount += 1;
    destroySocket(data.client);
    data.client = null;
    try {
      console.log(`[${name}] 🔁 Recréation d'un socket neuf...`);
      await spawnClient(data);
      // NE PAS remettre reconnecting=false ici : on garde la garde jusqu'à
      // l'événement 'open' (succès) ou un nouveau 'close'. Sinon une 2e
      // reconnexion peut créer un socket CONCURRENT → conflict (401).
    } catch (e) {
      data.reconnecting = false;
      data.metrics.lastError = { msg: `recréation: ${e.message}`, at: Date.now() };
      console.error(`[${name}] Recréation échouée:`, e.message);
      scheduleReconnect(name, `retry après échec recréation`);
    }
  }, delay);
}

// ── Sonde de vivacité : round-trip serveur pour distinguer "calme" de "mort" ──
// Renvoie true si la connexion répond, false si elle timeout/échoue.
async function probeAlive(sock, phone) {
  if (!sock) return false;
  try {
    const num = String(phone || '').replace(/[^0-9]/g, '') || '13135550002';
    const res = await Promise.race([
      sock.onWhatsApp(num),
      new Promise((_, rej) => setTimeout(() => rej(new Error('probe timeout')), 15_000)),
    ]);
    return Array.isArray(res);
  } catch {
    return false;
  }
}

// ── Watchdog : détecte les blocages d'initialisation ET les zombies ───────────
// Baileys n'a pas de "zombie Chrome" (pas de navigateur). Mais un socket peut
// rester coincé en INITIALIZING (sync qui ne finit jamais) OU rester CONNECTED
// sans plus rien recevoir (WhatsApp a cessé la livraison). On force une
// recréation propre dans les deux cas.
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

    // Détection ZOMBIE : CONNECTED mais flux entrant silencieux trop longtemps.
    if (data.status === 'CONNECTED' && !data.reconnecting) {
      const silentMs = Date.now() - (data.metrics.lastStreamAt || data.metrics.statusChangedAt);
      if (silentMs > ZOMBIE_HARD_MS) {
        // Backstop : même si la sonde répond, un silence aussi long en CONNECTED
        // sur ce compte = livraison cassée → reconnexion forcée.
        data.metrics.zombieKills += 1;
        data.metrics.lastError = { msg: `zombie: silence ${Math.round(silentMs/60000)}min en CONNECTED → reconnexion forcée`, at: Date.now() };
        console.warn(`[${name}] 🧟 Watchdog : silence ${Math.round(silentMs/60000)}min en CONNECTED → reconnexion forcée`);
        scheduleReconnect(name, `zombie silence ${Math.round(silentMs/60000)}min`);
      } else if (silentMs > ZOMBIE_PROBE_AFTER_MS) {
        // Silence modéré : on sonde activement. Si la connexion ne répond plus,
        // c'est un socket mort → reconnexion immédiate (sans attendre le backstop).
        const alive = await probeAlive(data.client, data.phone);
        if (!alive && data.status === 'CONNECTED' && !data.reconnecting) {
          data.metrics.zombieKills += 1;
          data.metrics.lastError = { msg: `zombie: sonde KO après ${Math.round(silentMs/60000)}min → reconnexion`, at: Date.now() };
          console.warn(`[${name}] 🧟 Watchdog : sonde KO après ${Math.round(silentMs/60000)}min → reconnexion forcée`);
          scheduleReconnect(name, `zombie probe failed ${Math.round(silentMs/60000)}min`);
        }
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

    // Reconnexion COINCÉE : la garde reconnecting est tenue jusqu'à 'open', mais
    // si le socket recréé reste bloqué en "connecting" (ni open ni close), la
    // garde ne se libère jamais → plus aucune reconnexion. On force le rattrapage
    // après 3 min (au-delà du connectTimeoutMs de Baileys).
    if (data.reconnecting && data.reconnectingSince) {
      const stuckMs = Date.now() - data.reconnectingSince;
      if (stuckMs > 180_000) {
        data.metrics.zombieKills += 1;
        data.reconnecting = false; // débloque la garde
        console.warn(`[${name}] 🧯 Watchdog : reconnexion coincée ${Math.round(stuckMs/1000)}s → relance forcée`);
        scheduleReconnect(name, `reconnexion coincée ${Math.round(stuckMs/1000)}s`);
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
  const { name, phone } = req.body;
  if (!name) return res.status(400).json({ error: 'name requis' });
  if (!sessions.has(name) && sessions.size >= MAX_SESSIONS) {
    return res.status(429).json({ error: `Plafond de ${MAX_SESSIONS} sessions atteint (anti-surcharge). Arrêtez-en une d'abord.` });
  }
  const s = createSession(name);
  if (phone) {
    s.phoneNumber = String(phone).replace(/[^0-9]/g, '');
  }
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
      // Demande manuelle explicite : on génère un code frais et on pose le garde
      // pour que les rotations de QR ne le régénèrent pas derrière (ce qui
      // l'invaliderait pendant que le client le saisit).
      const code = await data.client.requestPairingCode(normalizedPhone);
      data.pairingRequested = true;
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
    baileysVersion: BAILEYS_VERSION,
    uptime:   Math.floor((Date.now() - START_TIME) / 1000),
    sessions: sessions.size,
    maxSessions: MAX_SESSIONS,
    connected: [...sessions.values()].filter(s => s.status === 'CONNECTED').length,
  });
});

// ── Journal d'une session (temps réel) ──────────────────────────────────────────
app.get('/api/sessions/:name/logs', auth, (req, res) => {
  const name = req.params.name;
  const limit = Math.min(Number(req.query.limit) || 120, SESSION_LOG_MAX);
  const ring = sessionLogRing.get(name) || [];
  res.json({
    name,
    exists: sessions.has(name),
    logs: ring.slice(-limit),
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
  if (API_KEY === 'camille-core-secret') {
    console.warn('⚠️  SÉCURITÉ : API_KEY par défaut utilisée ! Définis la variable d\'env API_KEY en production (sinon n\'importe qui peut envoyer des messages depuis ton WhatsApp).');
  }
  autoStartSessions();
});

// Sécurité : ne jamais laisser une exception non gérée tuer le process
process.on('unhandledRejection', (reason) => {
  console.error('[unhandledRejection]', reason?.message || reason);
});
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err?.message || err);
});
