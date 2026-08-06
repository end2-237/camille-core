// ─────────────────────────────────────────────────────────────────────────────
// Camille Core — WhatsApp API Gateway (v2 — Baileys)
// Multi-sessions, envoi audio/vidéo/doc sans limite, QR + pairing code,
// reconnexion auto, watchdog, analytics. SANS Chrome/Puppeteer → ultra léger.
// API HTTP & dashboard 100% compatibles avec la v1 (n8n inchangé).
// ─────────────────────────────────────────────────────────────────────────────

'use strict';

// Baileys 7.x est un module ESM pur ("type":"module") → require() échoue sur
// Node < 22.12 (ERR_REQUIRE_ESM). On le charge donc via import() dynamique
// (compatible avec tous les Node en CommonJS) au démarrage, avant toute session.
let makeWASocket;
let useMultiFileAuthState;
let fetchLatestBaileysVersion;
let makeCacheableSignalKeyStore;
let DisconnectReason;
let Browsers;
let downloadMediaMessage;

async function loadBaileys() {
  const b = await import('@whiskeysockets/baileys');
  makeWASocket                = b.makeWASocket || b.default;
  useMultiFileAuthState       = b.useMultiFileAuthState;
  fetchLatestBaileysVersion   = b.fetchLatestBaileysVersion;
  makeCacheableSignalKeyStore = b.makeCacheableSignalKeyStore;
  DisconnectReason            = b.DisconnectReason;
  Browsers                    = b.Browsers;
  downloadMediaMessage        = b.downloadMediaMessage;
}

const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const QRCode   = require('qrcode');
const axios    = require('axios');
const path     = require('path');
const fs        = require('fs');
const crypto   = require('crypto');
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
const MAX_SESSIONS         = Number(process.env.MAX_SESSIONS)         || 5;

// ── Version du client WhatsApp Web annoncée au serveur ───────────────────────
//
// On appelait fetchLatestBaileysVersion() à chaque ouverture de socket. Cette
// fonction ne lit PAS la version de la bibliothèque installée : elle télécharge
// src/Defaults/index.ts sur la branche master de Baileys et en extrait le
// numéro. On annonçait donc à WhatsApp une version du protocole implémentée par
// master, avec une bibliothèque figée à la rc13 de mai.
//
// Conséquence : le jour où l'équipe Baileys avance master, la production change
// de comportement sans qu'une seule ligne n'ait bougé chez nous, et sans
// redéploiement. C'est intenable pour un service en production.
//
// Par défaut on annonce donc la version embarquée dans la bibliothèque qu'on a
// réellement installée (version: undefined => Baileys prend la sienne).
// WA_VERSION="2.3000.1043857760" force un numéro précis si WhatsApp finit par
// refuser celui-là, et WA_VERSION_SUIVRE_MASTER=1 restaure l'ancien comportement.
const WA_VERSION = String(process.env.WA_VERSION || '')
  .split('.').map((n) => Number(n.trim())).filter(Number.isFinite);
const WA_VERSION_SUIVRE_MASTER = process.env.WA_VERSION_SUIVRE_MASTER === '1';

// ── Fenêtre de conflit ───────────────────────────────────────────────────────
// Un « Stream Errored (conflict) » est presque toujours suivi, une à deux
// secondes plus tard, d'un 401 « Connection Failure » qui ne porte plus le mot
// conflict. C'est le même incident. Sans cette fenêtre, le second 401 est pris
// pour une authentification perdue.
const CONFLIT_FENETRE_MS = Number(process.env.CONFLIT_FENETRE_MS) || 120_000;

// Combien de 401 d'affilée, sans jamais rouvrir, avant d'admettre que le
// couplage est réellement mort et de repartir sur un QR.
//
// Ne pas effacer au premier 401 était la correction du 5 août ; ne jamais
// effacer serait l'excès inverse — la session tournerait indéfiniment en
// reconnexion sans jamais proposer de code à scanner. Cinq tentatives laissent
// passer un conflit ou une coupure réseau, et rendent la main en deux minutes
// si l'appareil a vraiment été retiré depuis le téléphone.
const AUTH_ECHECS_MAX = Number(process.env.AUTH_ECHECS_MAX) || 5;

// ── Alerte du vendeur quand une session tombe ────────────────────────────────
// Camille-core sait le premier qu'un agent est débranché, mais il ne le disait
// à personne : le vendeur découvrait la panne quand un client se plaignait.
//
// On ne signale pas la moindre coupure : Baileys se reconnecte seul en quelques
// secondes, et prévenir à chaque fois reviendrait à apprendre au vendeur à
// ignorer ses alertes. On attend donc que la panne dure — sauf pour une
// authentification perdue, qui ne se répare jamais toute seule.
// Valeur par defaut = la production, par symetrie avec camille qui pointe deja
// camille-core en dur. Une variable oubliee au deploiement eteindrait les
// alertes sans que personne ne s'en apercoive.
const CAMILLE_URL = (process.env.CAMILLE_URL || 'https://camille.vps.buyticle.com').replace(/\/$/, '');
const DISCONNECT_GRACE_MS = Number(process.env.DISCONNECT_GRACE_MS) || 3 * 60_000;
const pendingStateReports = new Map(); // name → timeout

async function postSessionState(name, status, reason, extra) {
  if (!CAMILLE_URL) return; // non configuré : on n'alerte pas, on ne casse rien
  try {
    await axios.post(
      `${CAMILLE_URL}/api/waha/session-event`,
      { session: name, status, reason: reason || '', ...(extra || {}) },
      { headers: { 'x-api-key': API_KEY }, timeout: 10_000 }
    );
  } catch (e) {
    debugLog(`[${name}] alerte état non transmise: ${e.message}`);
  }
}

/**
 * Signale l'état d'une session à camille, avec temporisation.
 * CONNECTED et AUTH_FAILURE partent tout de suite ; une simple déconnexion
 * n'est signalée que si elle dure encore après DISCONNECT_GRACE_MS.
 */
function reportSessionState(name, status, reason) {
  const pending = pendingStateReports.get(name);
  if (pending) { clearTimeout(pending); pendingStateReports.delete(name); }

  if (status === 'DISCONNECTED') {
    const t = setTimeout(() => {
      pendingStateReports.delete(name);
      // Toujours à terre ? Alors seulement c'est une nouvelle.
      if (sessions.get(name)?.status !== 'CONNECTED') {
        postSessionState(name, 'DISCONNECTED', reason);
      }
    }, DISCONNECT_GRACE_MS);
    // Ne pas retenir le process pour une alerte en attente.
    if (typeof t.unref === 'function') t.unref();
    pendingStateReports.set(name, t);
    return;
  }

  postSessionState(name, status, reason);
}

// Créer le dossier media au démarrage
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

// ── Métriques persistées (compteurs cumulés qui survivent aux redéploiements) ──
const METRICS_FILE = path.join(SESSIONS_DIR, 'metrics.json');
let savedMetrics = {};
try { if (fs.existsSync(METRICS_FILE)) savedMetrics = JSON.parse(fs.readFileSync(METRICS_FILE, 'utf8')) || {}; } catch { savedMetrics = {}; }
function saveMetrics() {
  try {
    const out = {};
    sessions.forEach((d, n) => {
      const m = d.metrics || {};
      out[n] = { messageCount: m.messageCount || 0, webhookErrors: m.webhookErrors || 0, mediaErrors: m.mediaErrors || 0,
                 emptyBodyCount: m.emptyBodyCount || 0, zombieKills: m.zombieKills || 0, createdAt: m.createdAt, lastMessageAt: m.lastMessageAt || null };
    });
    // conserve aussi les sessions non re-créées (au cas où)
    Object.keys(savedMetrics).forEach((n) => { if (!(n in out)) out[n] = savedMetrics[n]; });
    savedMetrics = out;
    fs.writeFileSync(METRICS_FILE + '.tmp', JSON.stringify(out));
    fs.renameSync(METRICS_FILE + '.tmp', METRICS_FILE);
  } catch (e) { /* best-effort */ }
}
// flush périodique + à l'arrêt (redéploiement Coolify envoie SIGTERM)
setInterval(saveMetrics, 20000);
process.on('SIGTERM', () => { saveMetrics(); process.exit(0); });
process.on('SIGINT', () => { saveMetrics(); process.exit(0); });

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

// ── Login du tableau de bord (email + mot de passe) ───────────────────────────
// N'affecte AUCUN appel API existant : les intégrations (n8n, Camille) continuent
// d'utiliser X-Api-Key. Ici on vérifie simplement le couple email/mot de passe,
// puis on renvoie la clé au dashboard pour qu'il fonctionne comme avant.
//   DASHBOARD_EMAIL : email autorisé (seul nouvel env à ajouter)
//   mot de passe    : API_KEY (déjà existante)
const DASHBOARD_EMAIL = process.env.DASHBOARD_EMAIL || 'admin@camille.local';

// Comparaison à temps constant (évite de révéler le secret par la durée de réponse)
function safeEqual(a, b) {
  const bufA = Buffer.from(String(a));
  const bufB = Buffer.from(String(b));
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

app.post('/auth/login', (req, res) => {
  const { email, password } = req.body || {};
  if (!email || !password) {
    return res.status(400).json({ error: 'Email et mot de passe requis.' });
  }
  const emailOk = String(email).trim().toLowerCase() === DASHBOARD_EMAIL.trim().toLowerCase();
  const passOk  = safeEqual(password, API_KEY);
  if (!emailOk || !passOk) {
    return res.status(401).json({ error: 'Identifiants invalides.' });
  }
  return res.json({ apiKey: API_KEY });
});

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

/**
 * Résout le VRAI numéro de téléphone derrière un JID.
 *
 * Depuis 2025 WhatsApp adresse les contacts par LID (`123456789@lid`) : ce
 * n'est PAS un numéro. Tel quel, un lien wa.me/<lid> ne mène nulle part et le
 * commerçant ne peut pas rappeler son client.
 *
 * Deux sources, dans l'ordre de fiabilité :
 *  1. `key.remoteJidAlt` / `key.participantAlt` — le JID téléphone fourni
 *     directement par WhatsApp à côté du LID.
 *  2. La table de correspondance de Baileys (`lidMapping.getPNForLID`).
 *
 * @returns {Promise<string|null>} JID au format `<numero>@s.whatsapp.net`
 */
async function resolvePhoneJid(sock, m) {
  const jid = m?.key?.remoteJid || '';
  if (!jid.endsWith('@lid')) return null; // deja un vrai numero

  const alt = m?.key?.remoteJidAlt || m?.key?.participantAlt || '';
  if (alt && alt.endsWith('@s.whatsapp.net')) return alt;

  try {
    const pn = await sock?.signalRepository?.lidMapping?.getPNForLID?.(jid);
    if (pn && String(pn).endsWith('@s.whatsapp.net')) return String(pn);
  } catch { /* correspondance inconnue : on gardera le LID */ }

  return null;
}

// Extrait la position d'un message Baileys (partage ponctuel ou position live).
// Baileys nomme les champs degreesLatitude / degreesLongitude ; sans cette
// extraction le webhook ne transmet aucune coordonnée (body vide + type seul).
function extractLocation(m) {
  const msg = m.message || {};
  const lm = msg.locationMessage || msg.liveLocationMessage;
  if (!lm) return null;
  const lat = Number(lm.degreesLatitude);
  const lng = Number(lm.degreesLongitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  if (lat === 0 && lng === 0) return null;
  if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return null;
  return {
    latitude:  lat,
    longitude: lng,
    name:      lm.name    || '',
    address:   lm.address || '',
    live:      !!msg.liveLocationMessage,
  };
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

/**
 * Message cité quand le client répond à un message précédent.
 *
 * Sans lui, un client qui répond à la photo d'un produit en écrivant « c'est
 * combien ? » envoyait trois mots sans aucun contexte : l'agent ne pouvait pas
 * savoir de quel produit on parlait, et redemandait. C'est une des frictions
 * les plus visibles, et l'information était là depuis le début — elle n'était
 * simplement jamais transmise.
 *
 * `text` reprend la légende de l'image ou le texte cité, c'est-à-dire ce que
 * l'agent avait lui-même écrit : de quoi retrouver le produit.
 */
function extractQuoted(m) {
  const msg = m.message || {};
  const ctx = msg.extendedTextMessage?.contextInfo
    || msg.imageMessage?.contextInfo
    || msg.videoMessage?.contextInfo
    || msg.stickerMessage?.contextInfo
    || msg.audioMessage?.contextInfo
    || msg.documentMessage?.contextInfo
    || null;

  const q = ctx?.quotedMessage;
  if (!q) return null;

  const text = q.conversation
    || q.extendedTextMessage?.text
    || q.imageMessage?.caption
    || q.videoMessage?.caption
    || q.documentMessage?.caption
    || '';

  return {
    id:      ctx.stanzaId || '',
    // fromMe côté cité : savoir si le client répond à NOTRE message ou au sien.
    fromMe:  Boolean(ctx.participant && m.key?.remoteJid && ctx.participant !== m.key.remoteJid) || false,
    type:    q.imageMessage ? 'image' : q.videoMessage ? 'video' : q.audioMessage ? 'audio' : 'chat',
    text:    String(text || '').slice(0, 1000),
  };
}

function msgType(m) {
  const msg = m.message || {};
  if (msg.conversation || msg.extendedTextMessage) return 'chat';
  if (msg.imageMessage)    return 'image';
  if (msg.videoMessage)    return 'video';
  if (msg.audioMessage)    return msg.audioMessage.ptt ? 'ptt' : 'audio';
  if (msg.documentMessage) return 'document';
  if (msg.stickerMessage)  return 'sticker';
  if (msg.locationMessage || msg.liveLocationMessage) return 'location';
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

// ── « Vous êtes où ? » → épingle de la boutique ───────────────────────────────
// Le client qui demande l'adresse recevait une phrase ; il lui restait à la
// recopier dans Maps. L'épingle WhatsApp ouvre l'itinéraire d'un tap.
//
// La détection est ici, sur le message entrant, et non dans le workflow : elle
// vaut donc à tous les niveaux d'agent et dans toutes les branches de
// conversation, y compris pendant une prise de commande. Le message part
// quand même vers n8n — le modèle répond son texte, l'épingle vient en plus.

// Un message peut demander l'adresse de la boutique de mille façons ; il ne
// doit pas la déclencher en demandant où en est SA commande.
//
// \b est inutilisable ici : JavaScript le calcule sur l'ASCII seul, donc il ne
// voit aucune frontière autour de « où » ou de « êtes » et le motif ne colle
// jamais. On délimite donc explicitement, accents compris.
const LTR = '[a-zà-öø-ÿ]';
const NB  = `(?<!${LTR})`;           // rien d'alphabétique avant
const NA  = `(?!${LTR})`;            // rien d'alphabétique après
const mot = (alt) => `${NB}(?:${alt})${NA}`;
const OU  = mot('o[uù]');
const rx  = (src) => new RegExp(src, 'i');

const LOCATION_INTENT = [
  // « où êtes-vous », « où est la boutique », « c'est où »
  rx(`${OU}[^?.!]{0,30}${mot("[êe]tes|est|es|se\\s+trouve|situ[ée]e?s?|localis[ée]e?s?|bas[ée]e?s?")}`),
  rx(`${mot("vous\\s+[êe]tes|tu\\s+es|c'?est|[çc]a\\s+se\\s+trouve|on\\s+vous\\s+trouve")}[^?.!]{0,20}${OU}`),
  rx(`${OU}[^?.!]{0,30}${mot("boutique|magasin|shop|local|bureau|si[èe]ge|atelier|agence|entrep[oô]t")}`),
  // demande directe
  rx(mot("localisation|g[ée]olocalisation|position\\s+exacte|coordonn[ée]es\\s+gps|itin[ée]raire|plan\\s+d'?acc[èe]s")),
  // « l'adresse » n'a pas d'espace après l'apostrophe : ce cas ne peut pas
  // passer par mot(), dont la frontière de fin interdit une lettre juste après.
  rx(`(?:${mot("votre|vot'?|ton|ta|la")}\\s+|${NB}l'\\s*)${mot('adresse')}`),
  rx(mot('google\\s*maps?|gps')),
  rx(`${mot('comment')}[^?.!]{0,30}${mot("venir|arriver|vous\\s+trouver|te\\s+trouver|acc[ée]der|rejoindre|passer")}`),
  // anglais
  rx(`${mot('where')}[^?.!]{0,30}${mot("are\\s+you|is\\s+(?:your|the)\\s+(?:shop|store|office|place)|can\\s+i\\s+find")}`),
  rx(`${mot('your')}\\s+${mot('address|location')}`),
  rx(mot("how\\s+(?:do|can)\\s+i\\s+(?:get|come|reach)")),
  rx(mot('directions?')),
  // pidgin
  rx(mot("wusai|which\\s+side|na\\s+where|una\\s+dey\\s+where")),
];

// Ce qui ressemble à la question sans en être une. « Où est ma commande » et
// « votre adresse mail » tombaient tous les deux dans les motifs ci-dessus.
const NOT_SHOP_LOCATION = rx([
  mot("ma|mon|mes|my|the") + "\\s+" + mot("commande|colis|livraison|paquet|order|parcel|package|delivery"),
  `${OU}\\s+${mot('en')}\\s+${mot('est')}`,
  mot("where\\s+is\\s+my"),
  `${mot('adresse')}\\s+${mot("mail|e-?mail|[ée]lectronique")}`,
  `${mot('adresse')}\\s+${mot('de')}\\s+${mot('livraison')}`,
  mot('livreur'),
].join('|'));

function wantsShopLocation(text) {
  const t = String(text || '').trim();
  if (t.length < 3 || t.length > 400) return false;
  if (NOT_SHOP_LOCATION.test(t)) return false;
  return LOCATION_INTENT.some((re) => re.test(t));
}

// Coordonnées de l'agent, lues chez camille (by-session les expose déjà).
// Cache court : le commerçant qui corrige la position de sa boutique n'a pas à
// attendre un redémarrage, et une rafale de messages n'interroge pas camille
// à chaque ligne.
const AGENT_GEO_TTL_MS = 5 * 60_000;
const agentGeoCache = new Map(); // session → { at, geo }

async function fetchAgentGeo(session) {
  const hit = agentGeoCache.get(session);
  if (hit && Date.now() - hit.at < AGENT_GEO_TTL_MS) return hit.geo;
  let geo = null;
  try {
    const r = await axios.get(`${CAMILLE_URL}/api/agents/by-session`, {
      params: { session },
      timeout: 8000,
    });
    const a = (r.data && r.data.agent) || {};
    if (a.latitude != null && a.longitude != null) {
      geo = {
        latitude:  Number(a.latitude),
        longitude: Number(a.longitude),
        name:      a.business_name || a.name || '',
        address:   a.location || '',
      };
    }
  } catch (e) {
    debugLog(`[${session}] géo agent non résolue: ${e.message}`);
  }
  // Un échec est mis en cache lui aussi, mais brièvement : sans ça, une panne
  // de camille ferait interroger le réseau à chaque message entrant.
  agentGeoCache.set(session, { at: geo ? Date.now() : Date.now() - AGENT_GEO_TTL_MS + 30_000, geo });
  return geo;
}

// Anti-répétition : dans un échange où l'on reparle du chemin plusieurs fois,
// renvoyer l'épingle à chaque phrase ressemble à un bug.
const LOCATION_COOLDOWN_MS = Number(process.env.LOCATION_COOLDOWN_MS) || 10 * 60_000;
// L'épingle est retardée pour arriver APRÈS la réponse écrite du modèle : elle
// illustre la phrase, elle ne la précède pas.
const LOCATION_DELAY_MS = Number(process.env.LOCATION_DELAY_MS) || 6000;
const lastLocationSent = new Map(); // `session|jid` → timestamp

async function maybeSendShopLocation(session, jid, body) {
  if (!wantsShopLocation(body)) return;

  const key = `${session}|${jid}`;
  const now = Date.now();
  if (now - (lastLocationSent.get(key) || 0) < LOCATION_COOLDOWN_MS) return;

  const geo = await fetchAgentGeo(session);
  if (!geo) return; // boutique sans coordonnées : le texte du modèle suffit

  // Réservé avant l'attente : deux messages rapprochés ne doivent pas produire
  // deux épingles pendant que la première patiente.
  lastLocationSent.set(key, now);
  if (lastLocationSent.size > 2000) {
    lastLocationSent.delete(lastLocationSent.keys().next().value);
  }

  try {
    await new Promise((r) => setTimeout(r, LOCATION_DELAY_MS));
    const s = getSession(session); // relit l'état : la session a pu tomber
    await s.client.sendMessage(toJid(jid), {
      location: {
        degreesLatitude:  geo.latitude,
        degreesLongitude: geo.longitude,
        name:             geo.name,
        address:          geo.address,
      },
    });
    debugLog(`[${session}] 📍 épingle boutique envoyée à ${jid}`);
  } catch (e) {
    // L'épingle est un bonus : son échec ne doit jamais peser sur la réponse.
    lastLocationSent.delete(key);
    debugLog(`[${session}] épingle boutique non envoyée: ${e.message}`);
  }
}

// ── Automatisation en panne : détecter, prévenir, et ne pas laisser le client
//    sans réponse ───────────────────────────────────────────────────────────────
//
// Quand le workflow ne répond plus (arrêté, 404, serveur tombé), WhatsApp reste
// connecté : rien ne signale la panne. Le client écrit et n'obtient rien, et le
// vendeur l'apprend quand quelqu'un se plaint — ou jamais.
//
// Deux réponses distinctes, et les deux comptent : prévenir le vendeur, et dire
// quelque chose au client. Un silence coûte la vente ; une phrase la garde en
// vie le temps qu'un humain reprenne la main.

const WEBHOOK_FAIL_SEUIL = Number(process.env.WEBHOOK_FAIL_SEUIL) || 3;
const FILET_COOLDOWN_MS = Number(process.env.FILET_COOLDOWN_MS) || 10 * 60_000;
const FILET_TEXTE = process.env.FILET_TEXTE
  || "Un instant 🙏 je transmets ton message au vendeur, il te répond tout de suite.";

const webhookFails = new Map();   // session → nombre d'échecs consécutifs
const webhookAlerte = new Map();  // session → true si le vendeur a déjà été prévenu
const filetEnvoye = new Map();    // `session|jid` → timestamp du dernier filet

/**
 * Message d'attente au client quand la réponse automatique n'a pas pu partir.
 *
 * Un par conversation et par fenêtre : pendant une panne longue, répéter la
 * même phrase à chaque message donnerait l'impression d'un second robot cassé.
 */
async function filetDeSecurite(session, jid) {
  const cle = `${session}|${jid}`;
  const now = Date.now();
  if (now - (filetEnvoye.get(cle) || 0) < FILET_COOLDOWN_MS) return;
  filetEnvoye.set(cle, now);
  if (filetEnvoye.size > 2000) filetEnvoye.delete(filetEnvoye.keys().next().value);
  try {
    const s = getSession(session);
    await s.client.sendMessage(toJid(jid), { text: FILET_TEXTE });
    slog(`[${session}] 🪢 filet de sécurité envoyé à ${jid}`);
    // Le vendeur doit savoir QUI attend, pas seulement que « ça ne marche
    // plus » : c'est le numéro qui lui permet de reprendre la vente à la main.
    // La cadence est celle du filet lui-même — un client, une alerte, par
    // fenêtre — donc une panne longue ne produit pas une avalanche.
    postSessionState(session, 'WEBHOOK_FALLBACK', '', {
      contact: String(jid || '').replace(/@(c\.us|lid|s\.whatsapp\.net)$/, ''),
    });
  } catch (e) {
    debugLog(`[${session}] filet de sécurité impossible: ${e.message}`);
  }
}

/** Compte les échecs consécutifs et prévient camille au franchissement du seuil. */
function noterEchecWebhook(session, raison) {
  const n = (webhookFails.get(session) || 0) + 1;
  webhookFails.set(session, n);
  if (n >= WEBHOOK_FAIL_SEUIL && !webhookAlerte.get(session)) {
    webhookAlerte.set(session, true);
    slog(`[${session}] ⚠ automatisation en panne (${n} échecs) — alerte au vendeur`);
    postSessionState(session, 'WEBHOOK_FAILING', raison);
  }
}

/** Un seul succès suffit à annoncer le retour à la normale. */
function noterSuccesWebhook(session) {
  webhookFails.set(session, 0);
  if (webhookAlerte.get(session)) {
    webhookAlerte.set(session, false);
    slog(`[${session}] ✓ automatisation rétablie`);
    postSessionState(session, 'WEBHOOK_OK', '');
  }
}

// ── Session Manager ───────────────────────────────────────────────────────────
//  Chaque "session" = 1 numéro WhatsApp = 1 socket Baileys
//  sessions Map : name → { name, status, client(sock), ... }

const sessions = new Map();

// ── « Cette session a-t-elle déjà fonctionné ? » ─────────────────────────────
//
// On se fiait à creds.registered pour décider si un 401 valait la peine
// d'effacer le couplage. Ce drapeau ment : après un couplage par code il reste
// à false alors que la session tourne — elle déchiffre, résout les LID et
// livre ses webhooks pendant des heures avec registered=false dans les logs.
//
// On garde donc notre propre preuve : un fichier posé à la première connexion
// réussie. Il vit dans le dossier d'auth, donc un reset manuel l'efface avec
// le reste — c'est voulu.
function marqueurOuvert(name) {
  return path.join(SESSIONS_DIR, `session-${name}`, '.camille-deja-ouvert');
}
function aDejaOuvert(name) {
  try {
    if (fs.existsSync(marqueurOuvert(name))) return true;
    // Filet pour les sessions couplées AVANT l'ajout du marqueur : Baileys
    // n'écrit `me` dans creds.json qu'une fois le compte réellement associé.
    // Sa présence prouve un couplage abouti, quoi que dise `registered`.
    const creds = path.join(SESSIONS_DIR, `session-${name}`, 'creds.json');
    if (!fs.existsSync(creds)) return false;
    const c = JSON.parse(fs.readFileSync(creds, 'utf8'));
    return !!(c && c.me && c.me.id);
  } catch { return false; }
}
function noterOuverture(name) {
  try { fs.writeFileSync(marqueurOuvert(name), new Date().toISOString()); } catch {}
}

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
  // Ré-hydrate les compteurs cumulés persistés (survivent aux redéploiements)
  const sm = savedMetrics[name];
  if (sm) {
    const m = data.metrics;
    m.messageCount   = sm.messageCount   || 0;
    m.webhookErrors  = sm.webhookErrors  || 0;
    m.mediaErrors    = sm.mediaErrors    || 0;
    m.emptyBodyCount = sm.emptyBodyCount || 0;
    m.zombieKills    = sm.zombieKills    || 0;
    m.createdAt      = sm.createdAt      || m.createdAt;
    m.lastMessageAt  = sm.lastMessageAt  || null;
  }
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

  // Voir le commentaire de WA_VERSION : par défaut on laisse Baileys annoncer
  // SA version, celle qu'il sait effectivement parler.
  let version = WA_VERSION.length === 3 ? WA_VERSION : undefined;
  if (!version && WA_VERSION_SUIVRE_MASTER) {
    try {
      ({ version } = await fetchLatestBaileysVersion());
    } catch (e) {
      console.warn(`[${name}] fetchLatestBaileysVersion KO (${e.message}) — version par défaut`);
    }
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
      slog(`OPEN registered=${sock.authState?.creds?.registered} wa=${(version || []).join('.') || 'défaut'}`);
      await saveCreds();
      // La connexion a abouti : à partir d'ici, un 401 isolé ne doit plus faire
      // effacer ce couplage. Et le compteur d'échecs repart de zéro — il ne
      // compte que les refus CONSÉCUTIFS, pas ceux d'un incident d'hier.
      noterOuverture(name);
      data.echecsAuth = 0;
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
      reportSessionState(name, 'CONNECTED');
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

      // Veille : une chute isolée ne dit rien, plusieurs chutes rapprochées sur
      // des comptes différents disent que le problème n'est chez aucun vendeur.
      noterChute(name, code);
      etatPlateforme()
        .then((etat) => annoncerIncidentPlateforme(etat))
        .catch(() => {});

      // Un "conflict"/"replaced" = deux connexions concurrentes (autre appareil
      // OU deuxième socket/container). Ce n'est PAS un logout : il ne faut SURTOUT
      // pas effacer les creds (sinon on perd le couplage et il faut re-scanner).
      const isConflict = /conflict|replaced/i.test(errMsg)
                      || code === DisconnectReason.connectionReplaced;
      // 515 = Baileys demande un redémarrage du flux, typiquement juste après le
      // couplage. C'est normal → reconnexion IMMÉDIATE pour finaliser la registration.
      const isRestart  = code === DisconnectReason.restartRequired;

      const isUnauthorized = code === 401 || code === DisconnectReason.loggedOut;

      // Un conflit ne se présente proprement qu'une fois. Le 401 qui suit une
      // seconde plus tard s'annonce « Connection Failure », sans le mot
      // conflict : c'est pourtant le même incident, et le prendre pour une
      // authentification perdue coûte le couplage.
      if (isConflict) data.dernierConflitAt = Date.now();
      const conflitRecent = data.dernierConflitAt
        && (Date.now() - data.dernierConflitAt) < CONFLIT_FENETRE_MS;

      // Cette session a-t-elle un couplage valide MAINTENANT ?
      //
      // La question porte sur le dossier d'authentification courant, et sur
      // rien d'autre. Le marqueur et le champ `me` de creds.json vivent tous
      // deux dedans, donc une réinitialisation les emporte — c'est ce qu'on
      // veut. (Le compteur de messages, lui, est persisté À CÔTÉ du dossier :
      // il survit à une réinitialisation et affirmait donc qu'une session
      // vidée « avait déjà fonctionné ». Résultat : on refusait d'effacer des
      // identifiants qui n'existaient plus, et la session tournait en
      // reconnexion sans jamais proposer de QR. Il n'entre plus dans le calcul.)
      const coupleMaintenant = aDejaOuvert(name);

      if (isUnauthorized) data.echecsAuth = (data.echecsAuth || 0) + 1;

      if (isUnauthorized && !isConflict && !conflitRecent
          && (!coupleMaintenant || data.echecsAuth >= AUTH_ECHECS_MAX)) {
        // Deux cas mènent ici, et un seul geste les règle : repartir sur un
        // couplage neuf.
        //   — aucun couplage en cours : les identifiants sont des brouillons ;
        //   — un couplage existait, mais AUTH_ECHECS_MAX tentatives d'affilée
        //     ont échoué sans jamais rouvrir. À ce stade ce n'est plus un
        //     incident passager, c'est un appareil retiré côté téléphone.
        const raison = coupleMaintenant
          ? `${data.echecsAuth} échecs d'authentification d'affilée`
          : 'couplage jamais abouti';
        setStatus('AUTH_FAILURE');
        data.echecsAuth = 0;
        data.metrics.lastError = { msg: `auth failure (${code}) — ${raison}, re-couplage`, at: Date.now() };
        io.emit('session:update', { name, status: data.status });
        reportSessionState(name, 'AUTH_FAILURE', `auth failure ${code}`);
        console.warn(`[${name}] ❌ Auth failure code=${code} — ${raison}, creds effacés, re-couplage`);
        try { fs.rmSync(authDir, { recursive: true, force: true }); } catch {}
        scheduleReconnect(name, `auth failure ${code} (${raison}) → nouveau couplage`, { immediate: true });
      } else if (isUnauthorized) {
        // Couplage valide : on reconnecte avec les MÊMES identifiants. C'est le
        // cas du conflit et de l'incident réseau, où effacer coûterait un
        // rescan pour rien. Le compteur ci-dessus borne l'obstination.
        const cause = conflitRecent ? 'conflit' : 'inconnue';
        setStatus('DISCONNECTED');
        data.metrics.lastError = { msg: `401 sur session couplée (${cause}) — tentative ${data.echecsAuth}/${AUTH_ECHECS_MAX}`, at: Date.now() };
        io.emit('session:update', { name, status: data.status });
        reportSessionState(name, 'DISCONNECTED', `401 sur session couplée (${cause})`);
        console.warn(`[${name}] ⚠️  401 sur session couplée (${cause}) — tentative ${data.echecsAuth}/${AUTH_ECHECS_MAX}, creds conservés`);
        scheduleReconnect(name, `401 sur session couplée (${cause})`);
      } else if (isRestart) {
        setStatus('DISCONNECTED');
        console.log(`[${name}] 🔁 Restart required (515) — reconnexion immédiate`);
        scheduleReconnect(name, 'restart required (515)', { immediate: true });
      } else {
        setStatus('DISCONNECTED');
        io.emit('session:update', { name, status: data.status });
        // Le restart (515) est routinier et se règle seul : on ne le signale pas.
        reportSessionState(name, 'DISCONNECTED', `close code ${code}`);
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
      // Message cité : le client qui répond à la photo d'un produit ne réécrit
      // pas son nom. Sans ce contexte, l'agent redemande de quoi on parle.
      const quoted = extractQuoted(m);
      if (quoted) slog(`↩ réponse à un message cité : "${quoted.text.slice(0, 40)}"`);
      // `from` reste l'adresse de conversation (c'est elle qui sert à répondre),
      // mais on expose en plus le vrai numéro pour que le commerçant puisse
      // rappeler son client depuis le dashboard ou l'app.
      const phoneJid = await resolvePhoneJid(sock, m);
      const from = toLegacyId(jid);
      const contactPhone = (phoneJid ? toLegacyId(phoneJid) : from).replace(/@(c\.us|lid|s\.whatsapp\.net)$/, '');
      if (phoneJid) slog(`☎ LID resolu: ${jid} -> ${contactPhone}`);
      const t    = msgType(m);
      const location = extractLocation(m);
      if (location) slog(`📍 position reçue: ${location.latitude},${location.longitude}${location.live ? ' (live)' : ''}`);

      // Image entrante : on la télécharge et on l'expose en URL publique pour n8n
      // (recherche par image côté camille). Best-effort : n'empêche jamais le webhook.
      let mediaUrl = '';
      if (t === 'image') {
        try {
          const buf = await downloadMediaMessage(m, 'buffer', {});
          const ext = (m.message.imageMessage?.mimetype || 'image/jpeg').split('/')[1].split(';')[0] || 'jpg';
          const fname = `in_${name}_${m.key.id}.${ext}`.replace(/[^a-zA-Z0-9._-]/g, '_');
          fs.writeFileSync(path.join(MEDIA_DIR, fname), buf);
          const baseUrl = (process.env.CORE_PUBLIC_URL || `http://localhost:${PORT}`).replace(/\/$/, '');
          mediaUrl = `${baseUrl}/media/${fname}`;
          slog(`📷 image reçue → ${mediaUrl}`);
        } catch (e) {
          slog(`✗ download image KO: ${e.message}`);
        }
      }

      data.metrics.lastMessageAt = Date.now();
      data.metrics.messageCount += 1;
      recordAnalytics(name, from);
      slog(`→ message accepté: from=${from} body="${body?.substring(0,50)}" type=${t}`);

      if ((!body || body.trim() === '') && t === 'chat') {
        data.metrics.emptyBodyCount += 1;
      }

      // Demande d'adresse → l'épingle part en parallèle du webhook. Volontairement
      // pas attendu : la réponse du modèle ne doit pas patienter derrière un
      // appel à camille ni derrière la temporisation de l'épingle.
      if (body) maybeSendShopLocation(name, jid, body).catch(() => {});

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
            mediaUrl,
            timestamp:  Number(m.messageTimestamp) || Math.floor(Date.now() / 1000),
            notifyName: m.pushName || '',
            // null pour tout message qui n'est pas un partage de position
            location,
            // Vrai numéro derrière un LID (= `from` nettoyé si déjà un numéro)
            contactPhone,
            // null quand le message ne cite rien
            quoted,
          },
        }, name)
        .then(() => {
          data.metrics.lastWebhookOkAt = Date.now();
          noterSuccesWebhook(name);
          slog(`✓ webhook OK (${from})`);
        })
        .catch((err) => {
          data.metrics.webhookErrors += 1;
          data.metrics.lastError = { msg: `webhook: ${err.message}`, at: Date.now() };
          slog(`✗ webhook ERROR (après retries): ${err.message}`);
          // Le client d'abord : il attend une réponse, pas une explication.
          filetDeSecurite(name, jid).catch(() => {});
          noterEchecWebhook(name, err.message);
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

// ═════════════════════════════════════════════════════════════════════════════
//  VEILLE PLATEFORME
//
//  WhatsApp change son protocole sans prévenir personne, et une bibliothèque
//  non officielle met quelques jours à suivre. Entre les deux, le vendeur voit
//  son agent tomber et ne comprend pas pourquoi — il croit que c'est son
//  téléphone, ou nous.
//
//  Ce module répond à trois questions, en continu :
//    1. Est-ce que ce qu'on ANNONCE à WhatsApp correspond à ce qu'on SAIT
//       parler ? (c'est ce décalage qui a causé la panne du 5 août)
//    2. Est-ce qu'une version plus récente de la bibliothèque existe ?
//       C'est le signal avancé : elle sort parce que WhatsApp a bougé.
//    3. Est-ce que plusieurs sessions tombent EN MÊME TEMPS ? Une session qui
//       tombe est un incident local ; trois qui tombent dans le même quart
//       d'heure, c'est la plateforme.
// ═════════════════════════════════════════════════════════════════════════════

const VEILLE_TTL_MS = Number(process.env.VEILLE_TTL_MS) || 6 * 3600_000;
// Fenêtre de corrélation : deux sessions distinctes qui tombent dedans font
// un incident de plateforme. En dessous de 15 min on rate les vagues lentes,
// au-dessus on rattache des pannes sans rapport.
const INCIDENT_FENETRE_MS = Number(process.env.INCIDENT_FENETRE_MS) || 15 * 60_000;
const INCIDENT_MIN_SESSIONS = Number(process.env.INCIDENT_MIN_SESSIONS) || 2;
// Une fois l'incident annoncé, on se tait pendant deux heures : une vague de
// déconnexions ne doit pas produire une vague de notifications.
const INCIDENT_SILENCE_MS = Number(process.env.INCIDENT_SILENCE_MS) || 2 * 3600_000;

const veille = {
  a: 0,                    // date du dernier relevé
  bibliothequeDerniere: null,   // dernière version publiée sur npm
  waMaster: null,               // version annoncée par la branche master
  erreur: null,
  signalee: null,               // dernière version déjà annoncée aux administrateurs
};

/**
 * Prévient les administrateurs qu'une version est parue.
 *
 * C'est le signal AVANCÉ : une release de Baileys paraît parce que WhatsApp a
 * changé quelque chose, quelques jours avant que les déconnexions ne
 * commencent. L'annoncer permet de programmer la montée plutôt que de la subir.
 *
 * Une seule annonce par version : la veille tourne toutes les six heures, et
 * répéter la même nouvelle quatre fois par jour la rend invisible.
 */
async function annoncerVersionParue(nouvelle, installee) {
  if (!CAMILLE_URL || !nouvelle || nouvelle === installee) return;
  if (veille.signalee === nouvelle) return;
  veille.signalee = nouvelle;
  try {
    await axios.post(
      `${CAMILLE_URL}/api/waha/platform-alert`,
      {
        niveau: 'attention',
        version: nouvelle,
        diagnostic: `Baileys ${nouvelle} est publiée (tu es en ${installee}).`,
        prevision: 'Une version paraît généralement parce que WhatsApp a changé son protocole. '
          + 'Programme la montée maintenant : après, ce sont des déconnexions en pleine journée.',
      },
      { headers: { 'x-api-key': API_KEY }, timeout: 10_000 }
    );
    console.warn(`[plateforme] 📣 version ${nouvelle} signalée aux administrateurs`);
  } catch (e) {
    debugLog(`[plateforme] annonce de version non transmise: ${e.message}`);
  }
}

// Chutes récentes, pour la corrélation. On ne garde que la fenêtre utile.
const chutes = [];
let dernierIncidentAt = 0;

function noterChute(name, code) {
  const now = Date.now();
  chutes.push({ name, code: Number(code) || 0, at: now });
  while (chutes.length && now - chutes[0].at > INCIDENT_FENETRE_MS) chutes.shift();
}

/** Version du client WhatsApp Web embarquée dans la bibliothèque installée. */
function versionWaBibliotheque() {
  // Baileys 7 est un module ESM avec une carte d'exports : require.resolve sur
  // un sous-chemin peut échouer selon la version de Node. On garde donc un
  // chemin en dur en secours — une veille qui s'aveugle sur un détail de
  // résolution de modules ne vaut rien.
  const candidats = [];
  try { candidats.push(path.dirname(require.resolve('@whiskeysockets/baileys/package.json'))); } catch {}
  candidats.push(path.join(__dirname, 'node_modules', '@whiskeysockets', 'baileys'));
  for (const base of candidats) {
    try {
      const src = fs.readFileSync(path.join(base, 'lib', 'Defaults', 'index.js'), 'utf8');
      const m = src.match(/const version = \[(\d+),\s*(\d+),\s*(\d+)\]/);
      if (m) return [Number(m[1]), Number(m[2]), Number(m[3])];
    } catch {}
  }
  return null;
}

async function rafraichirVeille() {
  if (Date.now() - veille.a < VEILLE_TTL_MS) return;
  veille.a = Date.now();
  veille.erreur = null;
  try {
    const [npmRes, masterRes] = await Promise.allSettled([
      axios.get('https://registry.npmjs.org/@whiskeysockets/baileys', { timeout: 15_000 }),
      axios.get('https://raw.githubusercontent.com/WhiskeySockets/Baileys/master/src/Defaults/index.ts',
        { timeout: 15_000, responseType: 'text' }),
    ]);
    if (npmRes.status === 'fulfilled') {
      veille.bibliothequeDerniere = npmRes.value.data?.['dist-tags']?.latest || null;
      // Au premier relevé après un démarrage, `signalee` est vide : si on est
      // déjà à jour, on note la version courante comme « déjà annoncée » pour
      // ne pas repartir de zéro au prochain redéploiement.
      if (veille.bibliothequeDerniere === BAILEYS_VERSION) veille.signalee = BAILEYS_VERSION;
      else await annoncerVersionParue(veille.bibliothequeDerniere, BAILEYS_VERSION);
    }
    if (masterRes.status === 'fulfilled') {
      const m = String(masterRes.value.data).match(/const version = \[(\d+),\s*(\d+),\s*(\d+)\]/);
      veille.waMaster = m ? [Number(m[1]), Number(m[2]), Number(m[3])].join('.') : null;
    }
  } catch (e) {
    veille.erreur = e.message;
  }
}

/**
 * L'état de la plateforme, en une structure lisible par un humain pressé.
 *
 * `prevision` est volontairement une phrase et pas un score : ce qu'un
 * exploitant veut savoir à 22 h, c'est « qu'est-ce qui va me tomber dessus »,
 * pas « 0.62 ».
 */
async function etatPlateforme() {
  await rafraichirVeille().catch(() => {});

  const waBib = versionWaBibliotheque();
  const annoncee = WA_VERSION.length === 3
    ? WA_VERSION.join('.')
    : (WA_VERSION_SUIVRE_MASTER ? (veille.waMaster || 'master (variable)') : (waBib ? waBib.join('.') : 'inconnue'));

  const bibliotheque = BAILEYS_VERSION;
  const derniere = veille.bibliothequeDerniere;
  const enRetard = !!(derniere && bibliotheque && derniere !== bibliotheque);

  // Décalage : on annonce une version que la bibliothèque installée
  // n'implémente pas. C'est la faute qui a coûté la nuit du 5 août.
  const waBibStr = waBib ? waBib.join('.') : null;
  const decalage = !!(waBibStr && annoncee !== 'inconnue' && annoncee !== waBibStr);

  // Incident en cours : sessions distinctes tombées dans la fenêtre.
  const now = Date.now();
  const recentes = chutes.filter((c) => now - c.at <= INCIDENT_FENETRE_MS);
  const touchees = [...new Set(recentes.map((c) => c.name))];
  const incident = touchees.length >= INCIDENT_MIN_SESSIONS;

  const enLigne = [...sessions.values()].filter((s) => s.status === 'CONNECTED').length;

  let niveau = 'ok';
  let diagnostic = 'Rien à signaler côté WhatsApp.';
  let prevision = 'Aucune action attendue.';

  if (enRetard) {
    niveau = 'attention';
    diagnostic = `Une version plus récente de la bibliothèque WhatsApp existe (${derniere}, tu es en ${bibliotheque}).`;
    prevision = 'Une nouvelle version paraît généralement parce que WhatsApp a changé quelque chose. '
      + 'Tant qu\'elle n\'est pas installée, des déconnexions peuvent apparaître sans prévenir.';
  }
  if (decalage) {
    niveau = 'critique';
    diagnostic = `On annonce à WhatsApp la version ${annoncee} alors que la bibliothèque installée parle ${waBibStr}.`;
    prevision = 'Ce décalage provoque des fermetures de flux à répétition. '
      + 'Aligner les deux (ou retirer WA_VERSION) est la seule correction durable.';
  }
  if (incident) {
    niveau = 'critique';
    diagnostic = `${touchees.length} sessions sont tombées en moins de ${Math.round(INCIDENT_FENETRE_MS / 60000)} minutes.`;
    prevision = 'Plusieurs comptes touchés en même temps : la cause est côté WhatsApp, pas côté vendeur. '
      + 'Les reconnexions automatiques suffisent le plus souvent ; sinon, une montée de version sera nécessaire.';
  }

  return {
    niveau, diagnostic, prevision,
    bibliotheque: { installee: bibliotheque, derniere, en_retard: enRetard },
    whatsapp:     { annoncee, embarquee: waBibStr, master: veille.waMaster, decalage },
    sessions:     { total: sessions.size, en_ligne: enLigne },
    incident: {
      en_cours: incident,
      fenetre_min: Math.round(INCIDENT_FENETRE_MS / 60000),
      sessions_touchees: touchees,
      chutes: recentes.map((c) => ({ session: c.name, code: c.code, at: c.at })),
    },
    veille: { dernier_releve: veille.a || null, erreur: veille.erreur },
  };
}

/**
 * Prévient Camille qu'un incident touche PLUSIEURS comptes.
 *
 * Un vendeur dont l'agent tombe pense d'abord à son téléphone, puis à nous.
 * Lui dire « ce n'est pas toi, c'est WhatsApp, et on le sait déjà » vaut mieux
 * qu'un silence pendant qu'on répare — c'est la différence entre une panne et
 * une panne qui fait perdre un client.
 */
async function annoncerIncidentPlateforme(etat) {
  if (!etat.incident.en_cours) return;
  if (Date.now() - dernierIncidentAt < INCIDENT_SILENCE_MS) return;
  dernierIncidentAt = Date.now();
  console.warn(`[plateforme] ⚠️  incident : ${etat.diagnostic}`);
  for (const name of etat.incident.sessions_touchees) {
    await postSessionState(name, 'PLATFORM_INCIDENT', etat.diagnostic, {
      portee: 'plateforme',
      prevision: etat.prevision,
      sessions_touchees: etat.incident.sessions_touchees.length,
    });
  }
}

app.get('/api/platform', auth, async (_req, res) => {
  try {
    res.json(await etatPlateforme());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

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
  // Légende acceptée à 3 endroits : caption (racine), file.caption, ou text —
  // selon d'où vient l'appel (n8n, WAHA-compat, curl).
  caption = caption || file.caption || req.body.text || '';

  try {
    const s   = getSession(session);
    const jid = toJid(chatId);
    console.log('[sendVideo] jid:', jid, 'url:', file.url, caption ? `caption:"${String(caption).slice(0,60)}"` : '(sans légende)');

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

// POST /api/sendImage  { chatId, session, file: { url }, caption }
// Envoie une VRAIE image inline (aperçu WhatsApp) avec légende — pour le catalogue N2.
app.post('/api/sendImage', auth, async (req, res) => {
  let { chatId, session = 'default', file, caption = '' } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId requis' });
  if (!file?.url) return res.json({ success: true, skipped: true, reason: 'Aucune URL image configurée' });
  caption = caption || file.caption || req.body.text || '';

  try {
    const s   = getSession(session);
    const jid = toJid(chatId);
    console.log('[sendImage] jid:', jid, 'url:', file.url, caption ? `caption:"${String(caption).slice(0,60)}"` : '(sans légende)');

    const buffer = await fetchMediaBuffer(file.url);
    if (!buffer || buffer.length < 100) {
      throw new Error(`Image invalide ou introuvable (${buffer ? buffer.length : 0} bytes)`);
    }

    await randomDelay(500, 1500);
    await s.client.sendMessage(jid, { image: buffer, caption: caption || undefined });
    console.log('[sendImage] envoyé ✓');

    res.json({ success: true });
  } catch (err) {
    const msg = err?.message || String(err);
    const sd = sessions.get(session);
    if (sd?.metrics) { sd.metrics.mediaErrors += 1; sd.metrics.lastError = { msg: `sendImage: ${msg}`, at: Date.now() }; }
    console.error('[sendImage] ERREUR FINALE:', msg);
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

// ══════════════════════════════════════════════════════════════════════════════
// OUTILS CONVERSATIONNELS WHATSAPP — expérience riche (N1/N2/N3)
// Construit une clé de message à partir de chatId + messageId (pour react/reply/…)
// ══════════════════════════════════════════════════════════════════════════════
function msgKey(chatId, messageId, fromMe) {
  return { remoteJid: toJid(chatId), id: messageId, fromMe: !!fromMe };
}
function wrap(res, session, label, fn) {
  return fn().then((r) => res.json(r || { success: true })).catch((err) => {
    const msg = err?.message || String(err);
    const sd = sessions.get(session);
    if (sd?.metrics) { sd.metrics.lastError = { msg: `${label}: ${msg}`, at: Date.now() }; }
    console.error(`[${label}]`, msg);
    res.status(500).json({ success: false, error: msg });
  });
}

// POST /api/sendAlbum  { chatId, session, items:[{url,caption}] }
// Envoie plusieurs images d'affilée — WhatsApp les regroupe en album (grille).
app.post('/api/sendAlbum', auth, (req, res) => {
  const { chatId, session = 'default', items } = req.body;
  if (!chatId || !Array.isArray(items) || !items.length) {
    return res.status(400).json({ error: 'chatId et items[] requis' });
  }
  wrap(res, session, 'sendAlbum', async () => {
    const s = getSession(session);
    const jid = toJid(chatId);

    // Télécharge d'abord les buffers valides
    const bufs = [];
    for (const it of items.slice(0, 6)) {
      if (!it || !it.url) continue;
      try {
        const buffer = await fetchMediaBuffer(it.url);
        if (buffer && buffer.length > 100) bufs.push({ buffer, caption: it.caption });
      } catch (e) { console.error('[sendAlbum] fetch KO:', e.message); }
    }
    if (!bufs.length) return { success: true, sent: 0 };

    // 1) Message album « parent » (annonce le nombre d'images attendues)
    const parent = await s.client.sendMessage(jid, {
      album: { expectedImageCount: bufs.length, expectedVideoCount: 0 },
    });

    // 2) Chaque image associée à l'album → WhatsApp affiche une vraie grille
    let sent = 0;
    for (const b of bufs) {
      await s.client.sendMessage(jid, {
        image: b.buffer,
        caption: b.caption || undefined,
        albumParentKey: parent.key,
      });
      sent++;
      await randomDelay(120, 300);
    }
    return { success: true, sent };
  });
});

// POST /api/sendLocation  { chatId, session, latitude, longitude, name, address }
app.post('/api/sendLocation', auth, (req, res) => {
  const { chatId, session = 'default', latitude, longitude, name = '', address = '' } = req.body;
  if (!chatId || latitude == null || longitude == null) return res.status(400).json({ error: 'chatId, latitude, longitude requis' });
  wrap(res, session, 'sendLocation', async () => {
    const s = getSession(session);
    await s.client.sendMessage(toJid(chatId), {
      location: { degreesLatitude: Number(latitude), degreesLongitude: Number(longitude), name, address },
    });
    return { success: true };
  });
});

// POST /api/sendPoll  { chatId, session, name, options:[], selectableCount }
app.post('/api/sendPoll', auth, (req, res) => {
  const { chatId, session = 'default', name, options, selectableCount = 1 } = req.body;
  if (!chatId || !name || !Array.isArray(options) || options.length < 2) {
    return res.status(400).json({ error: 'chatId, name et options[>=2] requis' });
  }
  wrap(res, session, 'sendPoll', async () => {
    const s = getSession(session);
    await s.client.sendMessage(toJid(chatId), {
      poll: { name, values: options.map(String), selectableCount: Math.max(1, Number(selectableCount) || 1) },
    });
    return { success: true };
  });
});

// POST /api/sendContact  { chatId, session, contactName, phone }
app.post('/api/sendContact', auth, (req, res) => {
  const { chatId, session = 'default', contactName, phone } = req.body;
  if (!chatId || !contactName || !phone) return res.status(400).json({ error: 'chatId, contactName, phone requis' });
  wrap(res, session, 'sendContact', async () => {
    const s = getSession(session);
    const num = String(phone).replace(/[^0-9]/g, '');
    const vcard = 'BEGIN:VCARD\nVERSION:3.0\nFN:' + contactName + '\nTEL;type=CELL;waid=' + num + ':+' + num + '\nEND:VCARD';
    await s.client.sendMessage(toJid(chatId), { contacts: { displayName: contactName, contacts: [{ vcard }] } });
    return { success: true };
  });
});

// POST /api/sendReaction  { chatId, session, messageId, emoji, fromMe }
app.post('/api/sendReaction', auth, (req, res) => {
  const { chatId, session = 'default', messageId, emoji = '👍', fromMe = false } = req.body;
  if (!chatId || !messageId) return res.status(400).json({ error: 'chatId, messageId requis' });
  wrap(res, session, 'sendReaction', async () => {
    const s = getSession(session);
    await s.client.sendMessage(toJid(chatId), { react: { text: emoji, key: msgKey(chatId, messageId, fromMe) } });
    return { success: true };
  });
});

// POST /api/sendReply  { chatId, session, text, messageId, quotedText, fromMe }
app.post('/api/sendReply', auth, (req, res) => {
  const { chatId, session = 'default', text, messageId, quotedText = '', fromMe = false } = req.body;
  if (!chatId || !text || !messageId) return res.status(400).json({ error: 'chatId, text, messageId requis' });
  wrap(res, session, 'sendReply', async () => {
    const s = getSession(session);
    const quoted = { key: msgKey(chatId, messageId, fromMe), message: { conversation: quotedText || '' } };
    await s.client.sendMessage(toJid(chatId), { text }, { quoted });
    return { success: true };
  });
});

// POST /api/sendSticker  { chatId, session, file:{url} }
app.post('/api/sendSticker', auth, (req, res) => {
  const { chatId, session = 'default', file } = req.body;
  if (!chatId || !file?.url) return res.status(400).json({ error: 'chatId et file.url requis' });
  wrap(res, session, 'sendSticker', async () => {
    const s = getSession(session);
    const buffer = await fetchMediaBuffer(file.url);
    await randomDelay(400, 1200);
    await s.client.sendMessage(toJid(chatId), { sticker: buffer });
    return { success: true };
  });
});

// POST /api/sendGif  { chatId, session, file:{url}, caption }
app.post('/api/sendGif', auth, (req, res) => {
  let { chatId, session = 'default', file, caption = '' } = req.body;
  if (!chatId || !file?.url) return res.status(400).json({ error: 'chatId et file.url requis' });
  caption = caption || file.caption || '';
  wrap(res, session, 'sendGif', async () => {
    const s = getSession(session);
    const buffer = await fetchMediaBuffer(file.url);
    await randomDelay(400, 1200);
    await s.client.sendMessage(toJid(chatId), { video: buffer, gifPlayback: true, caption: caption || undefined });
    return { success: true };
  });
});

// POST /api/setPresence  { chatId, session, presence }  (available|unavailable|composing|recording|paused)
app.post('/api/setPresence', auth, (req, res) => {
  const { chatId, session = 'default', presence = 'available' } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId requis' });
  wrap(res, session, 'setPresence', async () => {
    const s = getSession(session);
    await s.client.sendPresenceUpdate(presence, toJid(chatId));
    return { success: true };
  });
});

// POST /api/markRead  { chatId, session, messageId, fromMe }
app.post('/api/markRead', auth, (req, res) => {
  const { chatId, session = 'default', messageId, fromMe = false } = req.body;
  if (!chatId || !messageId) return res.status(400).json({ error: 'chatId, messageId requis' });
  wrap(res, session, 'markRead', async () => {
    const s = getSession(session);
    await s.client.readMessages([msgKey(chatId, messageId, fromMe)]);
    return { success: true };
  });
});

// POST /api/editMessage  { chatId, session, messageId, text, fromMe }
app.post('/api/editMessage', auth, (req, res) => {
  const { chatId, session = 'default', messageId, text, fromMe = true } = req.body;
  if (!chatId || !messageId || !text) return res.status(400).json({ error: 'chatId, messageId, text requis' });
  wrap(res, session, 'editMessage', async () => {
    const s = getSession(session);
    await s.client.sendMessage(toJid(chatId), { text, edit: msgKey(chatId, messageId, fromMe) });
    return { success: true };
  });
});

// POST /api/deleteMessage  { chatId, session, messageId, fromMe }
app.post('/api/deleteMessage', auth, (req, res) => {
  const { chatId, session = 'default', messageId, fromMe = true } = req.body;
  if (!chatId || !messageId) return res.status(400).json({ error: 'chatId, messageId requis' });
  wrap(res, session, 'deleteMessage', async () => {
    const s = getSession(session);
    await s.client.sendMessage(toJid(chatId), { delete: msgKey(chatId, messageId, fromMe) });
    return { success: true };
  });
});

// POST /api/sendList  { chatId, session, text, footer, title, buttonText, sections:[{title, rows:[{title,description,rowId}]}] }
// ⚠️ BEST-EFFORT : les listes interactives sont restreintes sur WhatsApp non-officiel.
// La sélection revient comme un message normal (selectedRowId) → déclenche la suite.
app.post('/api/sendList', auth, (req, res) => {
  const { chatId, session = 'default', text, footer = '', title = '', buttonText = 'Voir', sections } = req.body;
  if (!chatId || !text || !Array.isArray(sections) || !sections.length) {
    return res.status(400).json({ error: 'chatId, text, sections[] requis' });
  }
  wrap(res, session, 'sendList', async () => {
    const s = getSession(session);
    const clean = sections.map((sec) => ({
      title: sec.title || '',
      rows: (sec.rows || []).slice(0, 10).map((r, i) => ({
        title: String(r.title || ('Option ' + (i + 1))).slice(0, 24),
        description: r.description ? String(r.description).slice(0, 72) : undefined,
        rowId: r.rowId || r.title || ('row_' + (i + 1)),
      })),
    }));
    await s.client.sendMessage(toJid(chatId), {
      text, footer: footer || undefined, title: title || undefined, buttonText, sections: clean,
    });
    return { success: true, warning: 'best-effort — la liste peut ne pas s\'afficher (WhatsApp non-officiel)' };
  });
});

// POST /api/sendButtons  { chatId, session, text, footer, buttons:[{id,title}] }
// ⚠️ BEST-EFFORT : WhatsApp restreint les boutons à l'API officielle. Peut ne pas
// s'afficher selon l'appareil/version. Alternative fiable : sendPoll.
app.post('/api/sendButtons', auth, (req, res) => {
  const { chatId, session = 'default', text, footer = '', buttons } = req.body;
  if (!chatId || !text || !Array.isArray(buttons) || !buttons.length) {
    return res.status(400).json({ error: 'chatId, text, buttons[] requis' });
  }
  wrap(res, session, 'sendButtons', async () => {
    const s = getSession(session);
    const templateButtons = buttons.slice(0, 3).map((b, i) => ({
      index: i + 1,
      quickReplyButton: { displayText: b.title || ('Option ' + (i + 1)), id: b.id || ('btn_' + (i + 1)) },
    }));
    await s.client.sendMessage(toJid(chatId), { text, footer: footer || undefined, templateButtons });
    return { success: true, warning: 'best-effort — les boutons peuvent ne pas s\'afficher (WhatsApp non-officiel)' };
  });
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
    // URL publique : env explicite > host de la requête (proxy Render/Coolify) > localhost.
    const fwdProto = (req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
    const fwdHost  = (req.headers['x-forwarded-host']  || req.headers.host || '').split(',')[0].trim();
    const fromReq  = fwdHost ? `${fwdProto || 'https'}://${fwdHost}` : '';
    const baseUrl  = (process.env.CORE_PUBLIC_URL || fromReq || `http://localhost:${PORT}`).replace(/\/$/, '');
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

// On charge Baileys (ESM) AVANT de démarrer le serveur et les sessions.
(async () => {
  try {
    await loadBaileys();
  } catch (e) {
    console.error('[FATAL] Chargement de Baileys échoué:', e.message);
    process.exit(1);
  }
  server.listen(PORT, () => {
    console.log(`\n🔥 Camille Core v${VERSION} (Baileys ${BAILEYS_VERSION}) → http://localhost:${PORT}`);
    console.log(`🔑 API Key      → ${API_KEY}`);
    console.log(`📡 Webhook n8n  → ${N8N_WEBHOOK || '(non configuré)'}`);
    console.log(`🧩 Max sessions → ${MAX_SESSIONS}\n`);
    if (API_KEY === 'camille-core-secret') {
      console.warn('⚠️  SÉCURITÉ : API_KEY par défaut utilisée ! Définis la variable d\'env API_KEY en production (sinon n\'importe qui peut envoyer des messages depuis ton WhatsApp).');
    }
    autoStartSessions();

    // ── Veille plateforme ───────────────────────────────────────────────────
    // Elle doit tourner d'elle-même. Ne la rafraîchir qu'à l'ouverture de la
    // console ou après une déconnexion la réduirait à un constat : le signal
    // avancé n'a de valeur que s'il arrive AVANT que quelque chose casse.
    //
    // Premier relevé une minute après le démarrage — le temps que les sessions
    // se connectent, pour ne pas confondre un démarrage avec un incident.
    const premier = setTimeout(() => { rafraichirVeille().catch(() => {}); }, 60_000);
    if (typeof premier.unref === 'function') premier.unref();

    const boucle = setInterval(() => { rafraichirVeille().catch(() => {}); }, VEILLE_TTL_MS);
    if (typeof boucle.unref === 'function') boucle.unref();
  });
})();

// Sécurité : ne jamais laisser une exception non gérée tuer le process
process.on('unhandledRejection', (reason) => {
  console.error('[unhandledRejection]', reason?.message || reason);
});
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err?.message || err);
});
