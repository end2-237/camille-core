# ── Camille Core Dockerfile (v2 — Baileys, sans Chrome) ──────────────────────
# Plus de Puppeteer/Chrome : image ~200 Mo au lieu de 1,5 Go, CPU quasi nul.
# Baileys = crypto 100% JS → AUCUN outil de build natif requis (install ~12s).

FROM node:20-slim

# Seuls les certificats HTTPS sont nécessaires (axios → n8n, médias, version WA)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev --prefer-offline --no-audit --no-fund

COPY . .
RUN mkdir -p sessions public/media

# ⚠️ PERSISTANCE — ces dossiers DOIVENT être montés sur un volume persistant,
# sinon les sessions WhatsApp (auth Baileys) et les médias sont perdus à chaque
# redéploiement (le conteneur est recréé avec une image neuve).
#   • /app/sessions     → creds WhatsApp + webhooks.json + analytics
#   • /app/public/media → images produits uploadées
# Coolify : Storage → monter un volume vers /app/sessions (et /app/public/media).
# Docker  : -v camille_sessions:/app/sessions -v camille_media:/app/public/media
VOLUME ["/app/sessions", "/app/public/media"]

EXPOSE 3000
CMD ["node", "index.js"]
