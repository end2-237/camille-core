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

EXPOSE 3000
CMD ["node", "index.js"]
