# ── Camille Core Dockerfile (v2 — Baileys, sans Chrome) ──────────────────────
# Plus de Puppeteer/Chrome : image ~250 Mo au lieu de 1,5 Go, CPU quasi nul.
# node:20-slim + outils de build pour les deps natives (crypto signal).

FROM node:20-slim

# Outils nécessaires à node-gyp (libsignal) + certificats HTTPS
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .
RUN mkdir -p sessions public/media

EXPOSE 3000
CMD ["node", "index.js"]
