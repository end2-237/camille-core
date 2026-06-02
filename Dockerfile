# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# Utilise ghcr.io/puppeteer/puppeteer : Node 20 + Chromium pré-installé
# Evite l'installation apt de Chromium (~2-3 min) à chaque build

FROM ghcr.io/puppeteer/puppeteer:22

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

WORKDIR /app

# npm install en premier pour profiter du layer cache Docker
COPY package*.json ./
RUN npm install --production

COPY . .
RUN mkdir -p sessions

EXPOSE 3000
CMD ["node", "index.js"]
