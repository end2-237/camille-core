# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# Utilise ghcr.io/puppeteer/puppeteer : Node 20 + Chromium pré-installé

FROM ghcr.io/puppeteer/puppeteer:22

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome-stable

USER root

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .
RUN mkdir -p sessions public/media

EXPOSE 3000
CMD ["node", "index.js"]
