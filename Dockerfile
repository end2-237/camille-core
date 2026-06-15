# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# ghcr.io/puppeteer/puppeteer:22 = Node 20 + Google Chrome stable pré-installé
# Les zombies Chrome sont récoltés par l'init de Docker (init: true dans docker-compose)

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
