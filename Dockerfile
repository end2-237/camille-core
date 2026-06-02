# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# Utilise ghcr.io/puppeteer/puppeteer : Node 20 + Chromium pré-installé

FROM ghcr.io/puppeteer/puppeteer:22

# Ne pas télécharger Chromium via npm (déjà dans l'image)
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

# Lancer en root pour accès aux volumes montés depuis l'hôte (sessions/)
USER root

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .
RUN mkdir -p sessions public/media

EXPOSE 3000
CMD ["node", "index.js"]
