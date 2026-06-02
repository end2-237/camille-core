# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# Utilise ghcr.io/puppeteer/puppeteer : Node 20 + Chromium pré-installé

FROM ghcr.io/puppeteer/puppeteer:22

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

# Créer les dossiers nécessaires avec les bons droits (pptruser = uid 1000)
RUN mkdir -p sessions public/media && chown -R pptruser:pptruser /app

USER pptruser

EXPOSE 3000
CMD ["node", "index.js"]
