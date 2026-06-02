# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# Utilise ghcr.io/puppeteer/puppeteer : Node 20 + Chromium pré-installé

FROM ghcr.io/puppeteer/puppeteer:22

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

# Repasser root pour créer les dossiers et installer les dépendances
USER root

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

# Créer les dossiers et donner les droits à pptruser
RUN mkdir -p sessions public/media && chown -R pptruser:pptruser /app

USER pptruser

EXPOSE 3000
CMD ["node", "index.js"]
