# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# Node 18 slim + Chromium pour whatsapp-web.js (Puppeteer)

FROM node:18-bullseye-slim

# Chromium uniquement — ses dépendances (libogg, libnss3, etc.) sont
# résolues automatiquement par apt. --fix-missing gère les erreurs réseau
# transitoires pendant le build.
RUN apt-get update && apt-get install -y --fix-missing \
    chromium \
    fonts-freefont-ttf \
    ca-certificates \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Pointer Puppeteer vers le Chromium système
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .
RUN mkdir -p sessions

EXPOSE 3000
CMD ["node", "index.js"]
