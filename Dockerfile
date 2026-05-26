# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# Node 18 slim + Chromium pour whatsapp-web.js (Puppeteer)

FROM node:18-bullseye-slim

# Dépendances système pour Chromium / Puppeteer
RUN apt-get update && apt-get install -y \
    chromium \
    fonts-freefont-ttf \
    fonts-noto-color-emoji \
    ca-certificates \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Pointer Puppeteer vers Chromium système (évite le double téléchargement)
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app

# Installer les dépendances npm en premier (cache Docker)
COPY package*.json ./
RUN npm install --production

# Copier le code source
COPY . .

# Créer le dossier de sessions
RUN mkdir -p sessions

EXPOSE 3000

CMD ["node", "index.js"]
