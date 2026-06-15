# ── Camille Core Dockerfile ──────────────────────────────────────────────────
# ghcr.io/puppeteer/puppeteer:22 = Node 20 + Google Chrome stable pré-installé

FROM ghcr.io/puppeteer/puppeteer:22

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome-stable

# tini : init PID-1 léger → récolte les process zombies Chrome
# (sans init, les workers Chromium morts restent comme zombies indéfiniment)
RUN apt-get update -qq && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*

USER root
WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .
RUN mkdir -p sessions public/media

EXPOSE 3000

# tini comme PID 1 — capte SIGTERM et récolte les zombies
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["node", "index.js"]
