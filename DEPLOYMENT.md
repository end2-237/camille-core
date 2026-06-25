# Deployment Guide — Camille Core (Render)

## Quick Start

### 1. Prerequisites
- Render.com account ([sign up free](https://render.com))
- Git repository pushed to GitHub
- API_KEY changed from default (see step 3)

### 2. Connect Repository
1. Log into [Render Dashboard](https://dashboard.render.com)
2. Click **"New"** → **"Web Service"**
3. Select **"Build and deploy from a Git repository"**
4. Connect your GitHub account and select `end2-237/camille-core`
5. Select the **`render`** branch

### 3. Configure Service

| Setting | Value | Notes |
|---------|-------|-------|
| **Name** | `camille-core` | Display name in dashboard |
| **Runtime** | Node | Auto-detected |
| **Region** | `oregon` (or your choice) | Closest to your users |
| **Plan** | **Standard** ($12/mo) | Free tier stops after 15 min idle |
| **Build Command** | `npm ci --omit=dev` | Auto-set from render.yaml |
| **Start Command** | `node index.js` | Auto-set from render.yaml |

### 4. Set Environment Variables

Go to **Environment** tab and update:

| Key | Value | Required |
|-----|-------|----------|
| `API_KEY` | Generate a strong secret (e.g., `openssl rand -hex 32`) | ✅ Yes |
| `N8N_WEBHOOK_URL` | `https://your-n8n-instance/webhook/...` | ❌ Optional |
| `MAX_SESSIONS` | `5` (adjust for your plan) | ✅ Yes |

**⚠️ Never commit actual API_KEY to git!** Use Render's env var system.

### 5. Create Persistent Disk

1. Scroll to **"Disk"** section
2. Click **"Add Disk"**
3. Configure:
   - **Name**: `camille-data`
   - **Mount Path**: `/var/data`
   - **Size**: `10 GB` (adjust if needed)
4. This stores:
   - Session auth files (`/var/data/sessions/session-*`)
   - WhatsApp credentials (persistent across restarts)
   - Analytics (`/var/data/sessions/analytics.jsonl`)
   - Webhook config (`/var/data/sessions/webhooks.json`)

### 6. Deploy

Click **"Deploy"** — Render will:
1. Clone your repo
2. Install dependencies (`npm ci`)
3. Mount the persistent disk
4. Start the service on port `10000`
5. Run health checks

**First deployment: ~2-3 minutes**

---

## Accessing Your Service

Once deployed:

```
🌐 Base URL: https://camille-core-xxxxx.onrender.com
📱 Dashboard: https://camille-core-xxxxx.onrender.com/dashboard
🏥 Health: https://camille-core-xxxxx.onrender.com/health
```

### Example: Check Service Status
```bash
curl -X GET \
  https://camille-core-xxxxx.onrender.com/health \
  -H "X-Api-Key: your-api-key"
```

### Example: Create Session
```bash
curl -X POST \
  https://camille-core-xxxxx.onrender.com/api/sessions \
  -H "X-Api-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"name":"bot1"}'
```

---

## Monitoring & Logs

### View Logs
- **Dashboard**: [Render Dashboard](https://dashboard.render.com) → Select service → **"Logs"** tab
- **Real-time**: `tail -f /var/data/sessions/debug.log` (via SSH)

### Key Metrics
- **Disk Usage**: Check `/var/data` size
- **Sessions**: Count files in `/var/data/sessions/session-*`
- **Uptime**: Green status in dashboard = healthy

### Common Issues

| Issue | Solution |
|-------|----------|
| **Service keeps restarting** | Check logs for errors; increase `qrTimeout` or `defaultQueryTimeoutMs` |
| **"Disk full" error** | Increase disk size or reduce `ANALYTICS_RETENTION_DAYS` |
| **QR Code doesn't appear** | Ensure Socket.io CORS is enabled (default: `*`) |
| **Sessions lost after restart** | Verify `/var/data` disk is mounted correctly |

---

## Production Checklist

- [ ] Change `API_KEY` to a secure value
- [ ] Set `N8N_WEBHOOK_URL` if using n8n
- [ ] Verify persistent disk is mounted
- [ ] Test endpoint: `GET /health`
- [ ] Test session creation: `POST /api/sessions`
- [ ] Enable PR previews (optional)
- [ ] Set up uptime monitoring (Render's built-in or external)
- [ ] Configure log retention (if needed)

---

## Scaling

### For More Sessions
- **Free/Starter**: 1-2 sessions max
- **Standard**: 5-10 sessions (recommended)
- **Pro**: 10+ sessions with autoscaling

### Upgrade Plan
1. Render Dashboard → Service → **"Settings"**
2. Select new plan (charges prorated)
3. Service auto-restarts with new resources

### Multi-Instance (Advanced)
Enable autoscaling in `render.yaml`:
```yaml
autoscale:
  minInstances: 1
  maxInstances: 3
  targetCpuPercent: 70
```

---

## Troubleshooting

### Logs Show "Connection Failed"
1. Check WhatsApp account status (not banned/suspended)
2. Try re-pairing with new QR code
3. Check `MAX_RECONNECT_TRIES` not exhausted

### Persistent Data Not Saved
1. Verify `/var/data` disk is mounted: `df -h` (in SSH)
2. Check write permissions: `ls -la /var/data/`
3. Restart service: Dashboard → **"Restart"**

### Performance Issues
- Reduce `MAX_SESSIONS`
- Increase plan to Pro (more CPU/RAM)
- Enable `BAILEYS_LOG_LEVEL=fatal` (less I/O)

---

## Next Steps

- [Render Docs](https://render.com/docs)
- [Baileys GitHub](https://github.com/WhiskeySockets/Baileys)
- [n8n Integration](https://n8n.io)

**Need help?** Open an issue on GitHub or contact Render support.
