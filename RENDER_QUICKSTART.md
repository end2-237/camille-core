# 🚀 Render Deployment — Quick Start

**Branch**: `render`  
**Service**: WhatsApp API Gateway (Baileys)  
**Status**: Production-ready ✅

## 1️⃣ Prerequisites
- Render.com account (free)
- GitHub repo connected
- Strong API key (for security)

## 2️⃣ Deploy in 5 Minutes

### Step 1: Connect Repository
```bash
# On GitHub: Push the render branch
git push origin render
```

### Step 2: Create Service on Render
1. Go to [render.com/dashboard](https://dashboard.render.com)
2. Click **New** → **Web Service**
3. Select **Build from Git**
4. Choose your repo: `end2-237/camille-core`
5. Select branch: `render` ← Important!

### Step 3: Configure
| Setting | Value |
|---------|-------|
| Name | `camille-core` |
| Runtime | Node |
| Region | `oregon` (or your preference) |
| Plan | **Standard** ($12/mo) |
| Build | Auto (from render.yaml) |
| Start | Auto (from render.yaml) |

### Step 4: Set Environment Variables
In Render dashboard → **Environment**:

```env
API_KEY=your-super-secret-key-here
N8N_WEBHOOK_URL=https://your-n8n.com/webhook/camille
MAX_SESSIONS=5
```

### Step 5: Create Persistent Disk
In Render dashboard → **Disk**:

```
Name:       camille-data
Mount Path: /var/data
Size:       10 GB
```

### Step 6: Deploy
Click **Deploy** — Done! ✅

## 3️⃣ After Deployment

### Access Your Service
```
🌐 Base URL:  https://camille-core-xxxxx.onrender.com
📱 Dashboard: https://camille-core-xxxxx.onrender.com/dashboard
🏥 Health:    https://camille-core-xxxxx.onrender.com/health
```

### Test It
```bash
# Check health
curl https://camille-core-xxxxx.onrender.com/health

# Create session
curl -X POST https://camille-core-xxxxx.onrender.com/api/sessions \
  -H "X-Api-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"name":"bot1"}'
```

## 4️⃣ Why Render (vs Vercel)?

| Feature | Render | Vercel |
|---------|--------|--------|
| Node.js Backend | ✅ Native | ⚠️ Serverless |
| WebSocket/Socket.io | ✅ Yes | ❌ Paid only |
| Long-running tasks | ✅ Yes | ❌ 60-900s max |
| Persistent Disk | ✅ Yes | ❌ Ephemeral |
| Sessions storage | ✅ Survives restart | ❌ Lost |
| Cost | $12/mo (Standard) | Free/Paid |

## 5️⃣ Monitoring

### Logs
Dashboard → **Logs** tab (real-time)

### Metrics
- Status: Green = healthy
- Active sessions: Dashboard overview
- Uptime: Check HTTP status

### Common Issues
| Issue | Fix |
|-------|-----|
| Service keeps restarting | Check logs; increase timeout |
| "Disk full" error | Reduce `ANALYTICS_RETENTION_DAYS` |
| Sessions lost | Verify disk mounted at `/var/data` |
| QR code doesn't show | Check Socket.io CORS (default: `*`) |

## 6️⃣ Scaling

### More Sessions?
1. Dashboard → **Settings**
2. Upgrade plan (Starter → Standard → Pro)
3. Increase `MAX_SESSIONS` env var

### Auto-scaling (Advanced)
Edit `render.yaml` → Uncomment `autoscale` section

## 7️⃣ Production Checklist

- [ ] Change `API_KEY` from default
- [ ] Set `N8N_WEBHOOK_URL` if using n8n
- [ ] Verify disk is mounted (`/var/data`)
- [ ] Test health endpoint
- [ ] Test session creation
- [ ] Monitor logs for errors
- [ ] Enable PR previews (optional)

## 📚 More Info
- [Full Deployment Guide](./DEPLOYMENT.md)
- [Render Docs](https://render.com/docs)
- [Baileys GitHub](https://github.com/WhiskeySockets/Baileys)

---

**Questions?** Check DEPLOYMENT.md or open an issue on GitHub.

**Ready?** 👇

```bash
git push origin render
```

Then go to [render.com/dashboard](https://dashboard.render.com) and click **New Web Service**! 🚀
