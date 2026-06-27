# Configuration des Webhooks — Camille Core

Ce guide explique comment configurer et tester les webhooks pour envoyer les messages WhatsApp reçus vers n8n ou toute autre plateforme.

## Vue d'ensemble

Camille Core peut envoyer **tous les messages WhatsApp reçus** vers un webhook (URL HTTP) automatiquement. Vous pouvez configurer:

- **Webhook global**: s'applique à toutes les sessions
- **Webhooks par session**: chaque session peut avoir son propre webhook (surcharge le global)

Le système est **persistent** — la configuration est sauvegardée dans `/var/data/sessions/webhooks.json` et survit aux redémarrages.

---

## 1. Configuration rapide

### Option A: Environnement (global, au démarrage)

```bash
N8N_WEBHOOK_URL=https://your-n8n-instance.com/webhook/whatsapp
```

### Option B: API (par session, pendant l'exécution)

```bash
curl -X POST http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "default",
    "url": "https://your-n8n-instance.com/webhook/session1",
    "test": true
  }'
```

### Option C: Webhook global via API

```bash
curl -X POST http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "__global__",
    "url": "https://your-n8n-instance.com/webhook/global",
    "test": true
  }'
```

---

## 2. Endpoints API

### GET /api/config/webhooks
**Obtenir la configuration actuelle des webhooks**

```bash
curl http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: your-api-key"
```

**Réponse:**
```json
{
  "global": "https://your-n8n.com/webhook/default",
  "sessions": {
    "default": "https://your-n8n.com/webhook/session1",
    "bot2": "https://your-n8n.com/webhook/bot2"
  }
}
```

---

### GET /api/config/webhooks/:session
**Obtenir le webhook d'une session spécifique**

```bash
curl http://localhost:3000/api/config/webhooks/default \
  -H "X-Api-Key: your-api-key"
```

**Réponse:**
```json
{
  "session": "default",
  "url": "https://your-n8n.com/webhook/session1",
  "source": "session"
}
```

`source` indique d'où vient l'URL:
- `session`: Configuré spécifiquement pour cette session
- `global`: Utilise le webhook global
- `none`: Aucun webhook configuré

---

### POST /api/config/webhooks
**Configurer un webhook pour une session**

```bash
curl -X POST http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "default",
    "url": "https://your-n8n-instance.com/webhook/default",
    "test": false
  }'
```

**Paramètres:**
- `session` (string, requis): Nom de la session ou `__global__`
- `url` (string): URL du webhook (laisser vide pour supprimer)
- `test` (boolean, optionnel): Tester le webhook avant de le sauvegarder (défaut: false)

**Réponse (succès):**
```json
{
  "success": true,
  "message": "Webhook configuré avec succès",
  "config": {
    "session": "default",
    "url": "https://your-n8n.com/webhook/default",
    "tested": false
  }
}
```

**Réponse (test échoué):**
```json
{
  "error": "Webhook test failed: connect ECONNREFUSED",
  "details": "Connection error"
}
```

---

### POST /api/config/webhooks/:session/test
**Tester le webhook d'une session**

```bash
curl -X POST http://localhost:3000/api/config/webhooks/default/test \
  -H "X-Api-Key: your-api-key"
```

**Réponse (succès):**
```json
{
  "success": true,
  "message": "Webhook test OK",
  "response": {
    "status": 200,
    "statusText": "OK",
    "data": "Message received"
  }
}
```

**Réponse (erreur):**
```json
{
  "success": false,
  "error": "Webhook test failed",
  "details": {
    "message": "connect ECONNREFUSED",
    "code": "ECONNREFUSED",
    "status": null,
    "statusText": null
  }
}
```

---

## 3. Payload webhook

Quand un message est reçu, Camille Core envoie:

```json
{
  "event": "message",
  "session": "default",
  "payload": {
    "id": "3EB08E1234567890ABCDEF",
    "from": "33612345678@c.us",
    "fromMe": false,
    "body": "Bonjour!",
    "type": "chat",
    "timestamp": 1234567890,
    "notifyName": "John"
  }
}
```

**Types de messages** (`type`):
- `chat`: Message texte
- `image`: Photo
- `video`: Vidéo
- `audio`: Fichier audio
- `ptt`: Message vocal (push-to-talk)
- `document`: Fichier
- `sticker`: Sticker
- `location`: Localisation
- `vcard`: Contact
- `unknown`: Autre

**JID format**: Camille Core utilise le format `@c.us` (compatibilité n8n v1):
- `33612345678@c.us`: Numéro utilisateur
- `33612345678-1234567890@g.us`: Groupe

---

## 4. Ordre de priorité

Lors de la réception d'un message, Camille Core utilise **cette ordre** pour trouvé le webhook:

1. **Session webhook** (`webhookConfig.sessions[name]`)
   → Configuré via API `/api/config/webhooks`
   → Stocké dans `webhooks.json`

2. **Environment variable** (`N8N_WEBHOOK_${SESSION_NAME}`)
   → Exemple: `N8N_WEBHOOK_DEFAULT`, `N8N_WEBHOOK_BOT2`

3. **Webhook global** (`webhookConfig.global`)
   → Configuré via API avec `session: "__global__"`
   → Stocké dans `webhooks.json`

4. **Environment global** (`N8N_WEBHOOK_URL`)
   → Défini au démarrage du conteneur

5. **Aucun** → Message reçu mais non envoyé

**Exemple:**
```
Session: "default"

1. Cherche: webhookConfig.sessions["default"] → TROUVÉ!
   Utilise: https://your-n8n.com/webhook/default
```

---

## 5. Exemples pratiques

### Exemple 1: Webhook global simple

```bash
# Au démarrage (avec Docker)
docker run -e N8N_WEBHOOK_URL=https://n8n.example.com/webhook/camille ...

# Ou via API après le démarrage
curl -X POST http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "__global__",
    "url": "https://n8n.example.com/webhook/camille",
    "test": true
  }'
```

### Exemple 2: Webhooks différents par session

```bash
# Session "default" → webhook A
curl -X POST http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "default",
    "url": "https://n8n.example.com/webhook/account1",
    "test": true
  }'

# Session "bot2" → webhook B
curl -X POST http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "bot2",
    "url": "https://n8n.example.com/webhook/account2",
    "test": true
  }'

# Vérifier la configuration
curl http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: my-secret-key"
```

### Exemple 3: Tester avant de sauvegarder

```bash
# Configurer ET tester en même temps
curl -X POST http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "default",
    "url": "https://your-webhook.com/incoming",
    "test": true
  }'

# Si le test échoue, le webhook ne sera pas sauvegardé
```

### Exemple 4: Supprimer un webhook

```bash
# Laisser l'URL vide pour supprimer
curl -X POST http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "default",
    "url": ""
  }'
```

---

## 6. Débogage

### Logs des webhooks

Tous les logs des webhooks sont dans `/var/data/sessions/debug.log`:

```bash
# Vérifier les logs
tail -f /var/data/sessions/debug.log | grep webhook

# Chercher les erreurs
grep "webhook ERROR" /var/data/sessions/debug.log
```

**Exemples de logs:**
```
[2024-01-15T10:30:45.123Z] → webhook: https://n8n.example.com/webhook... (source: session)
[2024-01-15T10:30:46.456Z] ✓ webhook OK (200)

[2024-01-15T10:31:00.789Z] ✗ webhook ERROR: connect ECONNREFUSED (status: N/A)
```

### Endpoints de diagnostic

```bash
# Voir la configuration complète
curl http://localhost:3000/api/config/webhooks \
  -H "X-Api-Key: my-secret-key"

# Tester un webhook spécifique
curl -X POST http://localhost:3000/api/config/webhooks/default/test \
  -H "X-Api-Key: my-secret-key"

# Voir la santé globale du serveur
curl http://localhost:3000/api/server/info \
  -H "X-Api-Key: my-secret-key"
```

### Problèmes courants

| Problème | Cause | Solution |
|----------|-------|----------|
| `✗ pas de webhook configuré` | Aucun webhook défini | Configurer via `/api/config/webhooks` |
| `webhook ERROR: connect ECONNREFUSED` | URL inaccessible | Vérifier l'URL, les pare-feu, DNS |
| `webhook ERROR: timeout` | Webhook trop lent | Augmenter timeout (30s par défaut) ou optimiser le webhook |
| `webhook ERROR: 401 Unauthorized` | Authentification requise | Ajouter auth headers dans le webhook n8n |
| Config perdue après redémarrage | Webhooks non persistés | S'assurer que `/var/data` est un volume persistent Docker |

---

## 7. Avec Docker/Render

### Dans compose.yml ou render.yaml:

```yaml
environment:
  N8N_WEBHOOK_URL: https://your-n8n-instance.com/webhook/camille
  # OU laisser vide et configurer via API pendant l'exécution
```

### Disk persistant (requis):

```yaml
mounts:
  - path: /var/data
    size: 10GB
```

Cela garantit que `webhooks.json` survit aux redémarrages.

---

## 8. Intégration n8n

### Dans n8n, créer un webhook entrant:

1. Ajouter le trigger "Webhook"
2. Copier l'URL webhooks
3. Configurer dans Camille Core:
   ```bash
   curl -X POST http://localhost:3000/api/config/webhooks \
     -H "X-Api-Key: your-api-key" \
     -H "Content-Type: application/json" \
     -d '{
       "session": "default",
       "url": "https://n8n.your-domain.com/webhook/whatsapp-incoming"
     }'
   ```
4. Tester: envoyer un message WhatsApp vers la session → voir le webhook reçu dans n8n

---

## 9. Checklist de production

- [ ] Webhook global configuré OU webhooks par session configurés
- [ ] Webhook testé avec `/api/config/webhooks/:session/test`
- [ ] `/var/data` est un volume persistent (Docker/Render)
- [ ] API_KEY sécurisé dans les variables d'environnement
- [ ] n8n accessible depuis l'external (whitelist IP si nécessaire)
- [ ] Logs des webhooks vérifiés: `grep webhook /var/data/sessions/debug.log`
- [ ] Payload n8n testé avec JSON exemple
- [ ] Gestion des erreurs n8n configurée (retry, dead-letter queue)

---

## Support

Pour les problèmes:
1. Vérifier les logs: `tail -f /var/data/sessions/debug.log`
2. Tester le webhook: `POST /api/config/webhooks/:session/test`
3. Vérifier la config: `GET /api/config/webhooks`
4. Consulter les détails d'erreur dans la réponse API
