# Webhook Error 404 — Guide de Resolution

## Problem
```
⚠ 1 erreur(s) webhook n8n
Dernière erreur: webhook error: Request failed with status code 404
```

**Signification:** Le webhook n8n n'existe pas ou l'URL est incorrecte.

---

## Solution Rapide (5 minutes)

### 1. Vérifier l'URL du webhook dans n8n

L'URL fournie:
```
https://noushi14.app.n8n.cloud/webhook-test/6d5f403b-020d-47cd-aeda-a7349ee79e6e
```

Problème potentiel: C'est une URL de **test temporaire** qui peut expirer ou être supprimée.

**Action:**
1. Allez dans votre n8n: https://noushi14.app.n8n.cloud
2. Ouvrez votre workflow WhatsApp
3. Cherchez le nœud "Webhook" ou "HTTP"
4. Copiez l'URL correcte (elle doit être active et fonctionnelle)

### 2. Tester l'URL du webhook

Vérifier si l'URL est valide avec:

```bash
curl -X POST "https://noushi14.app.n8n.cloud/webhook-test/6d5f403b-020d-47cd-aeda-a7349ee79e6e" \
  -H "Content-Type: application/json" \
  -d '{
    "event": "test",
    "message": "Hello"
  }'
```

**Si vous recevez 404:** L'URL n'existe plus, créer une nouvelle URL dans n8n
**Si vous recevez 200 OK:** L'URL est valide, passer à l'étape 3

### 3. Reconfigurer le webhook dans Camille Core

```bash
curl -X POST https://camille-core.onrender.com/api/config/webhooks \
  -H "X-Api-Key: camille-core-secret" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "default",
    "url": "https://noushi14.app.n8n.cloud/webhook-test/VOTRE_NOUVELLE_URL",
    "test": true
  }'
```

Remplacez `VOTRE_NOUVELLE_URL` par l'URL correcte du webhook n8n.

### 4. Vérifier que ça fonctionne

```bash
curl -X POST https://camille-core.onrender.com/api/config/webhooks/default/test \
  -H "X-Api-Key: camille-core-secret"
```

Réponse réussie:
```json
{
  "success": true,
  "message": "Webhook test OK",
  "url": "https://noushi14.app.n8n.cloud/webhook-test/...",
  "response": {
    "status": 200,
    "statusText": "OK"
  }
}
```

Réponse échouée:
```json
{
  "success": false,
  "error": "Webhook test failed",
  "details": {
    "message": "Request failed with status code 404",
    "status": 404
  },
  "suggestion": "Le webhook n8n n'existe pas ou a été supprimé. Vérifier l'URL dans n8n."
}
```

---

## Causes Courantes de l'Erreur 404

| Cause | Solution |
|-------|----------|
| URL webhook supprimée dans n8n | Créer une nouvelle URL webhook dans n8n |
| URL webhook test temporaire expirée | Utiliser une URL webhook permanente |
| Workflow n8n désactivé | Activer le workflow dans n8n |
| URL mal copiée | Revérifier l'URL depuis n8n |
| Webhook n8n non configuré | Ajouter un nœud "Webhook" dans n8n |
| Mauvaise base URL n8n | Utiliser l'URL correcte du serveur n8n |

---

## Comment Créer un Webhook dans n8n

### Étape 1: Ouvrir/Créer un Workflow
- Allez dans "Workflows" dans n8n
- Créez nouveau ou ouvrez existant

### Étape 2: Ajouter un Webhook
1. Cliquez sur "+" pour ajouter un nœud
2. Cherchez "Webhook"
3. Sélectionnez le nœud Webhook
4. Choisissez "POST" comme méthode

### Étape 3: Copier l'URL
- Le nœud Webhook affiche une URL comme:
  ```
  https://noushi14.app.n8n.cloud/webhook-test/6d5f403b-020d-47cd-aeda-a7349ee79e6e
  ```
- Copiez cette URL complètement

### Étape 4: Configurer le Traitement
- Ajoutez des nœuds après le Webhook pour traiter les messages
- Exemple: "Save to DB", "Send Response", etc.

### Étape 5: Activer le Workflow
- Cliquez sur "Active" (bouton en haut)
- Vérifiez que le statut change à "Enabled"

---

## Debugger le Webhook

### Voir les logs en temps réel
```bash
# SSH dans Render (ou docker)
ssh your-render-instance
tail -f /var/data/sessions/debug.log | grep webhook
```

### Logs attendus (succès)
```
[default] → webhook: https://noushi14.app.n8n.cloud/webhook... (source: session)
[default] ✓ webhook OK (200)
```

### Logs attendus (erreur)
```
[default] → webhook: https://noushi14.app.n8n.cloud/webhook... (source: session)
[default] ✗ webhook ERROR: webhook 404: URL not found or webhook deleted (status: 404)
```

---

## Vérifier la Configuration Sauvegardée

```bash
# Voir la configuration actuellement sauvegardée
curl https://camille-core.onrender.com/api/config/webhooks \
  -H "X-Api-Key: camille-core-secret"
```

Réponse:
```json
{
  "global": "",
  "sessions": {
    "default": "https://noushi14.app.n8n.cloud/webhook-test/6d5f403b-020d-47cd-aeda-a7349ee79e6e"
  }
}
```

### Obtenir le webhook d'une session
```bash
curl https://camille-core.onrender.com/api/config/webhooks/default \
  -H "X-Api-Key: camille-core-secret"
```

---

## Format du Payload Attendu par n8n

Camille Core envoie ce format:

```json
{
  "event": "message",
  "session": "default",
  "payload": {
    "id": "message-id-123",
    "from": "1234567890@s.whatsapp.net",
    "fromMe": false,
    "body": "Bonjour!",
    "type": "chat",
    "timestamp": 1705337440,
    "notifyName": "User Name"
  }
}
```

n8n recevra ce payload dans le nœud Webhook. Vous pouvez l'accéder via:
- `{{ $json.body }}` — le contenu du message
- `{{ $json.from }}` — le numéro qui envoie
- `{{ $json.session }}` — la session (ex: "default")
- `{{ $json.payload.body }}` — le texte du message

---

## Erreurs Courantes et Solutions

### "webhook error: Request failed with status code 404"
**Cause:** URL webhook invalide  
**Solution:** Vérifier l'URL dans n8n et reconfigurer

### "webhook error: Request failed with status code 401"
**Cause:** Authentification requise  
**Solution:** Ajouter les headers d'auth dans la configuration Camille Core ou n8n

### "webhook: connexion refusée"
**Cause:** Serveur n8n inaccessible  
**Solution:** Vérifier que le serveur n8n est en ligne et accessible

### "webhook: timeout"
**Cause:** n8n est trop lent  
**Solution:** Optimiser le workflow n8n ou augmenter le timeout (30s par défaut)

---

## Commandes Utiles

### Reconfigurer le webhook
```bash
curl -X POST https://camille-core.onrender.com/api/config/webhooks \
  -H "X-Api-Key: camille-core-secret" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "default",
    "url": "https://votre-url-webhook-n8n.com",
    "test": true
  }'
```

### Tester le webhook
```bash
curl -X POST https://camille-core.onrender.com/api/config/webhooks/default/test \
  -H "X-Api-Key: camille-core-secret"
```

### Supprimer le webhook
```bash
curl -X POST https://camille-core.onrender.com/api/config/webhooks \
  -H "X-Api-Key: camille-core-secret" \
  -H "Content-Type: application/json" \
  -d '{
    "session": "default",
    "url": ""
  }'
```

---

## Support

Si le problème persiste:

1. Vérifiez que l'URL du webhook dans n8n est accessible (test curl)
2. Vérifiez que le workflow n8n est activé
3. Vérifiez les logs du webhook: `tail -f debug.log | grep webhook`
4. Testez manuellement avec curl pour isoler le problème
5. Vérifiez la configuration sauvegardée: `GET /api/config/webhooks`

---

## Checklist de Resolution

- [ ] Accédé à n8n et ouvert le workflow
- [ ] Copié l'URL correcte du webhook
- [ ] Testé l'URL avec curl (doit retourner 200)
- [ ] Reconfiguré le webhook dans Camille Core
- [ ] Testé avec POST /api/config/webhooks/:session/test
- [ ] Envoyé un message WhatsApp
- [ ] Vérifiez que le message arrive dans n8n
- [ ] Testé l'export/traitement du message dans n8n
