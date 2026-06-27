#!/bin/bash

# Test script for Camille Core webhook configuration
# Usage: ./test-webhook.sh <base-url> <api-key> [session-name] [webhook-url]

set -e

BASE_URL="${1:-http://localhost:3000}"
API_KEY="${2:-camille-core-secret}"
SESSION="${3:-default}"
WEBHOOK_URL="${4:-http://localhost:3000/webhook-test}"

echo "═══════════════════════════════════════════════════════════════"
echo "  Camille Core — Webhook Configuration Test"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Base URL:    $BASE_URL"
echo "API Key:     ${API_KEY:0:10}..."
echo "Session:     $SESSION"
echo "Webhook URL: $WEBHOOK_URL"
echo ""

# 1. Health check
echo "[1/5] Health Check..."
curl -s "$BASE_URL/health" | jq '.' 2>/dev/null || echo "❌ Server not responding"
echo ""

# 2. Get current webhooks
echo "[2/5] Current Webhook Configuration..."
curl -s -H "X-Api-Key: $API_KEY" "$BASE_URL/api/config/webhooks" | jq '.' || echo "❌ Failed to get webhooks"
echo ""

# 3. Configure webhook
echo "[3/5] Configure Webhook (without test)..."
RESPONSE=$(curl -s -X POST "$BASE_URL/api/config/webhooks" \
  -H "X-Api-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"session\": \"$SESSION\",
    \"url\": \"$WEBHOOK_URL\",
    \"test\": false
  }")

echo "$RESPONSE" | jq '.' || echo "❌ Failed to configure webhook"
echo ""

# 4. Get webhook for specific session
echo "[4/5] Get Webhook for Session '$SESSION'..."
curl -s -H "X-Api-Key: $API_KEY" "$BASE_URL/api/config/webhooks/$SESSION" | jq '.' || echo "❌ Failed to get session webhook"
echo ""

# 5. Test webhook
echo "[5/5] Test Webhook..."
curl -s -X POST "$BASE_URL/api/config/webhooks/$SESSION/test" \
  -H "X-Api-Key: $API_KEY" | jq '.' || echo "❌ Failed to test webhook"
echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "✅ Test completed!"
echo ""
echo "Next steps:"
echo "1. Check the logs: tail -f /var/data/sessions/debug.log"
echo "2. Send a WhatsApp message to trigger the webhook"
echo "3. Look for 'webhook OK' or 'webhook ERROR' in logs"
echo "═══════════════════════════════════════════════════════════════"
