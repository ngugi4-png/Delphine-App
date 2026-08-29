/**
 * Notification layer — sends SMS via Africa's Talking and/or WhatsApp via
 * the Meta WhatsApp Cloud API. Both are optional: if credentials for a
 * channel aren't configured, that channel is skipped (not an error), so
 * the app works with SMS only, WhatsApp only, or both.
 *
 * SETUP (one-time, from the `functions/` directory):
 *   firebase functions:secrets:set AT_USERNAME
 *   firebase functions:secrets:set AT_API_KEY
 *   firebase functions:secrets:set WA_PHONE_NUMBER_ID
 *   firebase functions:secrets:set WA_ACCESS_TOKEN
 *
 * Africa's Talking (SMS): https://africastalking.com — sign up, create an
 *   app, get your username + API key from the dashboard.
 * Meta WhatsApp Cloud API: https://developers.facebook.com/docs/whatsapp
 *   — create a Meta app, add WhatsApp product, get a phone number ID and
 *   a permanent access token.
 *
 * Recipient phone numbers, and which channels are enabled, are configured
 * from inside the app itself (Settings → Alerts), stored in the Firestore
 * doc `settings/alerts` — not here. This file only knows how to send.
 */

const { defineSecret } = require('firebase-functions/params');
const https = require('https');

const AT_USERNAME = defineSecret('AT_USERNAME');
const AT_API_KEY = defineSecret('AT_API_KEY');
const WA_PHONE_NUMBER_ID = defineSecret('WA_PHONE_NUMBER_ID');
const WA_ACCESS_TOKEN = defineSecret('WA_ACCESS_TOKEN');

function httpsRequest(options, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let chunks = '';
      res.on('data', (c) => { chunks += c; });
      res.on('end', () => resolve({ status: res.statusCode, body: chunks }));
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function sendSMS(to, message) {
  let username, apiKey;
  try { username = AT_USERNAME.value(); apiKey = AT_API_KEY.value(); } catch (e) { /* not configured */ }
  if (!username || !apiKey) return { skipped: true, reason: 'Africa\'s Talking credentials not configured' };

  const body = new URLSearchParams({ username, to, message }).toString();
  return httpsRequest({
    hostname: 'api.africastalking.com',
    path: '/version1/messaging',
    method: 'POST',
    headers: {
      apiKey: apiKey,
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json',
      'Content-Length': Buffer.byteLength(body),
    },
  }, body);
}

async function sendWhatsApp(to, message) {
  let phoneId, token;
  try { phoneId = WA_PHONE_NUMBER_ID.value(); token = WA_ACCESS_TOKEN.value(); } catch (e) { /* not configured */ }
  if (!phoneId || !token) return { skipped: true, reason: 'WhatsApp credentials not configured' };

  const toDigits = String(to).replace(/[^0-9]/g, ''); // WhatsApp wants digits only, no +
  const body = JSON.stringify({
    messaging_product: 'whatsapp',
    to: toDigits,
    type: 'text',
    text: { body: message },
  });
  return httpsRequest({
    hostname: 'graph.facebook.com',
    path: `/v19.0/${phoneId}/messages`,
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    },
  }, body);
}

/**
 * Reads recipient config from Firestore (`settings/alerts`), sends the
 * message through every enabled channel to every configured number, and
 * logs one row to `alerts` regardless of whether any channel was actually
 * configured — so the alert history is always complete even if nobody
 * has set up SMS/WhatsApp credentials yet.
 */
async function notifyAdmins(db, admin, { title, message, severity, category, meta }) {
  const cfgSnap = await db.collection('settings').doc('alerts').get();
  const cfg = cfgSnap.exists ? cfgSnap.data() : {};
  const numbers = Array.isArray(cfg.phoneNumbers) ? cfg.phoneNumbers : [];
  const channels = cfg.channels || { sms: true, whatsapp: false };

  const results = [];
  for (const num of numbers) {
    if (channels.sms) {
      const r = await sendSMS(num, message).catch((e) => ({ error: e.message }));
      results.push({ channel: 'sms', to: num, result: r });
    }
    if (channels.whatsapp) {
      const r = await sendWhatsApp(num, message).catch((e) => ({ error: e.message }));
      results.push({ channel: 'whatsapp', to: num, result: r });
    }
  }

  await db.collection('alerts').add({
    title,
    message,
    severity: severity || 'warning',
    category: category || 'general',
    meta: meta || {},
    sentTo: numbers,
    results,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  });

  return results;
}

module.exports = {
  sendSMS,
  sendWhatsApp,
  notifyAdmins,
  AT_USERNAME,
  AT_API_KEY,
  WA_PHONE_NUMBER_ID,
  WA_ACCESS_TOKEN,
};
