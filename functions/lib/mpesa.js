/**
 * Safaricom Daraja M-Pesa C2B Till Integration
 * -----------------------------------------------
 * Safaricom does NOT offer an API to pull historical till transactions
 * on demand. The only way to know what's landed in a till is a live
 * webhook: you register a Confirmation URL once, and Safaricom calls it
 * in real time, the instant each payment completes. This file:
 *   1. Registers that webhook with Safaricom (one-time, per till number)
 *   2. Receives the webhook calls and stores each payment in Firestore
 * The app then sums whatever's been captured for a branch/date when a
 * shift report is created — see `fetchMpesaCollection()` in the client.
 *
 * SETUP (one-time):
 *   1. Sign up at https://developer.safaricom.co.ke, create an app,
 *      get your Consumer Key + Consumer Secret.
 *   2. firebase functions:secrets:set MPESA_CONSUMER_KEY
 *      firebase functions:secrets:set MPESA_CONSUMER_SECRET
 *   3. firebase functions:secrets:set MPESA_ENV
 *      (value: "sandbox" while testing, "production" once your till is
 *      live and Safaricom has approved your app for production — set
 *      this to "production" or the webhook will silently point at
 *      Safaricom's test environment)
 *   4. In the app: Settings → M-Pesa Till Integration → enter each
 *      branch's till number → Save → click "Register Webhook URLs".
 *      That one click calls `registerMpesaC2BUrls` below, which tells
 *      Safaricom where to send payment confirmations for that till.
 *
 * Note: Safaricom's C2B registration only allows ONE pair of URLs per
 * shortcode/till, and only one active registration at a time on some
 * account tiers — if you've registered URLs for this till elsewhere
 * before, this will overwrite that registration.
 */

const { onRequest, onCall } = require('firebase-functions/v2/https');
const { defineSecret } = require('firebase-functions/params');
const { logger } = require('firebase-functions');
const https = require('https');

const MPESA_CONSUMER_KEY = defineSecret('MPESA_CONSUMER_KEY');
const MPESA_CONSUMER_SECRET = defineSecret('MPESA_CONSUMER_SECRET');
const MPESA_ENV = defineSecret('MPESA_ENV'); // "sandbox" or "production"

function daraja(env) {
  return env === 'production' ? 'api.safaricom.co.ke' : 'sandbox.safaricom.co.ke';
}

function httpsJson(options, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let chunks = '';
      res.on('data', (c) => { chunks += c; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(chunks || '{}') }); }
        catch (e) { resolve({ status: res.statusCode, body: chunks }); }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function getDarajaToken(env) {
  const key = MPESA_CONSUMER_KEY.value();
  const secret = MPESA_CONSUMER_SECRET.value();
  if (!key || !secret) throw new Error('M-Pesa Consumer Key/Secret not configured');
  const auth = Buffer.from(`${key}:${secret}`).toString('base64');
  const res = await httpsJson({
    hostname: daraja(env),
    path: '/oauth/v1/generate?grant_type=client_credentials',
    method: 'GET',
    headers: { Authorization: `Basic ${auth}` },
  });
  if (!res.body || !res.body.access_token) {
    throw new Error('Failed to get Daraja access token: ' + JSON.stringify(res.body));
  }
  return res.body.access_token;
}

/**
 * Callable from the app (Settings → M-Pesa Till Integration → Register
 * Webhook URLs). Registers the Confirmation + Validation URLs for one
 * till with Safaricom, so future payments to that till get pushed here.
 * The Confirmation/Validation URLs are this function's own deployed
 * HTTPS trigger URLs — pass them in from the client, which reads them
 * from the Firebase console or constructs them from the known project.
 */
exports.registerMpesaC2BUrls = onCall(
  { secrets: [MPESA_CONSUMER_KEY, MPESA_CONSUMER_SECRET, MPESA_ENV] },
  async (request) => {
    const { shortCode, confirmationUrl, validationUrl } = request.data || {};
    if (!shortCode || !confirmationUrl || !validationUrl) {
      throw new Error('shortCode, confirmationUrl, and validationUrl are all required');
    }

    let env = 'sandbox';
    try { env = MPESA_ENV.value() || 'sandbox'; } catch (e) { /* default to sandbox */ }

    const token = await getDarajaToken(env);
    const body = JSON.stringify({
      ShortCode: shortCode,
      ResponseType: 'Completed',
      ConfirmationURL: confirmationUrl,
      ValidationURL: validationUrl,
    });

    const res = await httpsJson({
      hostname: daraja(env),
      path: '/mpesa/c2b/v1/registerurl',
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    }, body);

    logger.info(`C2B URL registration for till ${shortCode} (${env}):`, res.body);
    return { env, response: res.body };
  }
);

/**
 * Safaricom's Validation callback — called BEFORE a payment is finalized,
 * asking "should this be accepted?". We accept everything; till payments
 * are already constrained by the till number itself, so there's nothing
 * meaningful to reject here.
 */
exports.mpesaValidation = onRequest((req, res) => {
  res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
});

/**
 * Safaricom's Confirmation callback — called AFTER a payment has
 * completed. This is the actual money-received event. Stored verbatim
 * (plus a resolved branch + normalized date) so the app can sum same-day
 * collections per branch when a shift report is created.
 */
exports.mpesaConfirmation = onRequest(async (req, res) => {
  try {
    const p = req.body || {};
    const shortCode = p.BusinessShortCode || p.ShortCode;
    const amount = parseFloat(p.TransAmount) || 0;
    const transId = p.TransID || '';
    const transTime = p.TransTime || ''; // format: YYYYMMDDHHmmss
    const phone = p.MSISDN || '';
    const name = [p.FirstName, p.MiddleName, p.LastName].filter(Boolean).join(' ');

    // TransTime -> YYYY-MM-DD for same-day querying alongside the rest of the app
    let date = '';
    if (transTime.length >= 8) {
      date = `${transTime.slice(0, 4)}-${transTime.slice(4, 6)}-${transTime.slice(6, 8)}`;
    }

    const admin = require('firebase-admin');
    const db = admin.firestore();

    // Resolve which branch this till belongs to
    const cfgSnap = await db.collection('settings').doc('tillNumbers').get();
    const tillMap = cfgSnap.exists ? cfgSnap.data() : {};
    let branch = null;
    Object.keys(tillMap).forEach((b) => { if (String(tillMap[b]) === String(shortCode)) branch = b; });

    // Idempotency: Safaricom can retry a confirmation call if it doesn't get
    // a timely 200 response. TransID is Safaricom's own unique transaction
    // ID, so using it as the doc ID makes a retry a harmless overwrite
    // instead of double-counting the payment.
    if (transId) {
      await db.collection('mpesaTransactions').doc(transId).set({
        transId, shortCode, branch, amount, phone, name, date, transTime,
        raw: p,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    }

    logger.info(`M-Pesa confirmed: KSh ${amount} to till ${shortCode} (${branch || 'unmatched branch'}) on ${date}`);
    res.status(200).json({ ResultCode: 0, ResultDesc: 'Success' });
  } catch (e) {
    logger.error('mpesaConfirmation error:', e);
    // Still return 200 — Safaricom will keep retrying an error response,
    // which would create duplicate-processing noise without fixing anything.
    res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }
});
