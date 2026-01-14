const express = require('express');
const bodyParser = require('body-parser');
const admin = require('firebase-admin');
const crypto = require('crypto');
const helmet = require('helmet');
const cors = require('cors');

// Initialize Admin SDK using service account JSON passed via env SERVICE_ACCOUNT_KEY
if (!process.env.SERVICE_ACCOUNT_KEY) {
  console.error('Missing SERVICE_ACCOUNT_KEY env var (service account JSON string)');
  process.exit(1);
}
let serviceAccount = null;
try {
  serviceAccount = JSON.parse(process.env.SERVICE_ACCOUNT_KEY);
} catch (err) {
  console.error('Failed to parse SERVICE_ACCOUNT_KEY as JSON', err);
  process.exit(2);
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  projectId: process.env.FIREBASE_PROJECT_ID || serviceAccount.project_id
});

const db = admin.firestore();
const app = express();
app.use(helmet());
app.use(cors());
app.use(bodyParser.json());

const ALPHABET = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ';
function generateCode(len = 6) {
  const bytes = crypto.randomBytes(len);
  let out = '';
  for (let i = 0; i < len; i++) {
    out += ALPHABET[bytes[i] % ALPHABET.length];
  }
  return out;
}

async function verifyIdToken(req, res, next) {
  const auth = req.headers.authorization || '';
  const match = auth.match(/^Bearer\s+(.+)$/i);
  if (!match) return res.status(401).json({ error: 'Missing Authorization Bearer token' });
  const token = match[1];
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    req.user = decoded;
    next();
  } catch (err) {
    console.warn('Token verify failed', err);
    return res.status(401).json({ error: 'Invalid token' });
  }
}

// Simple rate limit per issuer stored in Firestore (similar to functions implementation)
async function checkRateLimit(uid) {
  const maxPerMin = parseInt(process.env.CEO_MAX_PER_MIN || '5', 10);
  const maxPerDay = parseInt(process.env.CEO_MAX_PER_DAY || '200', 10);
  const now = admin.firestore.Timestamp.now();
  const ref = db.collection('rateLimits').doc(uid);
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const data = snap.exists ? snap.data() : { perMin: 0, perDay: 0, minuteWindowStart: now, dayWindowStart: now };
    let perMin = data.perMin || 0;
    let perDay = data.perDay || 0;
    const minuteWindowStart = data.minuteWindowStart || now;
    const dayWindowStart = data.dayWindowStart || now;
    if (now.seconds - minuteWindowStart.seconds >= 60) {
      perMin = 0;
    }
    if (now.toDate().toDateString() !== new Date(dayWindowStart.toDate()).toDateString()) {
      perDay = 0;
    }
    if (perMin + 1 > maxPerMin) {
      const e = new Error('Rate limit per minute exceeded');
      e.code = 'RATE_MIN';
      throw e;
    }
    if (perDay + 1 > maxPerDay) {
      const e = new Error('Rate limit per day exceeded');
      e.code = 'RATE_DAY';
      throw e;
    }
    tx.set(ref, { perMin: perMin + 1, perDay: perDay + 1, minuteWindowStart, dayWindowStart }, { merge: true });
    return true;
  });
}

app.post('/generateCeoVoucher', verifyIdToken, async (req, res) => {
  const user = req.user;
  if (!user) return res.status(401).json({ error: 'Auth required' });
  const claims = user || {};
  if (!claims.admin && !claims.ceo && !claims.staff) {
    return res.status(403).json({ error: 'Requires admin/ceo/staff privileges' });
  }
  try {
    await checkRateLimit(user.uid);
  } catch (err) {
    return res.status(429).json({ error: err.message || 'Rate limit' });
  }
  const perk = String((req.body && req.body.perk) || 'drink');
  const labelMap = {
    shot: '$1 shot voucher',
    drink: '$3 drink voucher',
    cover: 'Skip Line + No Cover Charge',
    free_drink: 'Free drink voucher'
  };
  const code = generateCode(6).toUpperCase();
  const doc = {
    code,
    perk: `CEO issued: ${labelMap[perk] || 'Voucher'}`,
    perkKey: `ceo_${perk}`,
    issuerUid: user.uid,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    expiresAt: null,
    used: false
  };
  try {
    await db.collection('ceoVouchers').doc(code).set(doc);
    await db.collection('auditLogs').add({ action: 'generateCeoVoucher', issuerUid: user.uid, timestamp: admin.firestore.FieldValue.serverTimestamp(), details: { code, perk } });
    return res.json({ success: true, code, perk: doc.perk });
  } catch (err) {
    console.error('generate error', err);
    return res.status(500).json({ error: 'Failed to create voucher' });
  }
});

app.post('/useCeoVoucher', verifyIdToken, async (req, res) => {
  const user = req.user;
  if (!user) return res.status(401).json({ error: 'Auth required' });
  const code = String((req.body && req.body.code) || '').toUpperCase();
  if (!code) return res.status(400).json({ error: 'Missing code' });
  const ref = db.collection('ceoVouchers').doc(code);
  try {
    const result = await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) throw new Error('not_found');
      const val = snap.data();
      if (val.used) throw new Error('already_used');
      tx.update(ref, { used: true, usedBy: user.uid, usedAt: admin.firestore.FieldValue.serverTimestamp() });
      await db.collection('auditLogs').add({ action: 'useCeoVoucher', issuerUid: user.uid, timestamp: admin.firestore.FieldValue.serverTimestamp(), details: { code } });
      return { success: true, code, perk: val.perk };
    });
    return res.json(result);
  } catch (err) {
    if (err.message === 'not_found') return res.status(404).json({ error: 'Voucher not found' });
    if (err.message === 'already_used') return res.status(409).json({ error: 'Voucher already used' });
    console.error('use error', err);
    return res.status(500).json({ error: 'Failed to use voucher' });
  }
});

const PORT = process.env.PORT || 8787;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
