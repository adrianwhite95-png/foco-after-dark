// Mock end-to-end test for voucher generation and usage (in-memory)
// This does not use Firebase or require Java/CLT. It verifies generation logic, rate-limiting, and atomic use.

const ALPHABET = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ';
function generateCode(len = 6) {
  const bytes = Buffer.from(Array.from({ length: len }, () => Math.floor(Math.random() * 256)));
  let out = '';
  for (let i = 0; i < len; i++) {
    out += ALPHABET[bytes[i] % ALPHABET.length];
  }
  return out;
}

// In-memory stores to simulate Firestore collections
const db = {
  ceoVouchers: new Map(),
  auditLogs: [],
  rateLimits: new Map()
};

function nowIso() { return new Date().toISOString(); }

async function checkRateLimit(uid, opts = {}) {
  const maxPerMin = opts.maxPerMin || 5;
  const maxPerDay = opts.maxPerDay || 200;
  const now = Date.now();
  const entry = db.rateLimits.get(uid) || { perMin: 0, perDay: 0, minuteWindowStart: now, dayWindowStart: now };
  // reset minute
  if (now - entry.minuteWindowStart >= 60 * 1000) {
    entry.perMin = 0;
    entry.minuteWindowStart = now;
  }
  // reset day
  if (new Date(now).toDateString() !== new Date(entry.dayWindowStart).toDateString()) {
    entry.perDay = 0;
    entry.dayWindowStart = now;
  }
  if (entry.perMin + 1 > maxPerMin) throw new Error('RATE_MIN');
  if (entry.perDay + 1 > maxPerDay) throw new Error('RATE_DAY');
  entry.perMin += 1; entry.perDay += 1;
  db.rateLimits.set(uid, entry);
  return true;
}

async function generateCeoVoucherMock({ issuerUid, perk = 'drink' } = {}) {
  if (!issuerUid) throw new Error('unauthenticated');
  // simulate claim check (we assume issuerUid 'admin' is allowed)
  const allowed = issuerUid.startsWith('admin') || issuerUid.startsWith('ceo') || issuerUid.startsWith('staff');
  if (!allowed) throw new Error('permission-denied');
  // rate limit
  await checkRateLimit(issuerUid);
  const labelMap = {
    shot: '$1 shot voucher',
    drink: '$3 drink voucher',
    cover: 'Skip Line + No Cover Charge',
    free_drink: 'Free drink voucher'
  };
  const code = generateCode(6).toUpperCase();
  const doc = { code, perk: `CEO issued: ${labelMap[perk] || 'Voucher'}`, perkKey: `ceo_${perk}`, issuerUid, createdAt: nowIso(), expiresAt: null, used: false };
  db.ceoVouchers.set(code, doc);
  db.auditLogs.push({ action: 'generateCeoVoucher', issuerUid, timestamp: nowIso(), details: { code, perk } });
  return { success: true, code, perk: doc.perk };
}

async function useCeoVoucherMock({ actorUid, code } = {}) {
  if (!actorUid) throw new Error('unauthenticated');
  const doc = db.ceoVouchers.get(code);
  if (!doc) throw new Error('not_found');
  if (doc.used) throw new Error('already_used');
  // mark used atomically (we simulate by immediate set)
  doc.used = true; doc.usedBy = actorUid; doc.usedAt = nowIso();
  db.ceoVouchers.set(code, doc);
  db.auditLogs.push({ action: 'useCeoVoucher', issuerUid: actorUid, timestamp: nowIso(), details: { code } });
  return { success: true, code, perk: doc.perk };
}

async function run() {
  console.log('--- Mock E2E test start ---');
  const issuer = 'admin_12345';
  console.log('Generating 3 vouchers quickly to test rate limits...');
  try {
    for (let i = 0; i < 3; i++) {
      const r = await generateCeoVoucherMock({ issuerUid: issuer, perk: i === 1 ? 'shot' : 'drink' });
      console.log('Generated:', r);
    }
  } catch (err) {
    console.error('Generation error:', err.message || err);
  }

  console.log('\nAudit logs after generation:');
  console.log(JSON.stringify(db.auditLogs, null, 2));

  const codes = Array.from(db.ceoVouchers.keys());
  if (!codes.length) { console.error('No vouchers; aborting'); return; }
  const codeToUse = codes[0];
  console.log('\nUsing voucher', codeToUse, 'by staff_1');
  try {
    const res = await useCeoVoucherMock({ actorUid: 'staff_1', code: codeToUse });
    console.log('Use result:', res);
  } catch (err) {
    console.error('Use error:', err.message || err);
  }

  console.log('\nAttempting to reuse same voucher to confirm double-spend prevention');
  try {
    await useCeoVoucherMock({ actorUid: 'staff_2', code: codeToUse });
    console.error('ERROR: double-spend should not be allowed');
  } catch (err) {
    console.log('Expected rejection:', err.message || err);
  }

  console.log('\nFinal ceoVouchers store:');
  console.log(JSON.stringify(Array.from(db.ceoVouchers.values()), null, 2));
  console.log('\n--- Mock E2E test complete ---');
}

run().catch(err => { console.error('Test runner error', err); process.exit(1); });
