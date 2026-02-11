/* Cloud Functions for FoCo After Dark
 * - generateCeoVoucher: callable, requires admin/ceo custom claim, creates voucher document
 * - useCeoVoucher: callable, marks voucher used in a transaction to prevent double-spend
 * - initUserProfile: auth trigger to seed member profile & username map
 * - reserveUsername: callable to atomically claim a username
 * - awardPoints: callable to server-trust points adjustments
 * - spinNightWheel: callable server-side spin with allowance + points
 * - registerPushToken/sendPush: push notification helpers
 * - nightlyCloseOut: scheduled 3am MT summary + cleanup
 * - weeklyCleanup: scheduled cleanup of old audit/log docs
 */
const functions = require('firebase-functions/v1'); // v1 for auth/pubsub legacy
const { HttpsError } = functions.https;
const admin = require('firebase-admin');
const { Timestamp } = require('firebase-admin/firestore');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const CEO_PASS_ID = "DREE4695";
const CEO_EMAIL = "ceo@gmail.com";
const CEO_UID = "ceo_master";
const BETA_UID = "foco-beta-demo";
const BETA_EMAIL = "beta@focoafterdark.com";
const BETA_USERNAME = "focobeta";
const STAFF_VENUES = {
  bar_district: { name: "The Bar District", login: "district" },
  yeti: { name: "Yeti Bar & Grill", login: "yeti" },
  rec_room: { name: "Rec Room Fort Collins", login: "rec" },
  bondi_beach: { name: "Bondi Beach Bar & Grill", login: "bondi" },
  surfside: { name: "Surfside", login: "surfside" },
  lucky_joes: { name: "Lucky Joe's Sidewalk Saloon", login: "joes" },
  trail_head: { name: "Trail Head Tavern", login: "trail" },
  steak_out: { name: "Steak-Out Saloon", login: "steak" },
  road_34: { name: "Road 34 Bike Bar", login: "road34" },
  brothers: { name: "Brothers", login: "brothers" },
  pour_brothers: { name: "Pour Brothers Community Tavern", login: "pour" },
  tap_handle: { name: "Tap and Handle", login: "tap" },
  high_point: { name: "High Point", login: "highpoint" },
  pinball_jones: { name: "Pinball Jones", login: "pinball" },
  elliotts: { name: "Elliott's Martini Bar", login: "elliotts" },
  town_pump: { name: "Town Pump", login: "townpump" },
  mayor_old_town: { name: "The Mayor of Old Town", login: "mayor" },
};
const STAFF_VENUE_ALIASES = {
  district: "bar_district",
  "the bar district": "bar_district",
  "bar district": "bar_district",
  yeti: "yeti",
  "yeti bar": "yeti",
  "yeti bar & grill": "yeti",
  rec: "rec_room",
  recroom: "rec_room",
  "rec room": "rec_room",
  "rec room fort collins": "rec_room",
  bondi: "bondi_beach",
  "bondi beach": "bondi_beach",
  "bondi beach bar": "bondi_beach",
  "bondi beach bar & grill": "bondi_beach",
  surfside: "surfside",
  "surf side": "surfside",
  joes: "lucky_joes",
  "lucky joes": "lucky_joes",
  "lucky joe's sidewalk saloon": "lucky_joes",
  trail: "trail_head",
  "trail head": "trail_head",
  trailhead: "trail_head",
  "trail head tavern": "trail_head",
  steak: "steak_out",
  "steak out": "steak_out",
  "steak-out": "steak_out",
  "steak-out saloon": "steak_out",
  road34: "road_34",
  road: "road_34",
  "road 34": "road_34",
  "road 34 bike bar": "road_34",
  brothers: "brothers",
  "brothers bar": "brothers",
  "brothers bar & grill": "brothers",
  pour: "pour_brothers",
  "pour brothers": "pour_brothers",
  "pour brothers community tavern": "pour_brothers",
  tap: "tap_handle",
  "tap and handle": "tap_handle",
  highpoint: "high_point",
  "high point": "high_point",
  pinball: "pinball_jones",
  "pinball jones": "pinball_jones",
  elliott: "elliotts",
  elliotts: "elliotts",
  "elliott's": "elliotts",
  "elliott’s": "elliotts",
  townpump: "town_pump",
  "town pump": "town_pump",
  mayor: "mayor_old_town",
  "the mayor": "mayor_old_town",
  "mayor of old town": "mayor_old_town",
  "the mayor of old town": "mayor_old_town"
};
Object.entries(STAFF_VENUES).forEach(([id, info]) => {
  if (info && info.login) {
    STAFF_VENUE_ALIASES[info.login] = id;
  }
});
let stripeClient = null;
const REPORTS_TO_EMAIL = "focoafterdark@gmail.com";
const reportEmailSecrets = { secrets: ["REPORTS_SMTP_USER", "REPORTS_SMTP_PASS"] };
const betaLoginSecrets = { secrets: ["BETA_LOGIN_CODE"] };
const staffLoginSecrets = { secrets: ["STAFF_GATE_CODE"] };
const ceoLoginSecrets = { secrets: ["CEO_LOGIN_CODE"] };
const appLockSecrets = { secrets: ["APP_LOCK_CODE"] };
const ceoAccessSecrets = { secrets: ["CEO_ACCESS_PASSWORD"] };

admin.initializeApp();
const db = admin.firestore();
const messaging = admin.messaging();

function getReportTransporter() {
  const user = process.env.REPORTS_SMTP_USER;
  const pass = process.env.REPORTS_SMTP_PASS;
  if (!user || !pass) return null;
  const host = process.env.REPORTS_SMTP_HOST || "smtp.gmail.com";
  const port = Number(process.env.REPORTS_SMTP_PORT || 465);
  const secure = port === 465;
  return nodemailer.createTransport({
    host,
    port,
    secure,
    auth: { user, pass }
  });
}

async function sendReportEmail(subject, text, opts = {}) {
  const transporter = getReportTransporter();
  if (!transporter) {
    return { sent: false, error: "SMTP not configured" };
  }
  const from = process.env.REPORTS_SMTP_FROM || process.env.REPORTS_SMTP_USER || "reports@focoafterdark.com";
  const to = opts.to || REPORTS_TO_EMAIL;
  try {
    await transporter.sendMail({
      from: `FoCo After Dark <${from}>`,
      to,
      subject,
      text
    });
    return { sent: true };
  } catch (err) {
    return { sent: false, error: err?.message || "SMTP send failed" };
  }
}

const ALPHABET = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ';
function generateCode(len = 6) {
  const bytes = crypto.randomBytes(len);
  let out = '';
  for (let i = 0; i < len; i++) {
    out += ALPHABET[bytes[i] % ALPHABET.length];
  }
  return out;
}

async function ensureUniquePassCode(attempts = 5) {
  for (let i = 0; i < attempts; i++) {
    const code = generateCode(6).toUpperCase();
    const snap = await db.collection('members').where('passCode', '==', code).limit(1).get();
    if (snap.empty) return code;
  }
  return `FD${Date.now().toString(36).toUpperCase()}`;
}

function resolveStaffVenueId(input) {
  const norm = (input || "").trim().toLowerCase();
  if (!norm) return "";
  if (STAFF_VENUE_ALIASES[norm]) return STAFF_VENUE_ALIASES[norm];
  if (STAFF_VENUES[norm]) return norm;
  const found = Object.entries(STAFF_VENUES).find(([, info]) => {
    const name = (info.name || "").toLowerCase();
    return name === norm || name.includes(norm);
  });
  return found ? found[0] : norm;
}

function getStaffVenueName(venueId) {
  return (STAFF_VENUES[venueId]?.name || venueId || "Venue").toString();
}

function getStaffVenueLoginCode(venueId) {
  return (STAFF_VENUES[venueId]?.login || venueId || "").toString();
}

// Helper: enforce simple per-issuer rate limits to reduce abuse
async function checkRateLimit(uid, opts = {}) {
  const maxPerMin = opts.maxPerMin || 5;
  const maxPerDay = opts.maxPerDay || 200;
  const now = admin.firestore.Timestamp.now();
  const ref = db.collection('rateLimits').doc(uid);
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const data = snap.exists ? snap.data() : { perMin: 0, perDay: 0, minuteWindowStart: now, dayWindowStart: now };
    const minuteWindowStart = data.minuteWindowStart || now;
    const dayWindowStart = data.dayWindowStart || now;
    let perMin = data.perMin || 0;
    let perDay = data.perDay || 0;
    // Reset minute window if older than 60s
    if (now.seconds - minuteWindowStart.seconds >= 60) {
      perMin = 0;
      tx.set(ref, { minuteWindowStart: now }, { merge: true });
    }
    // Reset day window if a day passed
    if (now.toDate().toDateString() !== new Date(dayWindowStart.toDate()).toDateString()) {
      perDay = 0;
      tx.set(ref, { dayWindowStart: now }, { merge: true });
    }
    if (perMin + 1 > maxPerMin) {
      throw new HttpsError('resource-exhausted', 'Rate limit exceeded (per minute)');
    }
    if (perDay + 1 > maxPerDay) {
      throw new HttpsError('resource-exhausted', 'Rate limit exceeded (per day)');
    }
    // increment counters
    tx.set(ref, { perMin: perMin + 1, perDay: perDay + 1, minuteWindowStart: minuteWindowStart, dayWindowStart: dayWindowStart }, { merge: true });
    return true;
  });
}

function getRequestIp(context) {
  const raw = context?.rawRequest;
  if (!raw) return "";
  const forwarded = raw.headers?.["x-forwarded-for"];
  if (forwarded) return String(forwarded).split(",")[0].trim();
  return String(raw.ip || "");
}

function hashLookupKey(input) {
  return crypto.createHash("sha256").update(String(input || "")).digest("hex");
}

async function enforceLookupRateLimit({ key, limit = 30, windowMs = 10 * 60 * 1000 }) {
  if (!key) return;
  const ref = db.collection("rateLimits").doc(key);
  const now = Date.now();
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const data = snap.exists ? snap.data() : {};
    const windowStart = Number(data.windowStart || 0);
    const count = Number(data.count || 0);
    const isSameWindow = windowStart && (now - windowStart) < windowMs;
    const nextCount = isSameWindow ? count + 1 : 1;
    const nextWindow = isSameWindow ? windowStart : now;
    if (nextCount > limit) {
      throw new HttpsError("resource-exhausted", "Too many lookup attempts. Try again soon.");
    }
    tx.set(ref, {
      windowStart: nextWindow,
      count: nextCount,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  });
}

exports.generateCeoVoucher = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new HttpsError('unauthenticated', 'Authentication required');
  }
  const claims = context.auth.token || {};
  if (!claims.admin && !claims.ceo) {
    throw new HttpsError('permission-denied', 'Requires admin/CEO privileges');
  }
  // Rate limit check per issuer UID
  const issuerUid = context.auth.uid || 'unknown';
  try {
    await checkRateLimit(issuerUid);
  } catch (rlErr) {
    console.warn('Rate limit check failed for', issuerUid, rlErr);
    throw rlErr;
  }
  const perk = (data && data.perk) ? String(data.perk) : 'drink';
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
    issuerUid: context.auth.uid || null,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    expiresAt: null,
    used: false
  };
  try {
    await db.collection('ceoVouchers').doc(code).set(doc);
    await db.collection('auditLogs').add({
      action: 'generateCeoVoucher',
      issuerUid: context.auth.uid || null,
      timestamp: admin.firestore.FieldValue.serverTimestamp(),
      details: { code, perk }
    });
    return { success: true, code, perk: doc.perk };
  } catch (err) {
    console.error('generateCeoVoucher error', err);
    throw new HttpsError('internal', 'Failed to create voucher');
  }
});

exports.useCeoVoucher = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new HttpsError('unauthenticated', 'Authentication required');
  }
  const code = (data && data.code) ? String(data.code).toUpperCase() : '';
  if (!code) {
    throw new HttpsError('invalid-argument', 'Missing code');
  }
  const ref = db.collection('ceoVouchers').doc(code);
  try {
    const result = await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) {
        throw new HttpsError('not-found', 'Voucher not found');
      }
      const val = snap.data();
      if (val.used) {
        throw new HttpsError('failed-precondition', 'Voucher already used');
      }
      tx.update(ref, { used: true, usedBy: context.auth.uid || null, usedAt: admin.firestore.FieldValue.serverTimestamp() });
      await db.collection('auditLogs').add({
        action: 'useCeoVoucher',
        issuerUid: context.auth.uid || null,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        details: { code }
      });
      return { success: true, code, perk: val.perk };
    });
    return result;
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.error('useCeoVoucher transaction error', err);
    throw new HttpsError('internal', 'Transaction failed');
  }
});

async function resolveMemberByPassCode(passCode, tx, fallbackUid = null) {
  const passRef = db.collection("passes").doc(passCode);
  const passSnap = await tx.get(passRef);
  const passData = passSnap.exists ? passSnap.data() : null;
  let uid = passData?.uid || null;
  let memberRef = null;
  let memberData = null;
  let passWrite = null;
  let memberPassUpdate = null;

  if (uid) {
    memberRef = db.collection("members").doc(uid);
    const memberSnap = await tx.get(memberRef);
    memberData = memberSnap.exists ? memberSnap.data() : null;
    return {
      uid,
      passRef,
      memberRef,
      memberData,
      passData,
      passWrite: null,
      memberPassUpdate: null
    };
  }

  const memberQuery = await tx.get(
    db.collection("members").where("passCode", "==", passCode).limit(1)
  );
  if (memberQuery.empty) {
    if (!fallbackUid) return null;
    memberRef = db.collection("members").doc(fallbackUid);
    const memberSnap = await tx.get(memberRef);
    if (!memberSnap.exists) return null;
    memberData = memberSnap.data() || {};
    const existingPass = (memberData.passCode || "").toString().toUpperCase();
    const isBetaMember =
      (memberData.tier || "").toString().toLowerCase() === "beta" ||
      (memberData.membershipTier || "").toString().toLowerCase() === "beta" ||
      (memberData.email || "").toString().toLowerCase() === BETA_EMAIL ||
      (memberData.email || "").toString().toLowerCase().includes("beta") ||
      (memberData.membershipOverride || "").toString().toUpperCase().includes("BETA") ||
      memberData.beta === true ||
      memberData.isBeta === true ||
      fallbackUid === BETA_UID;
    if (existingPass && existingPass !== passCode && !isBetaMember) {
      throw new HttpsError("permission-denied", "Pass ID does not match signed-in user.");
    }
    const finalPass = existingPass || passCode;
    if (!existingPass && finalPass) {
      memberPassUpdate = { passCode: finalPass, updatedAt: admin.firestore.FieldValue.serverTimestamp() };
    }
    passWrite = {
      ref: passRef,
      data: {
        uid: fallbackUid,
        passCode: finalPass,
        tier: memberData.tier || "standard",
        status: memberData.revoked ? "revoked" : "active",
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      }
    };
    return {
      uid: fallbackUid,
      passRef,
      memberRef,
      memberData: { ...memberData, passCode: finalPass },
      passData: null,
      passWrite,
      memberPassUpdate
    };
  }

  const memberDoc = memberQuery.docs[0];
  uid = memberDoc.id;
  memberRef = memberDoc.ref;
  memberData = memberDoc.data() || {};
  passWrite = {
    ref: passRef,
    data: {
      uid,
      passCode,
      tier: memberData.tier || "standard",
      status: memberData.revoked ? "revoked" : "active",
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }
  };
  return {
    uid,
    passRef,
    memberRef,
    memberData,
    passData: null,
    passWrite,
    memberPassUpdate: null
  };
}

function toMillis(value) {
  if (!value) return null;
  if (typeof value.toMillis === "function") return value.toMillis();
  const parsed = Date.parse(value);
  if (!Number.isNaN(parsed)) return parsed;
  return null;
}

const MAX_VOUCHER_CARRYOVER = 3;
const VOUCHER_LIMITS = { standard: 5, vip: 10 };
const PENDING_REDEMPTION_TIMEOUT_MS = 60 * 60 * 1000;

function toDateSafe(value) {
  if (!value) return null;
  if (typeof value.toDate === "function") {
    const parsed = value.toDate();
    return Number.isNaN(parsed?.getTime?.()) ? null : parsed;
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function oneMonthFrom(fromDate = new Date()) {
  const d = new Date(fromDate);
  const day = d.getDate();
  d.setMonth(d.getMonth() + 1);
  if (d.getDate() < day) {
    d.setDate(0);
  }
  d.setHours(23, 59, 59, 999);
  return d;
}

function shiftMonth(baseDate, delta) {
  const d = new Date(baseDate);
  const day = d.getDate();
  d.setMonth(d.getMonth() + delta);
  if (d.getDate() < day) {
    d.setDate(0);
  }
  return d;
}

function countBillingCycles(startIso, now = new Date()) {
  if (!startIso) return 0;
  const start = new Date(startIso);
  if (Number.isNaN(start.getTime())) return 0;
  let months = (now.getFullYear() - start.getFullYear()) * 12 + (now.getMonth() - start.getMonth());
  if (now.getDate() < start.getDate()) {
    months -= 1;
  }
  return Math.max(1, months + 1);
}

function resolveBillingStartIsoForWallet(memberData = {}, billing = {}) {
  const start = billing?.start || billing?.lastCharge;
  if (start) return start;
  if (billing?.nextRenewal) {
    const guess = new Date(billing.nextRenewal);
    if (!Number.isNaN(guess.getTime())) {
      guess.setMonth(guess.getMonth() - 1);
      return guess.toISOString();
    }
  }
  return memberData?.memberSince || null;
}

function getVoucherCycleWindowForWallet(memberData = {}, billing = {}, now = new Date()) {
  const startIso = resolveBillingStartIsoForWallet(memberData, billing);
  let startDate = startIso ? new Date(startIso) : null;
  if (!startDate || Number.isNaN(startDate.getTime())) {
    startDate = new Date(now);
    startDate.setDate(1);
    startDate.setHours(0, 0, 0, 0);
  }
  const cycles = countBillingCycles(startDate.toISOString(), now);
  const cycleStart = shiftMonth(startDate, Math.max(0, cycles - 1));
  const cycleEnd = oneMonthFrom(cycleStart);
  const hasPrevious = cycles > 1;
  const prevStart = hasPrevious ? shiftMonth(startDate, cycles - 2) : shiftMonth(cycleStart, -1);
  const prevEnd = cycleStart;
  return {
    current: { start: cycleStart, end: cycleEnd },
    previous: { start: prevStart, end: prevEnd },
    hasPrevious,
    startDate,
    cycles
  };
}

function getMonthTokenFromDate(date = new Date()) {
  const parsed = date instanceof Date ? date : new Date(date);
  if (Number.isNaN(parsed.getTime())) return getMonthTokenFromDate(new Date());
  const m = String(parsed.getMonth() + 1).padStart(2, "0");
  return `${parsed.getFullYear()}-${m}`;
}

function getVoucherLimitForTierForWallet(tier) {
  if (!tier) return 0;
  if (tier === "ceo" || tier === "free") return Infinity;
  return VOUCHER_LIMITS[tier] || VOUCHER_LIMITS.standard;
}

function normalizeVoucherTierForWallet(memberData = {}) {
  const overrideRaw =
    memberData?.membershipOverride ||
    memberData?.override ||
    memberData?.membership_override ||
    memberData?.membershipTierOverride ||
    "";
  const override = String(overrideRaw).toUpperCase();
  if (memberData?.ceo === true || override === "CEO") return "ceo";
  if (memberData?.freeMembership === true || override === "CEO_FREE") return "free";
  const tier = String(memberData?.tier || memberData?.membershipTier || "").toLowerCase();
  if (tier === "ceo") return "ceo";
  if (tier === "free") return "free";
  if (tier === "vip") return "vip";
  return "standard";
}

function isCountedRedemptionStatus(status, includePending = false) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "verified" || normalized === "approved" || normalized === "confirmed") return true;
  if (includePending && (normalized === "pending" || normalized === "requested")) return true;
  return false;
}

function getRedemptionEntryDate(entry = {}) {
  return toDateSafe(entry?.verifiedAt || entry?.timestamp || entry?.createdAt || null);
}

function isPendingRedemptionFresh(entry, now = new Date(), maxMs = PENDING_REDEMPTION_TIMEOUT_MS) {
  const stamp = getRedemptionEntryDate(entry);
  if (!stamp) return false;
  return (now.getTime() - stamp.getTime()) <= maxMs;
}

function countRedemptionsInWindow(entries = [], window, options = {}) {
  if (!window) return 0;
  const includePending = Boolean(options.includePending);
  const now = options.now instanceof Date ? options.now : new Date();
  const pendingMaxMs = Number(options.pendingMaxMs || PENDING_REDEMPTION_TIMEOUT_MS);
  const start = window.start;
  const end = window.end;
  return entries.reduce((count, entry) => {
    const status = String(entry?.status || "").toLowerCase();
    if (status === "pending" || status === "requested") {
      if (!includePending) return count;
      if (!isPendingRedemptionFresh(entry, now, pendingMaxMs)) return count;
    } else if (!isCountedRedemptionStatus(status, false)) {
      return count;
    }
    const stamp = getRedemptionEntryDate(entry);
    if (!stamp) return count;
    if (stamp >= start && stamp <= end) return count + 1;
    return count;
  }, 0);
}

function resolveLatestSuccessfulPaymentDate(billing = {}, memberData = {}) {
  const candidates = [
    billing?.lastCharge,
    billing?.lastPaidAt,
    billing?.lastPaymentAt,
    billing?.paymentDate,
    memberData?.lastCharge,
    memberData?.lastChargeAt,
    memberData?.lastPaidAt,
    memberData?.lastPaymentAt
  ].map(toDateSafe).filter(Boolean);
  if (!candidates.length) return null;
  return new Date(Math.max(...candidates.map((d) => d.getTime())));
}

function getPointsMonthlyTokenGrantTotal(memberData = {}, now = new Date()) {
  const grants = memberData?.pointsMonthlyTokenGrants;
  if (!grants || typeof grants !== "object") return 0;
  const key = getMonthTokenFromDate(now);
  const value = Number(grants[key] || 0);
  return Number.isFinite(value) ? Math.max(0, value) : 0;
}

function hasSuccessfulPaymentThisCycleForWallet(tier, billing = {}, memberData = {}, now = new Date()) {
  if (tier !== "standard" && tier !== "vip") return true;
  const status = String(memberData?.paymentStatus || "active").toLowerCase();
  if (status !== "active") return false;
  const paidAt = resolveLatestSuccessfulPaymentDate(billing, memberData);
  if (!paidAt) return false;
  const cycle = getVoucherCycleWindowForWallet(memberData, billing, now)?.current;
  if (!cycle?.start || !cycle?.end) return false;
  return paidAt >= cycle.start && paidAt <= cycle.end;
}

function computeVoucherBalanceForWallet(memberData = {}, entries = [], now = new Date(), unlimited = false) {
  if (unlimited) {
    return { unlimited: true, remaining: Infinity, paymentRequired: false };
  }
  const tier = normalizeVoucherTierForWallet(memberData);
  const limit = getVoucherLimitForTierForWallet(tier);
  if (!Number.isFinite(limit)) {
    return { unlimited: true, remaining: Infinity, paymentRequired: false };
  }
  const billing = (memberData?.billing && typeof memberData.billing === "object") ? memberData.billing : {};
  if (!hasSuccessfulPaymentThisCycleForWallet(tier, billing, memberData, now)) {
    return {
      unlimited: false,
      remaining: 0,
      limit,
      carryover: 0,
      usedCurrent: 0,
      total: 0,
      extraTokens: 0,
      paymentRequired: true
    };
  }

  const startIso = resolveBillingStartIsoForWallet(memberData, billing);
  let startDate = startIso ? new Date(startIso) : null;
  if (!startDate || Number.isNaN(startDate.getTime())) {
    startDate = new Date(now);
    startDate.setDate(1);
    startDate.setHours(0, 0, 0, 0);
  }
  const cycles = countBillingCycles(startDate.toISOString(), now);
  const purchasedTotal = Math.max(0, Number(memberData?.extraRedemptionTokens || 0));
  const grantKey = getMonthTokenFromDate(now);
  const monthlyGrantRaw = memberData?.ceoMonthlyTokenGrants && typeof memberData.ceoMonthlyTokenGrants === "object"
    ? memberData.ceoMonthlyTokenGrants[grantKey]
    : 0;
  const ceoMonthlyGrant = Math.max(0, Number(monthlyGrantRaw || 0));
  const pointsMonthlyGrant = getPointsMonthlyTokenGrantTotal(memberData, now);
  const monthlyGrant = ceoMonthlyGrant + pointsMonthlyGrant;
  let purchasedRemaining = purchasedTotal;
  let carryover = 0;
  let result = {
    unlimited: false,
    remaining: 0,
    limit,
    carryover: 0,
    usedCurrent: 0,
    total: 0,
    extraTokens: 0,
    paymentRequired: false
  };
  for (let i = 0; i < cycles; i += 1) {
    const cycleStart = shiftMonth(startDate, i);
    const cycleEnd = oneMonthFrom(cycleStart);
    const isCurrent = i === cycles - 1;
    const cycleUsed = countRedemptionsInWindow(
      entries,
      { start: cycleStart, end: cycleEnd },
      { includePending: isCurrent, now, pendingMaxMs: PENDING_REDEMPTION_TIMEOUT_MS }
    );
    const regularAllowance = limit + carryover;
    const regularAllowanceWithGrant = regularAllowance + (isCurrent ? monthlyGrant : 0);
    const consumedTokens = Math.max(0, cycleUsed - regularAllowanceWithGrant);
    const purchasedAfter = Math.max(0, purchasedRemaining - consumedTokens);
    const regularUnused = Math.max(0, regularAllowance - cycleUsed);
    const nextCarryover = purchasedAfter > 0 ? 0 : Math.min(MAX_VOUCHER_CARRYOVER, regularUnused);
    if (isCurrent) {
      const remainingRegular = Math.max(0, regularAllowanceWithGrant - cycleUsed);
      const remaining = remainingRegular + purchasedAfter;
      result = {
        unlimited: false,
        remaining,
        limit,
        carryover,
        usedCurrent: cycleUsed,
        total: regularAllowanceWithGrant + purchasedRemaining,
        extraTokens: purchasedAfter,
        paymentRequired: false
      };
    }
    purchasedRemaining = purchasedAfter;
    carryover = nextCarryover;
  }
  return result;
}

function parsePerkQuantity(value) {
  const qty = Number(value);
  if (!Number.isFinite(qty) || qty <= 0) return null;
  return Math.round(qty);
}

function getTierPerkQuantity(perkData = {}, tier = "standard") {
  const standardQty = parsePerkQuantity(
    perkData?.standardQty ?? perkData?.standard ?? perkData?.standardCount ?? null
  );
  const vipQty = parsePerkQuantity(
    perkData?.vipQty ?? perkData?.vip ?? perkData?.vipCount ?? null
  );
  const legacyLimit = parsePerkQuantity(
    perkData?.limit ?? perkData?.qty ?? perkData?.quantity ?? null
  );
  let resolvedStandard = standardQty || legacyLimit || null;
  let resolvedVip = vipQty || legacyLimit || null;
  if (resolvedStandard && !resolvedVip) resolvedVip = resolvedStandard;
  if (resolvedVip && !resolvedStandard) resolvedStandard = resolvedVip;
  if (tier === "vip") return resolvedVip || 0;
  if (tier === "standard") return resolvedStandard || 0;
  return Infinity;
}

function getVenuePerkUsageCountForWallet(entries = [], perkId = "", venueId = "") {
  if (!perkId) return 0;
  const normalizedPerkId = String(perkId);
  const normalizedVenueId = String(venueId || "").toLowerCase();
  let used = 0;
  entries.forEach((entry = {}) => {
    const entryPerkId = entry?.venuePerkId || entry?.perkId || entry?.perkKey || "";
    if (!entryPerkId || String(entryPerkId) !== normalizedPerkId) return;
    const entryVenue = String(entry?.requestedVenue || entry?.venue || "").toLowerCase();
    if (normalizedVenueId && entryVenue && entryVenue !== normalizedVenueId) return;
    const status = String(entry?.status || "").toLowerCase();
    if (status === "pending" || status === "verified" || status === "approved" || status === "confirmed") {
      used += 1;
    }
  });
  return used;
}

exports.createRedemption = functions.https.onCall(async (data, context) => {
  try {
    if (!context.auth) {
      throw new HttpsError("unauthenticated", "Sign in required.");
    }
    const passCode = String(data?.passCode || "").trim().toUpperCase();
    const venueId = String(data?.venueId || "").trim().toLowerCase();
    const perkId = String(data?.perkId || "").trim();
    const perkLabel = String(data?.perkLabel || "").trim();
    const perkKey = String(data?.perkKey || "venue_perk").trim();
    const missingFields = [];
    if (!passCode) missingFields.push("passCode");
    if (!venueId) missingFields.push("venueId");
    if (!perkId) missingFields.push("perkId");
    if (missingFields.length) {
      console.warn("[createRedemption] invalid-argument", {
        uid: context.auth.uid,
        missingFields,
        receivedKeys: Object.keys(data || {}),
        passCodeLength: passCode.length
      });
      throw new HttpsError("invalid-argument", "Missing required fields.", {
        missingFields,
        receivedKeys: Object.keys(data || {})
      });
    }

    const now = admin.firestore.Timestamp.now();
    const serverNow = admin.firestore.FieldValue.serverTimestamp();
    const perkRef = db.collection("venues").doc(venueId).collection("perks").doc(perkId);
    const memberDisplayName = (member) =>
      (member?.displayName || member?.name || member?.fullName || member?.username || "FoCo member");

    for (let attempt = 0; attempt < 5; attempt += 1) {
      const redemptionId = generateCode(6).toUpperCase();
      try {
        const result = await db.runTransaction(async (tx) => {
          const resolved = await resolveMemberByPassCode(passCode, tx, context.auth?.uid || null);
          if (!resolved?.uid || !resolved.memberRef) {
            throw new HttpsError("not-found", "Pass ID not found.");
          }
          const memberData = resolved.memberData || {};
          const claims = context.auth?.token || {};
          const isPrivileged = claims.admin === true || claims.ceo === true;
          const isBetaMember =
            claims.beta === true ||
            (memberData.tier || "").toString().toLowerCase() === "beta" ||
            (memberData.membershipTier || "").toString().toLowerCase() === "beta" ||
            (memberData.email || "").toString().toLowerCase() === BETA_EMAIL ||
            resolved.uid === BETA_UID;
          if (resolved.uid !== context.auth.uid && !isPrivileged && !isBetaMember) {
            throw new HttpsError("permission-denied", "Pass ID does not match signed-in user.");
          }
          const resolvedPassCode = (memberData.passCode || passCode || "").toUpperCase();
          if (memberData.revoked) {
            throw new HttpsError("failed-precondition", "Membership is inactive.");
          }
          const validUntil = memberData.validUntil;
          if (validUntil && validUntil !== "never") {
            const expiryMs = toMillis(validUntil);
            if (expiryMs && expiryMs < Date.now()) {
              throw new HttpsError("failed-precondition", "Membership expired.");
            }
          }
          const lastRedeemMs = toMillis(memberData.lastRedemptionAt);
          if (lastRedeemMs && (Date.now() - lastRedeemMs) < 15000) {
            throw new HttpsError("resource-exhausted", "Slow down and try again.");
          }

          const unlimited = isStripeExcluded(claims, memberData)
            || ["ceo", "free"].includes(normalizeVoucherTierForWallet(memberData));
          let memberEntries = [];
          if (!unlimited) {
            const memberRedRef = db.collection("members").doc(resolved.uid).collection("redemptions");
            const memberRedSnap = await tx.get(memberRedRef);
            memberEntries = memberRedSnap.docs.map((docSnap) => docSnap.data() || {});
            const wallet = computeVoucherBalanceForWallet(memberData, memberEntries, now.toDate(), false);
            if (wallet.remaining <= 0) {
              throw new HttpsError(
                "failed-precondition",
                wallet.paymentRequired
                  ? "Payment required to reload monthly tokens."
                  : "Thank you for using FoCo After Dark this month, balance will reload next month.",
                {
                  reason: wallet.paymentRequired ? "PAYMENT_REQUIRED" : "TOKEN_LIMIT_REACHED",
                  remaining: wallet.remaining
                }
              );
            }
          }

          const perkSnap = await tx.get(perkRef);
          let perkData = perkSnap.data() || {};
          if (!perkSnap.exists) {
            const fallbackId = perkId.toLowerCase();
            const allowFallback = perkKey === "reward_shot" || fallbackId === "drink" || fallbackId === "shot" || fallbackId === "cover";
            if (allowFallback && perkLabel) {
              perkData = { label: perkLabel };
            } else {
              throw new HttpsError("not-found", "Perk not found.");
            }
          }
          if (!unlimited) {
            const tierForPerk = normalizeVoucherTierForWallet(memberData);
            if ((tierForPerk === "standard" || tierForPerk === "vip") && perkSnap.exists) {
              const perkQty = getTierPerkQuantity(perkData, tierForPerk);
              if (!perkQty) {
                throw new HttpsError("failed-precondition", "No redemptions left for this perk.", {
                  reason: "PERK_LIMIT_REACHED",
                  perkId,
                  tier: tierForPerk
                });
              }
              const perkUsed = getVenuePerkUsageCountForWallet(memberEntries, perkId, venueId);
              if (perkUsed >= perkQty) {
                throw new HttpsError("failed-precondition", "No redemptions left for this perk.", {
                  reason: "PERK_LIMIT_REACHED",
                  perkId,
                  tier: tierForPerk,
                  used: perkUsed,
                  limit: perkQty
                });
              }
            }
          }

          const venueRedRef = db.collection("venues").doc(venueId).collection("redemptions").doc(redemptionId);
          const memberRedRef = db.collection("members").doc(resolved.uid).collection("redemptions").doc(redemptionId);
          const existing = await tx.get(venueRedRef);
          if (existing.exists) {
            throw new HttpsError("already-exists", "Try again.");
          }
          if (resolved.passWrite) {
            tx.set(resolved.passWrite.ref, resolved.passWrite.data, { merge: true });
          }
          if (resolved.memberPassUpdate) {
            tx.set(resolved.memberRef, resolved.memberPassUpdate, { merge: true });
          }
          const expiresAt = admin.firestore.Timestamp.fromMillis(now.toMillis() + PENDING_REDEMPTION_TIMEOUT_MS);
          const storedPayload = {
            redemptionId,
            passCode: resolvedPassCode || passCode,
            memberUid: resolved.uid,
            memberName: memberDisplayName(memberData),
            tier: memberData.tier || "standard",
            venueId,
            perkId,
            perkKey,
            perkLabel: perkLabel || perkData.label || "Perk",
            status: "pending",
            expiresAt,
            createdAt: serverNow,
            updatedAt: serverNow,
            timestamp: serverNow,
            requestedVenue: venueId
          };
          const returnPayload = {
            ...storedPayload,
            createdAt: now,
            updatedAt: now,
            timestamp: now
          };
          tx.set(venueRedRef, storedPayload);
          tx.set(memberRedRef, storedPayload);
          tx.set(resolved.memberRef, {
            lastRedemptionAt: serverNow,
            lastRedemptionVenue: venueId,
            lastRedemptionPerk: perkId,
            updatedAt: serverNow
          }, { merge: true });
          return returnPayload;
        });

        return { ok: true, redemptionId: result.redemptionId, redemption: result };
      } catch (err) {
        if (err?.code === "already-exists") {
          continue;
        }
        throw err;
      }
    }

    throw new HttpsError("internal", "Failed to create redemption.");
  } catch (err) {
    console.error("[createRedemption]", {
      uid: context.auth?.uid || null,
      dataKeys: Object.keys(data || {}),
      err: err?.message || err
    });
    if (err instanceof HttpsError) throw err;
    throw new HttpsError("internal", "createRedemption failed", {
      original: String(err?.message || err),
      stack: String(err?.stack || "")
    });
  }
});

exports.verifyRedemption = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new HttpsError("unauthenticated", "Sign in required.");
  }
  const venueId = String(data?.venueId || "").trim().toLowerCase();
  const redemptionId = String(data?.redemptionId || "").trim().toUpperCase();
  const action = String(data?.action || "confirm").trim().toLowerCase();
  if (!venueId || !redemptionId) {
    throw new HttpsError("invalid-argument", "Redemption code and venue are required.");
  }
  const claims = context.auth.token || {};
  const uid = String(context.auth.uid || "");
  const email = String(claims.email || "").toLowerCase();
  const staffUidVenue = uid.startsWith("staff_") ? uid.slice(6) : "";
  const isStaffUid = !!staffUidVenue || email.startsWith("staff+");
  let allowedByRole = claims.admin === true || claims.ceo === true || claims.staff === true || isStaffUid;
  let allowedVenue = true;
  const tokenVenue = String(claims.venue || "").trim().toLowerCase();
  if (claims.staff === true && tokenVenue) {
    allowedVenue = tokenVenue === venueId;
  } else if (staffUidVenue) {
    allowedVenue = staffUidVenue === venueId;
  }
  if (!allowedByRole) {
    // Final fallback: role marker in members doc for legacy staff sessions.
    const memberSnap = await db.collection("members").doc(uid).get();
    const memberData = memberSnap.exists ? (memberSnap.data() || {}) : {};
    const role = String(memberData.role || "").toLowerCase();
    const memberVenue = String(memberData.venueId || "").toLowerCase();
    if (memberData.staff === true || role === "staff") {
      allowedByRole = true;
      if (memberVenue) {
        allowedVenue = memberVenue === venueId;
      }
    }
  }
  if (!allowedByRole) {
    throw new HttpsError("permission-denied", "Staff access required.");
  }
  if (!allowedVenue) {
    throw new HttpsError("permission-denied", "Wrong venue.");
  }

  const now = admin.firestore.Timestamp.now();
  const serverNow = admin.firestore.FieldValue.serverTimestamp();
  const venueRedRef = db.collection("venues").doc(venueId).collection("redemptions").doc(redemptionId);
  const ceoVoucherRef = db.collection("ceoVouchers").doc(redemptionId);

  const result = await db.runTransaction(async (tx) => {
    const redSnap = await tx.get(venueRedRef);
    if (!redSnap.exists) {
      const ceoSnap = await tx.get(ceoVoucherRef);
      if (!ceoSnap.exists) {
        throw new HttpsError("not-found", "Code not found.");
      }
      const ceoPayload = {
        redemptionId,
        passCode: "CEO",
        memberUid: null,
        memberName: "CEO issued",
        tier: "ceo",
        venueId,
        perkId: ceoSnap.data()?.perk || "ceo_perk",
        perkKey: "ceo_perk",
        perkLabel: ceoSnap.data()?.perk || "CEO issued",
        status: "verified",
        createdAt: ceoSnap.data()?.createdAt || now,
        updatedAt: now,
        verifiedAt: now,
        timestamp: ceoSnap.data()?.createdAt || now,
        requestedVenue: venueId,
        ceo: true
      };
      tx.set(venueRedRef, ceoPayload, { merge: true });
      tx.set(ceoVoucherRef, { used: true, usedAt: now, venueId }, { merge: true });
      return ceoPayload;
    }
    const dataSnap = redSnap.data() || {};
    if (dataSnap.venueId && dataSnap.venueId !== venueId) {
      throw new HttpsError("permission-denied", "This code belongs to another venue.");
    }
    const pendingExpired = (String(dataSnap.status || "").toLowerCase() === "pending")
      && !isPendingRedemptionFresh(dataSnap, now.toDate(), PENDING_REDEMPTION_TIMEOUT_MS);
    if (pendingExpired) {
      const memberUid = dataSnap.memberUid;
      const expiredUpdates = {
        status: "expired",
        updatedAt: serverNow,
        expiredAt: serverNow
      };
      tx.update(venueRedRef, expiredUpdates);
      if (memberUid) {
        const memberRedRef = db.collection("members").doc(memberUid).collection("redemptions").doc(redemptionId);
        tx.set(memberRedRef, expiredUpdates, { merge: true });
      }
      return { ...dataSnap, ...expiredUpdates, expired: true };
    }
    if (dataSnap.status === "verified" && action === "confirm") {
      return dataSnap;
    }
    const memberUid = dataSnap.memberUid;
    const memberRef = memberUid ? db.collection("members").doc(memberUid) : null;
    const memberSnap = memberRef ? await tx.get(memberRef) : null;
    const nextStatus = action === "deny" ? "denied" : "verified";
    const storedUpdates = {
      status: nextStatus,
      updatedAt: serverNow
    };
    if (nextStatus === "verified") {
      storedUpdates.verifiedAt = serverNow;
    } else {
      storedUpdates.deniedAt = serverNow;
    }
    tx.update(venueRedRef, storedUpdates);

    if (memberUid) {
      const memberRedRef = db.collection("members").doc(memberUid).collection("redemptions").doc(redemptionId);
      tx.set(memberRedRef, storedUpdates, { merge: true });
      if (memberSnap && memberSnap.exists) {
        const memberData = memberSnap.data() || {};
        const remaining = memberData.perksRemaining;
        if (remaining && typeof remaining.tokens === "number" && nextStatus === "verified") {
          tx.update(memberRef, { "perksRemaining.tokens": Math.max(0, remaining.tokens - 1), lastVerifiedAt: serverNow, updatedAt: serverNow });
        } else if (nextStatus === "verified") {
          tx.set(memberRef, { lastVerifiedAt: serverNow, updatedAt: serverNow }, { merge: true });
        }
      }
    }
    const returnUpdates = {
      status: nextStatus,
      updatedAt: now
    };
    if (nextStatus === "verified") {
      returnUpdates.verifiedAt = now;
    } else {
      returnUpdates.deniedAt = now;
    }
    return { ...dataSnap, ...returnUpdates };
  });

  if (result?.expired) {
    throw new HttpsError("failed-precondition", "Code expired. Ask the member to redeem again.");
  }
  return { ok: true, redemption: result };
});

// Resolve username to email (rate-limited, no auth required)
exports.resolveUsernameToEmail = functions.https.onCall(async (data, context) => {
  const raw = (data?.username || "").toString().trim().replace(/^@/, "").toLowerCase();
  if (!raw) {
    throw new HttpsError("invalid-argument", "Username required.");
  }
  if (!/^[a-z0-9_]{3,24}$/.test(raw)) {
    throw new HttpsError("invalid-argument", "Invalid username.");
  }
  const ip = getRequestIp(context) || "unknown";
  const key = `usernameLookup_${hashLookupKey(ip)}`;
  await enforceLookupRateLimit({ key, limit: 30, windowMs: 10 * 60 * 1000 });

  const unameSnap = await db.collection("usernames").doc(raw).get();
  if (!unameSnap.exists) {
    return { email: null };
  }
  const dataSnap = unameSnap.data() || {};
  if (dataSnap.email) {
    return { email: dataSnap.email };
  }
  if (dataSnap.uid) {
    const memberSnap = await db.collection("members").doc(dataSnap.uid).get();
    const memberData = memberSnap.exists ? memberSnap.data() : {};
    return { email: memberData?.email || null };
  }
  return { email: null };
});

// Award points server-side to prevent client tampering.
exports.awardPoints = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const delta = Number(data?.points || 0);
  const reason = (data?.reason || "").toString().slice(0, 80);
  const venue = (data?.venue || "").toString().trim().toLowerCase();
  if (!Number.isFinite(delta) || delta <= 0 || delta > 50) {
    throw new HttpsError("invalid-argument", "Invalid points amount.");
  }
  const todayKey = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Denver' });
  const ref = db.collection("members").doc(uid);
  const result = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const memberData = snap.exists ? snap.data() : {};
    const current = Number(memberData.points || 0);
    let daily = Number(memberData.pointsDaily || 0);
    let dailyDate = (memberData.pointsDailyDate || "").toString();
    if (!dailyDate || dailyDate !== todayKey) {
      daily = 0;
      dailyDate = todayKey;
    }
    const dailyCap = 200;
    if (daily + delta > dailyCap) {
      throw new HttpsError("resource-exhausted", "Daily points limit reached.");
    }
    const next = current + delta;
    const updates = {
      points: next,
      pointsDaily: daily + delta,
      pointsDailyDate: dailyDate,
      pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
      lastPointsReason: reason || null
    };
    if (venue) {
      updates[`venuesVisited.${venue}`] = true;
    }
    tx.set(ref, updates, { merge: true });
    return { points: next, venuesVisited: venue ? { ...(memberData.venuesVisited || {}), [venue]: true } : (memberData.venuesVisited || {}) };
  });
  return result;
});

// Seed user profile and username directory on auth create
exports.initUserProfile = functions.auth.user().onCreate(async (user) => {
  const uid = user.uid;
  const email = (user.email || '').toLowerCase();
  const isAnonymousUser = !email && (!user.providerData || user.providerData.length === 0);
  const memberRef = db.collection('members').doc(uid);
  if (uid.startsWith("staff_")) {
    await memberRef.set({
      staff: true,
      role: "staff",
      tier: "staff",
      displayName: user.displayName || "Venue staff",
      email,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return;
  }
  if (isAnonymousUser) {
    return;
  }
  const existing = await memberRef.get();
  let passCode = null;
  if (existing.exists) {
    const data = existing.data() || {};
    if (data.passCode) passCode = String(data.passCode).toUpperCase();
  }
  if (!passCode) {
    passCode = await ensureUniquePassCode();
  }
  const defaultPerks = {
    tier: 'standard',
    remaining: { drink: 2, shot: 1, cover: 1 }
  };
  const profile = {
    passCode,
    tier: existing.exists && existing.data().tier ? existing.data().tier : 'standard',
    memberSince: existing.exists && existing.data().memberSince ? existing.data().memberSince : new Date().toISOString(),
    extraVouchers: existing.exists && existing.data().extraVouchers ? existing.data().extraVouchers : { drink: 0, shot: 0, cover: 0 },
    perks: existing.exists && existing.data().perks ? existing.data().perks : defaultPerks,
    points: existing.exists && existing.data().points ? existing.data().points : 0,
    username: existing.exists && existing.data().username ? existing.data().username : (email ? email.split('@')[0] : ''),
    email
  };
  await memberRef.set(profile, { merge: true });
  await db.collection('passes').doc(passCode).set({
    uid,
    passCode,
    tier: profile.tier || 'standard',
    status: 'active',
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  const uname = (profile.username || '').trim().toLowerCase();
  if (uname) {
    await db.collection('usernames').doc(uname).set({
      uid,
      email,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
  }
  await db.collection('settings').doc('appStats').set({
    membersCount: admin.firestore.FieldValue.increment(1),
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return true;
});

// Nightly close-out summary with analytics + guaranteed email queue
async function runNightlyCloseOutCore(source = "nightlyCloseOut") {
  const now = Timestamp.now();
  const dayStart = Timestamp.fromDate(new Date(Date.now() - 24 * 60 * 60 * 1000));

  const VENUE_CATALOG = {
    bar_district: "The Bar District",
    yeti: "Yeti Bar & Grill",
    rec_room: "Rec Room Fort Collins",
    bondi_beach: "Bondi Beach Bar & Grill",
    surfside: "Surfside",
    lucky_joes: "Lucky Joe’s Sidewalk Saloon",
    trail_head: "Trail Head Tavern",
    steak_out: "Steak-Out Saloon",
    road_34: "Road 34 Bike Bar",
    brothers: "Brothers",
    pour_brothers: "Pour Brothers Community Tavern",
    tap_handle: "Tap and Handle",
    high_point: "High Point",
    pinball_jones: "Pinball Jones",
    elliotts: "Elliott’s Martini Bar",
    town_pump: "Town Pump",
    mayor_old_town: "The Mayor of Old Town"
  };
  const summary = {
    scansToday: 0,
    uniqueGuests: 0,
    venuesActive: 0,
    topPerk: 'None',
    alertsPosted: 0,
    vipDealsPosted: 0,
    generatedAt: now.toDate().toISOString(),
  };
  const perVenue = {};
  const toMillis = (ts) => {
    if (!ts) return Date.now();
    if (ts.toMillis) return ts.toMillis();
    return new Date(ts).getTime();
  };
  const dayKey = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Denver' });

  Object.entries(VENUE_CATALOG).forEach(([venueId, venueName]) => {
    perVenue[venueId] = {
      venue: venueId,
      venueName,
      verified: 0,
      pending: 0,
      byPerk: {},
      uniqueMembers: new Set(),
      hourly: {},
      items: []
    };
  });

  // Redemptions in last 24h
  try {
    const redSnap = await db.collectionGroup('redemptions')
      .where('timestamp', '>=', dayStart)
      .get();
    summary.scansToday = redSnap.size;
    const guestSet = new Set();
    const venueSet = new Set();
    const allowedVenues = new Set(Object.keys(VENUE_CATALOG));
    const perkCounts = {};
    redSnap.forEach(doc => {
      const d = doc.data() || {};
      const venueId = (d.venue || 'unknown').toLowerCase();
      if (!allowedVenues.has(venueId)) return;
      if (d.passId) guestSet.add(String(d.passId).toUpperCase());
      if (d.venue) venueSet.add(String(d.venue).toLowerCase());
      const perkKey = (d.perkKey || d.perk || 'perk').toLowerCase();
      perkCounts[perkKey] = (perkCounts[perkKey] || 0) + 1;
      if (!perVenue[venueId]) {
        perVenue[venueId] = {
          venue: venueId,
          venueName: d.venueName || VENUE_CATALOG[venueId] || venueId,
          verified: 0,
          pending: 0,
          byPerk: {},
          uniqueMembers: new Set(),
          hourly: {},
          items: []
        };
      }
      if (d.venueName && !perVenue[venueId].venueName) {
        perVenue[venueId].venueName = d.venueName;
      }
      const status = d.status || (d.used ? 'verified' : 'pending');
      if (status === 'verified') {
        perVenue[venueId].verified += 1;
      } else {
        perVenue[venueId].pending += 1;
      }
      const perkLabel = d.perk || d.perkKey || 'Perk';
      perVenue[venueId].byPerk[perkLabel] = (perVenue[venueId].byPerk[perkLabel] || 0) + 1;
      const ts = toMillis(d.timestamp);
      if (status === 'verified') {
        const hr = new Date(ts).getHours();
        perVenue[venueId].hourly[hr] = (perVenue[venueId].hourly[hr] || 0) + 1;
      }
      const memberKey = d.member || d.passId || d.code || 'member';
      perVenue[venueId].uniqueMembers.add(String(memberKey));
      perVenue[venueId].items.push({
        code: d.code || '',
        perk: perkLabel,
        member: d.member || d.passId || 'FoCo member',
        status,
        timestamp: new Date(ts).toISOString()
      });
    });
    summary.uniqueGuests = guestSet.size;
    summary.venuesActive = venueSet.size;
    if (Object.keys(perkCounts).length) {
      summary.topPerk = Object.entries(perkCounts).sort((a, b) => b[1] - a[1])[0][0];
    }
  } catch (err) {
    console.warn('nightlyCloseOut: redemptions summary failed', err);
  }

  // Alerts in last 24h
  try {
    const alertSnap = await db.collection('alerts')
      .where('createdAt', '>=', dayStart)
      .get();
    summary.alertsPosted = alertSnap.size;
  } catch (err) {
    console.warn('nightlyCloseOut: alerts summary failed', err);
  }

  // VIP deals updated in last 24h
  try {
    const dealSnap = await db.collection('deals')
      .where('updatedAt', '>=', dayStart)
      .get();
    summary.vipDealsPosted = dealSnap.size;
  } catch (err) {
    console.warn('nightlyCloseOut: vipDeals summary failed', err);
  }

  // Build email payload
  const payload = {
    to: REPORTS_TO_EMAIL,
    type: 'nightly-closeout',
    subject: `Nightly FoCo After Dark report • ${new Date().toLocaleDateString('en-US', { timeZone: 'America/Denver' })}`,
    createdAt: now,
    status: 'pending',
    meta: summary,
    source
  };

  const humanText = `FoCo After Dark nightly report

Scans: ${summary.scansToday}
Unique guests: ${summary.uniqueGuests}
Venues active: ${summary.venuesActive}
Top perk: ${summary.topPerk}
Alerts posted: ${summary.alertsPosted}
VIP deals posted: ${summary.vipDealsPosted}
Generated at (MT): ${summary.generatedAt}

By venue:
${Object.values(perVenue).map(entry => {
    const topPerk = Object.entries(entry.byPerk).sort((a, b) => b[1] - a[1])[0]?.[0] || 'None';
    return `- ${entry.venueName || entry.venue}: ${entry.verified} verified, ${entry.pending} pending, ${entry.uniqueMembers.size} guests, top perk ${topPerk}`;
  }).join('\n') || '- No venue activity yet.'}

This email sends even when counts are zero.`;

  let emailSent = false;
  let emailError = null;
  const smtpResult = await sendReportEmail(payload.subject, humanText, { to: REPORTS_TO_EMAIL });
  if (smtpResult.sent) {
    emailSent = true;
    console.log("Nightly close-out email sent via SMTP");
  } else {
    emailError = smtpResult.error || "SMTP send failed";
  }
  // Direct email fallback (FormSubmit) so reports go out even without SMTP.
  if (!emailSent) {
    try {
      const resp = await fetch(`https://formsubmit.co/ajax/${REPORTS_TO_EMAIL}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          name: "FoCo After Dark nightly report",
          email: "reports@focoafterdark.com",
          message: humanText,
          formsubmit_account: REPORTS_TO_EMAIL,
          _subject: payload.subject,
          _template: "table"
        })
      });
      const data = await resp.json().catch(() => ({}));
      emailSent = resp.ok && data.success === "true";
      if (emailSent) {
        console.log("Nightly close-out email sent via FormSubmit");
      } else {
        emailError = emailError || "FormSubmit send failed";
      }
    } catch (err) {
      console.warn("nightlyCloseOut: FormSubmit send failed", err);
      if (!emailError) emailError = err?.message || "FormSubmit send failed";
    }
  }

  payload.status = emailSent ? 'sent' : 'pending';
  if (emailSent) {
    payload.sentAt = now;
  } else if (emailError) {
    payload.lastError = emailError;
  }

  // Optional: if the "mail" collection is wired to an email extension (Trigger Email / SendGrid), enqueue there too
  try {
    await db.collection('mail').add({
      to: [REPORTS_TO_EMAIL],
      message: {
        subject: payload.subject,
        text: humanText,
      },
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      source: 'nightlyCloseOut'
    });
    console.log('Queued nightly email in mail collection');
  } catch (err) {
    console.warn('nightlyCloseOut: failed to enqueue mail collection', err);
  }

  // Per-venue close-out emails (one per venue, daily)
  const dayLabel = new Date().toLocaleDateString('en-US', { timeZone: 'America/Denver' });
  const venueEntries = Object.values(perVenue || {});
  for (const entry of venueEntries) {
    try {
      const venueName = entry.venueName || entry.venue || "Venue";
      const venueTotals = {
        verified: entry.verified || 0,
        pending: entry.pending || 0,
        uniqueMembers: entry.uniqueMembers?.size || 0,
        topPerk: Object.entries(entry.byPerk || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || "None",
        peakHour: Object.entries(entry.hourly || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || "n/a"
      };
      const venueLines = (entry.items || []).slice(0, 50).map(item =>
        `${item.code} · ${item.perk} · ${new Date(item.timestamp).toLocaleString('en-US', { timeZone: 'America/Denver' })}`
      );
      const venueText = `FoCo After Dark close-out report

Venue: ${venueName}
Generated at (MT): ${summary.generatedAt}
Window: last 24 hours

Verified: ${venueTotals.verified}
Pending: ${venueTotals.pending}
Unique guests: ${venueTotals.uniqueMembers}
Top perk: ${venueTotals.topPerk}
Peak hour: ${venueTotals.peakHour}

Recent activity:
${venueLines.length ? venueLines.join('\n') : 'No redemptions yet.'}
`;

      const venueSubject = `Close-out report • ${venueName} • ${dayLabel}`;
      const venueResult = await sendReportEmail(venueSubject, venueText, { to: REPORTS_TO_EMAIL });
      if (venueResult.sent) {
        console.log(`Venue close-out email sent via SMTP: ${venueName}`);
      } else {
        console.warn(`Venue close-out email failed via SMTP: ${venueName}`, venueResult.error);
      }

      try {
        await db.collection('mail').add({
          to: [REPORTS_TO_EMAIL],
          message: {
            subject: venueSubject,
            text: venueText
          },
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          source: 'nightlyCloseOutVenue',
          venue: entry.venue || null
        });
      } catch (err) {
        console.warn('nightlyCloseOut: failed to enqueue venue mail', venueName, err);
      }
    } catch (err) {
      console.warn('nightlyCloseOut: per-venue email failed', err);
    }
  }

  // Insert into systemEmails for delivery (use your mail pipeline / Extension)
  try {
    await db.collection('systemEmails').add(payload);
    console.log('Queued nightly close-out summary email');
  } catch (err) {
    console.error('nightlyCloseOut: failed to queue email', err);
  }

  // Also log a minimal doc for audit even if email queue fails
  try {
    await db.collection('closeOutReports').add({
      generatedAt: now,
      summary,
      venue: 'all',
      auto: true,
    });
  } catch (err) {
    console.warn('nightlyCloseOut: failed to log fallback report', err);
  }

  // Write per-venue close-out reports for CEO review (daily)
  try {
    const entries = Object.values(perVenue);
    for (const entry of entries) {
      const topPerk = Object.entries(entry.byPerk).sort((a, b) => b[1] - a[1])[0]?.[0] || 'None';
      const peakHour = Object.entries(entry.hourly).sort((a, b) => b[1] - a[1])[0];
      const peakLabel = peakHour ? `${peakHour[0]}:00 (${peakHour[1]} scans)` : 'n/a';
      const reportId = `auto-${entry.venue}-${dayKey}`;
      await db.collection('closeOutReports').doc(reportId).set({
        venue: entry.venue,
        venueName: entry.venueName || entry.venue,
        generatedAt: now,
        windowLabel: 'Window: last 24 hours',
        totals: {
          verified: entry.verified,
          pending: entry.pending,
          uniqueMembers: entry.uniqueMembers.size,
          topPerk,
          peakHour: peakLabel,
          byPerk: entry.byPerk
        },
        items: entry.items.slice(0, 80),
        auto: true,
        source: 'nightlyCloseOut'
      }, { merge: true });
    }
  } catch (err) {
    console.warn('nightlyCloseOut: failed to write per-venue reports', err);
  }

  // Update health doc
  try {
    await db.collection('settings').doc('health').set({
      lastCloseoutAttempt: admin.firestore.FieldValue.serverTimestamp(),
      lastCloseoutEmail: admin.firestore.FieldValue.serverTimestamp(),
      lastCloseoutSummary: summary,
      lastCloseoutEmailStatus: emailSent ? 'sent' : 'queued',
      lastCloseoutError: emailSent ? null : (emailError || "Email not sent")
    }, { merge: true });
  } catch (err) {
    console.warn('nightlyCloseOut: failed to update health doc', err);
  }

  return null;
}

exports.nightlyCloseOut = functions.runWith(reportEmailSecrets)
  .pubsub.schedule('0 3 * * *')
  .timeZone('America/Denver')
  .onRun(async () => runNightlyCloseOutCore('nightlyCloseOut'));

exports.runCloseOutNow = functions.runWith(reportEmailSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const uid = context.auth.uid;
  let requesterData = {};
  try {
    const snap = await db.collection('members').doc(uid).get();
    requesterData = snap.exists ? (snap.data() || {}) : {};
  } catch (_) {}
  if (!isCeoContext(context, requesterData)) {
    throw new HttpsError('permission-denied', 'CEO only');
  }
  await runNightlyCloseOutCore('manualCloseOut');
  return { ok: true };
});


// Atomic username reservation/update
exports.reserveUsername = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const uid = context.auth.uid;
  const desired = (data && data.username ? String(data.username) : '').trim().toLowerCase();
  if (!desired || desired.length < 3 || desired.length > 10 || !/^[a-z0-9_ ]+$/.test(desired)) {
    throw new HttpsError('invalid-argument', 'Invalid username format');
  }
  const memberRef = db.collection('members').doc(uid);
  return db.runTransaction(async (tx) => {
    const unameRef = db.collection('usernames').doc(desired);
    const existingUname = await tx.get(unameRef);
    if (existingUname.exists && existingUname.data().uid !== uid) {
      throw new HttpsError('already-exists', 'Username already taken');
    }
    const memberSnap = await tx.get(memberRef);
    if (!memberSnap.exists) {
      throw new HttpsError('failed-precondition', 'Member profile missing');
    }
    const member = memberSnap.data() || {};
    tx.set(memberRef, { username: desired, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    tx.set(unameRef, {
      uid,
      email: member.email || context.auth.token.email || '',
      passCode: (member.passCode || '').toUpperCase(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return { success: true, username: desired };
  });
});

// Server-side points adjust
exports.awardPoints = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const points = Number(data?.points || 0);
  const reason = (data?.reason || 'adjustment').toString().slice(0, 120);
  if (!Number.isFinite(points) || points === 0) throw new HttpsError('invalid-argument', 'points required');
  const uid = context.auth.uid;
  const memberRef = db.collection('members').doc(uid);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(memberRef);
    if (!snap.exists) throw new HttpsError('failed-precondition', 'Member missing');
    const current = snap.data()?.points || 0;
    const next = Math.max(0, current + points);
    tx.set(memberRef, { points: next, pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    await db.collection('auditLogs').add({
      action: 'awardPoints',
      uid,
      delta: points,
      reason,
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });
  });
  return { success: true };
});

// Helpers for night wheel server-side
function getWeekToken(date = new Date()) {
  const now = date;
  const jan1 = new Date(now.getFullYear(), 0, 1);
  const dayMs = 24 * 60 * 60 * 1000;
  const week = Math.ceil((((now - jan1) / dayMs) + jan1.getDay() + 1) / 7);
  return `${now.getFullYear()}-W${week}`;
}
function nightWheelAllowance(tier, flags = {}) {
  if (flags.freeMembership || flags.ceo) return Infinity;
  if (tier === 'vip' || tier === 'ceo') return 2;
  return 1;
}

exports.spinNightWheel = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const uid = context.auth.uid;
  const memberRef = db.collection('members').doc(uid);
  const specials = [
    "2-for-1 shots",
    "Half-price cocktails",
    "No-cover tonight",
    "VIP skip-the-line"
  ];
  const challenges = [
    "Buy a stranger a drink",
    "Start a dance circle",
    "Compliment three people",
    "Teach someone your favorite move",
    "Start a group cheers",
    "Ask DJ for a throwback",
    "Swap accessories with a friend",
    "Get a photo at the neon sign",
    "Tell the bartender a joke",
    "Find someone with your name",
    "Sing one line of a song aloud",
    "Start a conga line",
    "High-five five people",
    "Do a 10-second pose-off",
    "Trade bracelets",
    "Get a group of 4 to yell “FoCo!”",
    "Gift a free water to someone",
    "Toast to a stranger’s night",
    "Start a mini shuffle",
    "Teach someone a handshake",
    "Challenge a friend to a dance battle",
    "Wear sunglasses inside for 1 minute",
    "Start a slow-mo walk",
    "Start a “cheers” wave",
    "Find someone from your hometown",
    "Compliment someone’s outfit",
    "Get a selfie with 3 people",
    "Spell FOCO with friends’ bodies",
    "Create a human tunnel",
    "Start a karaoke chant"
  ];
  const bars = ["FoCo Bar District", "Social", "Bondi Beach Bar", "Surfside", "The Exchange"];
  function pick(list) { return list[Math.floor(Math.random() * list.length)]; }
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(memberRef);
    if (!snap.exists) throw new HttpsError('failed-precondition', 'Member missing');
    const member = snap.data() || {};
    const passCode = (member.passCode || '').toUpperCase();
    const tier = (member.tier || 'standard').toLowerCase();
    const allowance = nightWheelAllowance(tier, { freeMembership: !!member.freeMembership, ceo: !!member.ceo });
    const weekToken = getWeekToken();
    const state = member.nightWheel || {};
    const entry = state[passCode] || {};
    const spins = (entry.week === weekToken ? (entry.spins || 0) : 0);
    if (allowance !== Infinity && spins >= allowance) {
      throw new HttpsError('resource-exhausted', 'No spins left this week');
    }
    const nextSpins = spins + 1;
    const bar = pick(bars);
    const drink = pick(specials);
    const challenge = pick(challenges);
    state[passCode] = { week: weekToken, spins: nextSpins };
    const points = Math.max(0, (member.points || 0) + 20);
    tx.set(memberRef, {
      nightWheel: state,
      points,
      pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    tx.set(db.collection('auditLogs').doc(), {
      action: 'spinNightWheel',
      uid,
      passCode,
      week: weekToken,
      spins: nextSpins,
      pointsAwarded: 20,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      result: { bar, drink, challenge }
    });
    return { bar, drink, challenge, remaining: allowance === Infinity ? Infinity : allowance - nextSpins, points };
  });
});

// Push notifications: register token per user
exports.registerPushToken = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const token = (data?.token || '').trim();
  const clientKey = (data?.clientKey || '').trim().slice(0, 120);
  if (!token) throw new HttpsError('invalid-argument', 'Token required');
  if (token.length < 80 || /\s/.test(token)) {
    throw new HttpsError('invalid-argument', 'Invalid token');
  }
  const uid = context.auth.uid;
  // Keep each device token bound to only one user at a time.
  const existing = await db.collectionGroup('tokens').where('token', '==', token).get();
  if (!existing.empty) {
    const cleanup = db.batch();
    existing.docs.forEach((docSnap) => {
      const ownerUid = docSnap.get('uid');
      if (ownerUid && ownerUid !== uid) {
        cleanup.delete(docSnap.ref);
      }
    });
    await cleanup.commit();
  }
  if (clientKey) {
    const staleForClient = await db.collection('pushTokens').doc(uid).collection('tokens')
      .where('clientKey', '==', clientKey)
      .get();
    if (!staleForClient.empty) {
      const staleBatch = db.batch();
      staleForClient.docs.forEach((docSnap) => {
        if (docSnap.id !== token) {
          staleBatch.delete(docSnap.ref);
        }
      });
      await staleBatch.commit();
    }
    // Keep a single active token per device client key across all users/sessions.
    const globalStale = await db.collectionGroup('tokens')
      .where('clientKey', '==', clientKey)
      .get();
    if (!globalStale.empty) {
      const globalBatch = db.batch();
      globalStale.docs.forEach((docSnap) => {
        if (docSnap.id !== token) {
          globalBatch.delete(docSnap.ref);
        }
      });
      await globalBatch.commit();
    }
  } else {
    // Legacy clients without a client key keep one active token per user.
    const ownTokens = await db.collection('pushTokens').doc(uid).collection('tokens').get();
    if (!ownTokens.empty) {
      const ownBatch = db.batch();
      ownTokens.docs.forEach((docSnap) => {
        if (docSnap.id !== token) ownBatch.delete(docSnap.ref);
      });
      await ownBatch.commit();
    }
  }
  const ref = db.collection('pushTokens').doc(uid).collection('tokens').doc(token);
  await ref.set({
    token,
    uid,
    clientKey: clientKey || null,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return { success: true };
});

async function getAllPushTokens() {
  const snap = await db.collectionGroup('tokens').get();
  const byClientKey = new Map();
  const byUidWithoutClientKey = new Map();
  const fallbackNoUid = new Set();
  snap.docs.forEach((docSnap) => {
    const token = docSnap.id;
    if (!token) return;
    const data = docSnap.data() || {};
    const clientKey = String(data.clientKey || "").trim();
    const uid = String(data.uid || "").trim();
    const updatedMs = toMillisSafe(data.updatedAt || data.createdAt);
    if (!clientKey) {
      if (uid) {
        const prev = byUidWithoutClientKey.get(uid);
        if (!prev || updatedMs >= prev.updatedMs) {
          byUidWithoutClientKey.set(uid, { token, updatedMs });
        }
      } else {
        fallbackNoUid.add(token);
      }
      return;
    }
    const prev = byClientKey.get(clientKey);
    if (!prev || updatedMs >= prev.updatedMs) {
      byClientKey.set(clientKey, { token, updatedMs });
    }
  });
  const uniqueClientTokens = Array.from(byClientKey.values()).map(item => item.token);
  const uniqueLegacyTokens = Array.from(byUidWithoutClientKey.values()).map(item => item.token);
  return Array.from(new Set([...uniqueClientTokens, ...uniqueLegacyTokens, ...fallbackNoUid]));
}

function chunkTokens(tokens, size = 500) {
  const chunks = [];
  for (let i = 0; i < tokens.length; i += size) {
    chunks.push(tokens.slice(i, i + size));
  }
  return chunks;
}

function toMillisSafe(value) {
  if (!value) return 0;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (value instanceof Date) return value.getTime();
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function getPushVenueName(venueId, fallbackName = "") {
  const normalized = String(venueId || "").trim().toLowerCase();
  if (normalized && STAFF_VENUES[normalized]?.name) return STAFF_VENUES[normalized].name;
  if (fallbackName) return String(fallbackName);
  return normalized ? normalized.replace(/[_-]+/g, " ").replace(/\b\w/g, c => c.toUpperCase()) : "A venue";
}

function normalizePushLink(link) {
  const fallback = "https://foco-after-dark.web.app/";
  const raw = String(link || "").trim();
  if (!raw) return fallback;
  if (raw.startsWith("https://")) return raw;
  if (raw.startsWith("http://")) return raw.replace(/^http:\/\//i, "https://");
  if (raw.startsWith("#")) return `https://foco-after-dark.web.app/${raw}`;
  if (raw.startsWith("/")) return `https://foco-after-dark.web.app${raw}`;
  return `https://foco-after-dark.web.app/${raw.replace(/^\/+/, "")}`;
}

async function cleanupInvalidPushToken(token) {
  if (!token) return;
  try {
    const existing = await db.collectionGroup("tokens").where("token", "==", token).get();
    if (existing.empty) return;
    const batch = db.batch();
    existing.docs.forEach(docSnap => batch.delete(docSnap.ref));
    await batch.commit();
  } catch (err) {
    console.warn("push cleanup failed", token.slice(-8), err?.message || err);
  }
}

async function claimPushDispatchWindow(dispatchKey, windowMs = 10 * 1000) {
  const normalized = String(dispatchKey || "").trim();
  if (!normalized) return true;
  const hash = crypto.createHash("sha1").update(normalized).digest("hex");
  const ref = db.collection("pushSignals").doc(`dispatch_${hash}`);
  const now = Date.now();
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const storedLastMs = Number(snap.exists ? snap.data()?.lastSentMs : 0);
    const lastMs = Number.isFinite(storedLastMs) && storedLastMs > 0
      ? storedLastMs
      : toMillisSafe(snap.exists ? snap.data()?.lastSentAt : null);
    if (lastMs && (now - lastMs) < windowMs) {
      return false;
    }
    tx.set(ref, {
      key: normalized.slice(0, 280),
      lastSentMs: now,
      lastSentAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return true;
  });
}

async function sendPushToAll({ title, body, link, source = "system", dedupeKey = "", dedupeWindowMs = 10000, force = false }) {
  const dispatchKey = dedupeKey || `${String(title || "").trim()}|${String(body || "").trim()}|${String(link || "").trim()}`;
  if (!force) {
    const canSend = await claimPushDispatchWindow(dispatchKey, dedupeWindowMs);
    if (!canSend) return { success: true, skipped: true, reason: "deduped", sent: 0, failed: 0 };
  }
  const safeLink = normalizePushLink(link);
  const tokens = await getAllPushTokens();
  if (!tokens.length) return { success: false, reason: 'no_tokens' };
  const batches = chunkTokens(tokens, 500);
  let sent = 0;
  let failed = 0;
  const sentAt = String(Date.now());
  const invalidTokens = new Set();
  const errorCodes = {};
  let firstErrorCode = null;
  for (const batch of batches) {
    const message = {
      data: {
        title: String(title || ''),
        body: String(body || ''),
        link: safeLink,
        sentAt
      },
      webpush: {
        headers: {
          Urgency: "high",
          TTL: "2419200"
        },
        notification: {
          title,
          body,
          icon: "https://foco-after-dark.web.app/foco-logo.png",
          badge: "https://foco-after-dark.web.app/foco-logo.png",
          tag: `foco-${sentAt}`
        },
        fcmOptions: safeLink ? { link: safeLink } : undefined
      },
      tokens: batch
    };
    const res = await messaging.sendEachForMulticast(message);
    sent += res.successCount || 0;
    failed += res.failureCount || 0;
    res.responses.forEach((response, idx) => {
      if (response?.success) return;
      const code = response?.error?.code || "";
      if (code) {
        errorCodes[code] = (errorCodes[code] || 0) + 1;
        if (!firstErrorCode) firstErrorCode = code;
      }
      if (
        code === "messaging/registration-token-not-registered" ||
        code === "messaging/invalid-registration-token" ||
        code === "messaging/invalid-argument"
      ) {
        const badToken = batch[idx];
        if (badToken) invalidTokens.add(badToken);
      }
    });
  }
  if (invalidTokens.size) {
    await Promise.all(Array.from(invalidTokens).map(token => cleanupInvalidPushToken(token)));
  }
  await db.collection("pushAudit").add({
    source,
    title: String(title || "").slice(0, 120),
    body: String(body || "").slice(0, 220),
    link: String(link || "").slice(0, 220),
    tokenCount: tokens.length,
    sent,
    failed,
    firstErrorCode: firstErrorCode || null,
    errorCodes,
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  }).catch(() => {});
  return { success: true, sent, failed, firstErrorCode, errorCodes };
}

async function sendPushToUid(uid, { title, body, link, source = "direct" } = {}) {
  const targetUid = String(uid || "").trim();
  if (!targetUid) return { success: false, reason: "missing_uid", sent: 0, failed: 0 };
  const tokenSnap = await db.collection('pushTokens').doc(targetUid).collection('tokens').get();
  const tokens = tokenSnap.docs.map((d) => d.id).filter(Boolean);
  if (!tokens.length) return { success: false, reason: 'no_tokens', sent: 0, failed: 0 };
  const safeLink = normalizePushLink((link || '').toString());
  const sentAt = String(Date.now());
  const message = {
    data: {
      title: String(title || ''),
      body: String(body || ''),
      link: safeLink,
      sentAt
    },
    webpush: {
      headers: {
        Urgency: "high",
        TTL: "2419200"
      },
      notification: {
        title: String(title || ''),
        body: String(body || ''),
        icon: "https://foco-after-dark.web.app/foco-logo.png",
        badge: "https://foco-after-dark.web.app/foco-logo.png",
        tag: `foco-${sentAt}`
      },
      fcmOptions: safeLink ? { link: safeLink } : undefined
    },
    tokens
  };
  const res = await messaging.sendEachForMulticast(message);
  const invalidTokens = [];
  const errorCodes = {};
  let firstErrorCode = null;
  res.responses.forEach((r, idx) => {
    if (r?.success) return;
    const code = r?.error?.code || "unknown";
    errorCodes[code] = (errorCodes[code] || 0) + 1;
    if (!firstErrorCode) firstErrorCode = code;
    if (
      code === "messaging/registration-token-not-registered" ||
      code === "messaging/invalid-registration-token" ||
      code === "messaging/invalid-argument"
    ) {
      const badToken = tokens[idx];
      if (badToken) invalidTokens.push(badToken);
    }
  });
  if (invalidTokens.length) {
    await Promise.all(invalidTokens.map((token) => cleanupInvalidPushToken(token)));
  }
  await db.collection("pushAudit").add({
    source,
    uid: targetUid,
    title: String(title || "").slice(0, 120),
    body: String(body || "").slice(0, 220),
    link: String(link || "").slice(0, 220),
    tokenCount: tokens.length,
    sent: res.successCount || 0,
    failed: res.failureCount || 0,
    firstErrorCode: firstErrorCode || null,
    errorCodes,
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  }).catch(() => {});
  return {
    success: true,
    sent: res.successCount || 0,
    failed: res.failureCount || 0,
    firstErrorCode
  };
}

// Push notifications: send to a user by uid
exports.sendPushToUser = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const targetUid = data?.uid;
  const title = (data?.title || 'FoCo Alert').toString();
  const body = (data?.body || '').toString().slice(0, 200);
  if (!targetUid || !body) throw new HttpsError('invalid-argument', 'uid and body required');
  const res = await sendPushToUid(targetUid, {
    title,
    body,
    link: (data?.link || '').toString(),
    source: "direct-callable"
  });
  await db.collection('pushDebug').doc(targetUid).set({
    lastSentAt: admin.firestore.FieldValue.serverTimestamp(),
    lastResult: {
      sent: res.sent || 0,
      failed: res.failed || 0,
      lastError: res.firstErrorCode || null
    }
  }, { merge: true });
  return { success: true, sent: res.sent || 0, failed: res.failed || 0 };
});

exports.pushBroadcastNow = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const claims = context.auth.token || {};
  const uid = String(context.auth.uid || "");
  let allowed =
    claims.staff === true ||
    claims.ceo === true ||
    claims.admin === true ||
    uid.startsWith("staff_") ||
    uid === CEO_UID;
  if (!allowed) {
    try {
      const memberSnap = await db.collection("members").doc(uid).get();
      const member = memberSnap.exists ? (memberSnap.data() || {}) : {};
      const role = String(member.role || "").toLowerCase();
      allowed = member.staff === true || role === "staff" || role === "ceo";
    } catch (_) {}
  }
  if (!allowed) throw new HttpsError("permission-denied", "Staff/CEO only");
  const title = String(data?.title || "FoCo Alert").trim().slice(0, 80);
  const body = String(data?.body || "").trim().slice(0, 160);
  if (!body) throw new HttpsError("invalid-argument", "body required");
  const link = String(data?.link || "/").trim().slice(0, 220);
  const source = String(data?.source || "manual").trim().slice(0, 60);
  return sendPushToAll({
    title,
    body,
    link,
    source,
    dedupeKey: String(data?.dedupeKey || "").trim().slice(0, 240),
    dedupeWindowMs: Number(data?.dedupeWindowMs || 10000) || 10000,
    force: data?.force === true
  });
});

exports.getPushDebugStatus = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const uid = context.auth.uid;
  const tokenSnap = await db.collection('pushTokens').doc(uid).collection('tokens').get();
  const tokenSuffixes = tokenSnap.docs
    .map(d => d.id)
    .filter(Boolean)
    .map(token => token.slice(-12));
  const debugSnap = await db.collection('pushDebug').doc(uid).get();
  const lastSentAt = debugSnap.exists && debugSnap.data().lastSentAt
    ? debugSnap.data().lastSentAt.toMillis()
    : null;
  const lastResult = debugSnap.exists ? (debugSnap.data().lastResult || null) : null;
  return {
    tokenCount: tokenSnap.size || 0,
    tokenSuffixes,
    lastSentAt,
    lastResult
  };
});

exports.pushOnAlertCreate = functions.firestore.document('alerts/{alertId}').onWrite(async (change, context) => {
  // Only notify for newly posted alerts (not edits/removals/expiry updates).
  if (!change.after.exists || change.before.exists) return null;
  const data = change.after.data() || {};
  const expiresMs = toMillisSafe(data.expiresAt);
  if (expiresMs && expiresMs <= Date.now()) return null;
  const venueName = getPushVenueName(data.venueId, data.venueName);
  const headline = (data.title || "Tonight's alert").toString().trim();
  const detail = (data.detail || "").toString().trim();
  const title = `Tonight Alert - ${venueName}`.slice(0, 80);
  const body = `${headline}${detail ? ` - ${detail}` : ""}`.slice(0, 160) || `New alert from ${venueName}.`;
  const dedupeKey = `alert-create:${String(data.venueId || "")}:${String(context?.params?.alertId || "")}`;
  return sendPushToAll({
    title,
    body,
    link: '/#alertCard',
    source: "alert-create",
    dedupeKey,
    dedupeWindowMs: 60000
  });
});

exports.pushOnVipDeal = functions.firestore.document('deals/{venueId}').onWrite(async (change, context) => {
  if (!change.after.exists) return null;
  const after = change.after.data() || {};
  const before = change.before.exists ? (change.before.data() || {}) : null;
  const afterExpiresMs = toMillisSafe(after.expiresAt);
  // Do not notify when staff expires/removes a deal.
  if (afterExpiresMs && afterExpiresMs <= Date.now()) {
    return null;
  }
  const beforeSig = before
    ? [
        String(before.title || "").trim(),
        String(before.detail || "").trim(),
        String(before.meta || "").trim(),
        Number(before.standardQty || 0),
        Number(before.vipQty || 0),
        toMillisSafe(before.expiresAt)
      ]
    : null;
  const afterSig = [
    String(after.title || "").trim(),
    String(after.detail || "").trim(),
    String(after.meta || "").trim(),
    Number(after.standardQty || 0),
    Number(after.vipQty || 0),
    afterExpiresMs
  ];
  if (beforeSig && JSON.stringify(beforeSig) === JSON.stringify(afterSig)) return null;
  const venueName = getPushVenueName(context.params.venueId, after.venueName);
  const title = (`VIP Deal - ${venueName}`).slice(0, 80);
  const body = ((after.detail || after.title || 'A VIP deal just dropped.').toString()).slice(0, 160);
  const dedupeKey = `vip-deal:${String(context?.params?.venueId || "")}`;
  return sendPushToAll({
    title,
    body,
    link: '/#alertCard',
    source: "vip-deal",
    dedupeKey,
    dedupeWindowMs: 60000
  });
});

async function claimPerkUpdatePushWindow(venueId, windowMs = 2 * 60 * 1000) {
  const normalizedVenue = String(venueId || "").trim().toLowerCase();
  if (!normalizedVenue) return false;
  const ref = db.collection("pushSignals").doc(`perkUpdate_${normalizedVenue}`);
  const now = Date.now();
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const lastSentMs = toMillisSafe(snap.exists ? snap.data()?.lastSentAt : null);
    if (lastSentMs && (now - lastSentMs) < windowMs) {
      return false;
    }
    tx.set(ref, {
      venueId: normalizedVenue,
      lastSentAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return true;
  });
}

exports.pushOnPerkUpdate = functions.firestore.document('venues/{venueId}/perks/{perkId}').onWrite(async (change, context) => {
  if (!change.after.exists) return null;
  const after = change.after.data() || {};
  const before = change.before.exists ? (change.before.data() || {}) : null;
  const afterSig = [
    String(after.label || "").trim(),
    Number(after.standardQty || 0),
    Number(after.vipQty || 0)
  ];
  const beforeSig = before
    ? [
        String(before.label || "").trim(),
        Number(before.standardQty || 0),
        Number(before.vipQty || 0)
      ]
    : null;
  // Notify only when a perk is posted/changed (not timestamp-only writes).
  // Allow either tier to be active; skip only if both are unavailable.
  if (!afterSig[0] || (afterSig[1] <= 0 && afterSig[2] <= 0)) return null;
  if (beforeSig && JSON.stringify(beforeSig) === JSON.stringify(afterSig)) return null;
  const venueId = context.params.venueId;
  const canSend = await claimPerkUpdatePushWindow(venueId);
  if (!canSend) return null;
  const venueName = getPushVenueName(venueId, after.venueName);
  const title = (`Perks Updated - ${venueName}`).slice(0, 80);
  const body = `${venueName} just updated this month's perks.`.slice(0, 160);
  const dedupeKey = `perk-update:${String(venueId || "").trim().toLowerCase()}`;
  return sendPushToAll({
    title,
    body,
    link: '/#alertCard',
    source: "perk-update",
    dedupeKey,
    dedupeWindowMs: 60000
  });
});

exports.pushOnAlertExpiringSoon = functions.pubsub.schedule('every 1 minutes').timeZone('America/Denver').onRun(async () => {
  const nowMs = Date.now();
  const windowStart = admin.firestore.Timestamp.fromMillis(nowMs + (9 * 60 * 1000) + (45 * 1000));
  const windowEnd = admin.firestore.Timestamp.fromMillis(nowMs + (10 * 60 * 1000) + (15 * 1000));
  const snap = await db.collection("alerts")
    .where("expiresAt", ">=", windowStart)
    .where("expiresAt", "<=", windowEnd)
    .limit(100)
    .get();
  if (snap.empty) return null;
  for (const docSnap of snap.docs) {
    const alertData = await db.runTransaction(async (tx) => {
      const fresh = await tx.get(docSnap.ref);
      if (!fresh.exists) return null;
      const data = fresh.data() || {};
      if (data.pushExpiringSoonAt) return null;
      const expiresMs = toMillisSafe(data.expiresAt);
      if (!expiresMs || expiresMs <= Date.now()) return null;
      tx.set(docSnap.ref, { pushExpiringSoonAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
      return data;
    });
    if (!alertData) continue;
    const venueName = getPushVenueName(alertData.venueId, alertData.venueName);
    const title = (`10 Minutes Left - ${venueName}`).slice(0, 80);
    const body = `Only 10 minutes remaining on Tonight's Alert at ${venueName}.`.slice(0, 160);
    await sendPushToAll({
      title,
      body,
      link: '/#alertCard',
      source: "alert-expiring"
    });
  }
  return null;
});

exports.pushUnusedVoucherReminder = functions.pubsub.schedule('every 60 minutes').timeZone('America/Denver').onRun(async () => {
  const now = new Date();
  const nowMs = now.getTime();
  const weekMs = 7 * 24 * 60 * 60 * 1000;
  const lower = nowMs + weekMs - (30 * 60 * 1000);
  const upper = nowMs + weekMs + (30 * 60 * 1000);
  const snap = await db.collection('members')
    .where('stripeSubscriptionId', '!=', null)
    .orderBy('stripeSubscriptionId')
    .limit(1000)
    .get();
  for (const docSnap of snap.docs) {
    const uid = docSnap.id;
    const memberData = docSnap.data() || {};
    if (memberData.revoked === true || memberData.paused === true) continue;
    const tier = normalizeVoucherTierForWallet(memberData);
    if (tier !== "standard" && tier !== "vip") continue;
    if (String(memberData.paymentStatus || "active").toLowerCase() !== "active") continue;
    const billing = (memberData.billing && typeof memberData.billing === "object") ? memberData.billing : {};
    const renewalDate = toDateSafe(billing.nextRenewal || memberData.nextRenewal || memberData.stripeCurrentPeriodEnd);
    if (!renewalDate) continue;
    const renewalMs = renewalDate.getTime();
    if (renewalMs < lower || renewalMs > upper) continue;
    const cycle = getVoucherCycleWindowForWallet(memberData, billing, now)?.current;
    const cycleKey = cycle?.start ? cycle.start.toISOString().slice(0, 10) : getMonthTokenFromDate(now);
    if (memberData.voucherReminderCycleKey === cycleKey) continue;
    let entries = [];
    try {
      const redSnap = await db.collection('members').doc(uid).collection('redemptions').get();
      entries = redSnap.docs.map((d) => d.data() || {});
    } catch (_) {
      entries = [];
    }
    const wallet = computeVoucherBalanceForWallet(memberData, entries, now, false);
    if (!Number.isFinite(wallet.remaining) || wallet.remaining <= 0 || wallet.paymentRequired) continue;
    const body = `You still have ${wallet.remaining} unused voucher${wallet.remaining === 1 ? "" : "s"} this month. Use them before they expire.`;
    const pushRes = await sendPushToUid(uid, {
      title: "Unused Vouchers Waiting",
      body: body.slice(0, 160),
      link: "/#rewards",
      source: "voucher-reminder"
    });
    if ((pushRes.sent || 0) > 0) {
      await docSnap.ref.set({
        voucherReminderCycleKey: cycleKey,
        voucherReminderSentAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
    }
  }
  return null;
});

// Weekly cleanup: delete old audit/rateLimit docs to save space
exports.weeklyCleanup = functions.pubsub.schedule('0 5 * * 0').timeZone('America/Denver').onRun(async () => {
  const cutoff = admin.firestore.Timestamp.fromDate(new Date(Date.now() - 30 * 24 * 60 * 60 * 1000));
  const collections = ['auditLogs', 'rateLimits', 'systemEmails'];
  for (const col of collections) {
    const snap = await db.collection(col).where('createdAt', '<', cutoff).limit(500).get();
    if (!snap.empty) {
      const batch = db.batch();
      snap.docs.forEach(d => batch.delete(d.ref));
      await batch.commit();
      console.log(`Cleaned ${snap.size} from ${col}`);
    }
  }
  return null;
});

// --- Payments & Billing (Stripe) ---
function priceForTier(tier = 'standard') {
  const map = {
    standard: 500, // $5.00
    vip: 1000      // $10.00
  };
  return map[tier] || map.standard;
}

const TOKEN_PACKS_STANDARD = {
  tokens_1: { perk: "tokens", amount: 1, priceCents: 300, priceLabel: "$3.00", label: "Redemption token" },
  tokens_2: { perk: "tokens", amount: 2, priceCents: 600, priceLabel: "$6.00", label: "Redemption tokens" },
  tokens_3: { perk: "tokens", amount: 3, priceCents: 900, priceLabel: "$9.00", label: "Redemption tokens" },
  tokens_4: { perk: "tokens", amount: 4, priceCents: 1200, priceLabel: "$12.00", label: "Redemption tokens" },
  tokens_5: { perk: "tokens", amount: 5, priceCents: 1500, priceLabel: "$15.00", label: "Redemption tokens" },
};

const TOKEN_PACKS_VIP = {
  tokens_1: { perk: "tokens", amount: 1, priceCents: 250, priceLabel: "$2.50", label: "Redemption token" },
  tokens_2: { perk: "tokens", amount: 2, priceCents: 500, priceLabel: "$5.00", label: "Redemption tokens" },
  tokens_3: { perk: "tokens", amount: 3, priceCents: 750, priceLabel: "$7.50", label: "Redemption tokens" },
  tokens_4: { perk: "tokens", amount: 4, priceCents: 1000, priceLabel: "$10.00", label: "Redemption tokens" },
  tokens_5: { perk: "tokens", amount: 5, priceCents: 1250, priceLabel: "$12.50", label: "Redemption tokens" },
};

const VOUCHER_PACKS = {
  standard: {
    drink: { perk: "drink", amount: 2, priceCents: 600, priceLabel: "$6.00", label: "$3 drink voucher" },
    shot: { perk: "shot", amount: 4, priceCents: 500, priceLabel: "$5.00", label: "$1 shot voucher" },
    cover: { perk: "cover", amount: 3, priceCents: 2000, priceLabel: "$20.00", label: "Skip Line + No Cover Charge" },
    ...TOKEN_PACKS_STANDARD,
  },
  vip: {
    drink: { perk: "drink", amount: 4, priceCents: 1000, priceLabel: "$10.00", label: "$3 drink voucher" },
    shot: { perk: "shot", amount: 4, priceCents: 500, priceLabel: "$5.00", label: "$1 shot voucher" },
    cover: { perk: "cover", amount: 3, priceCents: 1500, priceLabel: "$15.00", label: "Skip Line + No Cover Charge" },
    ...TOKEN_PACKS_VIP,
  },
};

function resolveVoucherPack(tier, packId) {
  const t = (tier || "standard").toLowerCase();
  const key = (packId || "").toLowerCase();
  const pack = (VOUCHER_PACKS[t] && VOUCHER_PACKS[t][key]) || (VOUCHER_PACKS.standard && VOUCHER_PACKS.standard[key]) || null;
  if (!pack) return null;
  return { id: key, tier: t, ...pack };
}

async function applyVoucherPack(uid, pack) {
  const ref = db.collection("members").doc(uid);
  if (pack.perk === "tokens") {
    let nextCount = pack.amount;
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const data = snap.exists ? snap.data() : {};
      const current = Number(data.extraRedemptionTokens || 0);
      nextCount = current + pack.amount;
      tx.set(ref, {
        extraRedemptionTokens: nextCount,
        lastVoucherPurchase: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    });
    return { extraRedemptionTokens: nextCount };
  }
  const updates = {
    [`extraVouchers.${pack.perk}`]: admin.firestore.FieldValue.increment(pack.amount),
    lastVoucherPurchase: admin.firestore.FieldValue.serverTimestamp(),
  };
  await ref.set(updates, { merge: true });
  const snap = await ref.get();
  const data = snap.exists ? snap.data() : {};
  return { extraVouchers: data.extraVouchers || {} };
}

const stripeSecrets = { secrets: ["STRIPE_SECRET", "STRIPE_PUBLISHABLE"] };
const STRIPE_PROMOS = {
  LAUNCH30: {
    couponId: "iydJPH3m",
    limit: 30,
  },
  FOCOFAM20: {
    promotionCodeId: "promo_1Sz1GSQ4Ij3ax7macXklpdje",
  },
};
const LAUNCH30_RESERVE_MS = 30 * 60 * 1000;
const stripeWebhookSecrets = { secrets: ["STRIPE_SECRET", "STRIPE_WEBHOOK_SECRET"] };

exports.redeemPoints = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const type = (data?.type || "").toString().toLowerCase();
  const points = Math.floor(Number(data?.points || 0));
  if (!points || points < 500 || points % 500 !== 0) {
    throw new HttpsError("invalid-argument", "Points must be in 500-point increments.");
  }
  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  if (isStripeExcluded(context.auth.token, memberDocData)) {
    throw new HttpsError("failed-precondition", "This account is not eligible for point redemptions.");
  }
  const tier = normalizeTierKey(memberDocData?.tier || memberDocData?.membershipTier || "");
  if (!["standard", "vip"].includes(tier)) {
    throw new HttpsError("failed-precondition", "Points are only available for Standard and VIP members.");
  }
  if (memberDocData?.paused || memberDocData?.revoked) {
    throw new HttpsError("failed-precondition", "Membership is inactive.");
  }
  const paymentStatus = String(memberDocData?.paymentStatus || "active").toLowerCase();
  if (paymentStatus !== "active") {
    throw new HttpsError("failed-precondition", "Payment must be active to redeem points.");
  }
  const available = Math.max(0, Number(memberDocData?.points || 0));
  if (available < points) {
    throw new HttpsError("failed-precondition", "Not enough points.");
  }

  if (type === "tokens") {
    const tokenGrant = (points / 500) * 2;
    const monthKey = getMonthTokenFromDate(new Date());
    const redemptionRef = memberRef.collection("pointsRedemptions").doc();
    const result = await db.runTransaction(async (tx) => {
      const snap = await tx.get(memberRef);
      const dataSnap = snap.exists ? snap.data() : {};
      const current = Math.max(0, Number(dataSnap.points || 0));
      if (current < points) throw new HttpsError("failed-precondition", "Not enough points.");
      const updates = {
        points: current - points,
        pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastPointsRedemptionAt: admin.firestore.FieldValue.serverTimestamp(),
        lastPointsRedemptionType: "tokens",
        pointsRedeemedTotal: admin.firestore.FieldValue.increment(points),
        [`pointsMonthlyTokenGrants.${monthKey}`]: admin.firestore.FieldValue.increment(tokenGrant)
      };
      tx.set(memberRef, updates, { merge: true });
      tx.set(redemptionRef, {
        type: "tokens",
        points,
        tokens: tokenGrant,
        monthKey,
        status: "applied",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        appliedAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
      return { points: current - points };
    });
    return { ok: true, type: "tokens", points: result.points, tokenGrant, monthKey };
  }

  if (type === "discount") {
    if (points > 2000) {
      throw new HttpsError("invalid-argument", "Maximum of 2000 points per discount.");
    }
    const creditCents = Math.max(0, Math.round(points / 10));
    if (creditCents <= 0) {
      throw new HttpsError("invalid-argument", "Invalid discount amount.");
    }
    const stripeCustomerId = memberDocData?.stripeCustomerId;
    if (!stripeCustomerId) {
      throw new HttpsError("failed-precondition", "Billing profile not found.");
    }
    const stripe = getStripeClient();
    const redemptionRef = memberRef.collection("pointsRedemptions").doc();
    let remainingPoints = available;
    try {
      await db.runTransaction(async (tx) => {
        const snap = await tx.get(memberRef);
        const dataSnap = snap.exists ? snap.data() : {};
        const current = Math.max(0, Number(dataSnap.points || 0));
        if (current < points) throw new HttpsError("failed-precondition", "Not enough points.");
        remainingPoints = current - points;
        tx.set(memberRef, {
          points: remainingPoints,
          pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
          lastPointsRedemptionAt: admin.firestore.FieldValue.serverTimestamp(),
          lastPointsRedemptionType: "discount",
          pointsRedeemedTotal: admin.firestore.FieldValue.increment(points)
        }, { merge: true });
        tx.set(redemptionRef, {
          type: "discount",
          points,
          creditCents,
          status: "pending",
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
      });

      const customer = await stripe.customers.retrieve(stripeCustomerId);
      const currentBalance = Number(customer?.balance || 0);
      const nextBalance = currentBalance - creditCents;
      await stripe.customers.update(stripeCustomerId, { balance: nextBalance });
      await redemptionRef.set({
        status: "applied",
        appliedAt: admin.firestore.FieldValue.serverTimestamp(),
        stripeCustomerId,
        creditCents,
        stripeBalanceAfter: nextBalance
      }, { merge: true });
      return { ok: true, type: "discount", points: remainingPoints, creditCents, balanceAfter: nextBalance };
    } catch (err) {
      await db.runTransaction(async (tx) => {
        const snap = await tx.get(memberRef);
        const dataSnap = snap.exists ? snap.data() : {};
        const current = Math.max(0, Number(dataSnap.points || 0));
        tx.set(memberRef, {
          points: current + points,
          pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        tx.set(redemptionRef, {
          status: "failed",
          failedAt: admin.firestore.FieldValue.serverTimestamp(),
          error: err?.message || "Stripe update failed"
        }, { merge: true });
      });
      throw new HttpsError("internal", "Could not apply discount. Points refunded.");
    }
  }

  throw new HttpsError("invalid-argument", "Unknown redemption type.");
});

function isCeoContext(context, profileData) {
  const email = (context?.auth?.token?.email || "").toLowerCase();
  const pass = (profileData?.passCode || "").toUpperCase();
  return (
    context?.auth?.token?.ceo === true ||
    email === "ceo@gmail.com" ||
    pass === CEO_PASS_ID ||
    profileData?.ceo === true
  );
}

function isCeoMemberDoc(data = {}, uid = "") {
  const pass = (data.passCode || "").toUpperCase();
  const email = (data.email || "").toLowerCase();
  const override = (data.membershipOverride || data.override || "").toString().toUpperCase();
  return (
    data.ceo === true ||
    uid === CEO_UID ||
    pass === CEO_PASS_ID ||
    email === CEO_EMAIL ||
    override.includes("CEO")
  );
}

async function deleteQueryInBatches(query, dryRun = false, batchSize = 400) {
  let total = 0;
  let snapshot = await query.limit(batchSize).get();
  while (!snapshot.empty) {
    total += snapshot.size;
    if (!dryRun) {
      const batch = db.batch();
      snapshot.docs.forEach(doc => batch.delete(doc.ref));
      await batch.commit();
    }
    if (snapshot.size < batchSize) break;
    const lastDoc = snapshot.docs[snapshot.docs.length - 1];
    snapshot = await query.startAfter(lastDoc).limit(batchSize).get();
  }
  return total;
}

function normalizeTierKey(tier = "standard") {
  const key = String(tier || "standard").toLowerCase();
  if (key === "vip") return "vip";
  return "standard";
}

function membershipTierLabel(tier = "standard") {
  return normalizeTierKey(tier).toUpperCase();
}

function isoFromUnix(seconds) {
  const value = Number(seconds || 0);
  if (!Number.isFinite(value) || value <= 0) return null;
  return new Date(value * 1000).toISOString();
}

function stripeStatusToPaymentStatus(status = "") {
  const key = String(status || "").toLowerCase();
  if (key === "active" || key === "trialing") return "active";
  if (key === "past_due" || key === "unpaid" || key === "incomplete") return "past_due";
  if (key === "canceled" || key === "incomplete_expired") return "canceled";
  return "active";
}

async function purgeAnonymousUsersInternal({ dryRun = false, limit = 1000 } = {}) {
  const page = await admin.auth().listUsers(Math.min(limit, 1000));
  let matchedUsers = 0;
  let deletedUsers = 0;
  let deletedMemberDocs = 0;
  let deletedUsernames = 0;

  for (const user of page.users) {
    const isAnon = (user.providerData || []).length === 0 && !user.email && !user.phoneNumber;
    if (!isAnon) continue;
    matchedUsers += 1;
    if (dryRun) continue;
    const uid = user.uid;
    const memberRef = db.collection('members').doc(uid);
    const memberSnap = await memberRef.get();
    const memberData = memberSnap.exists ? memberSnap.data() : {};
    if (isCeoMemberDoc(memberData, uid)) continue;
    if (memberSnap.exists) {
      await memberRef.delete();
      deletedMemberDocs += 1;
    }
    const username = (memberData.username || "").toString().trim().toLowerCase();
    if (username) {
      await db.collection('usernames').doc(username).delete();
      deletedUsernames += 1;
    }
    await admin.auth().deleteUser(uid);
    deletedUsers += 1;
  }
  return { matchedUsers, deletedUsers, deletedMemberDocs, deletedUsernames };
}

function stripeExcludedError() {
  return new HttpsError(
    "failed-precondition",
    "Stripe disabled for this account type.",
    { reason: "CEO_OR_CEO_FREE" }
  );
}

function isStripeExcluded(token = {}, memberDocData = {}) {
  const claimEmail = (token?.email || "").toLowerCase();
  const memberEmail = (memberDocData?.email || "").toLowerCase();
  const email = claimEmail || memberEmail;
  const overrideRaw = memberDocData?.membershipOverride
    || memberDocData?.override
    || memberDocData?.membership_override
    || memberDocData?.membershipTierOverride
    || "";
  const override = String(overrideRaw || "").toUpperCase();
  return (
    token?.ceo === true
    || email === "ceo@gmail.com"
    || memberDocData?.ceo === true
    || memberDocData?.freeMembership === true
    || override === "CEO_FREE"
  );
}

function normalizePromoCodeInput(raw) {
  return (raw || "").toString().trim().toUpperCase();
}

function isNewMembership(memberDocData = {}) {
  return !(
    memberDocData?.membershipActivatedAt
    || memberDocData?.stripeSubscriptionId
    || memberDocData?.lastCharge
  );
}

async function reserveLaunch30(uid) {
  const ref = db.collection("promoCounters").doc("launch30");
  const now = Date.now();
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const data = snap.exists ? snap.data() : {};
    const usedUids = data.usedUids || {};
    const reservedUidsRaw = data.reservedUids || {};
    const reservedUids = {};
    Object.entries(reservedUidsRaw).forEach(([key, ts]) => {
      if (typeof ts === "number" && now - ts < LAUNCH30_RESERVE_MS) {
        reservedUids[key] = ts;
      }
    });
    const usedCount = Object.keys(usedUids).length;
    const reservedCount = Object.keys(reservedUids).length;
    if (usedUids[uid]) {
      return { eligible: false, reason: "already_used", usedCount, reservedCount };
    }
    if (reservedUids[uid]) {
      return { eligible: true, alreadyReserved: true, usedCount, reservedCount };
    }
    if (usedCount + reservedCount >= STRIPE_PROMOS.LAUNCH30.limit) {
      return { eligible: false, reason: "limit_reached", usedCount, reservedCount };
    }
    reservedUids[uid] = now;
    tx.set(ref, {
      usedUids,
      reservedUids,
      usedCount,
      reservedCount: reservedCount + 1,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return { eligible: true, reserved: true, usedCount, reservedCount: reservedCount + 1 };
  });
}

async function finalizeLaunch30(uid, success) {
  if (!uid) return;
  const ref = db.collection("promoCounters").doc("launch30");
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (!snap.exists) return;
    const data = snap.data() || {};
    const usedUids = data.usedUids || {};
    const reservedUids = data.reservedUids || {};
    let changed = false;
    if (success && !usedUids[uid]) {
      usedUids[uid] = Date.now();
      changed = true;
    }
    if (reservedUids[uid]) {
      delete reservedUids[uid];
      changed = true;
    }
    if (!changed) return;
    tx.set(ref, {
      usedUids,
      reservedUids,
      usedCount: Object.keys(usedUids).length,
      reservedCount: Object.keys(reservedUids).length,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  });
}

async function resolveMembershipPromo({ uid, memberDocData, promoCodeInput }) {
  const promoInput = normalizePromoCodeInput(promoCodeInput);
  const isNew = isNewMembership(memberDocData);
  if (!isNew && promoInput) {
    throw new HttpsError("failed-precondition", "Promo codes are only available for new memberships.");
  }
  if (promoInput) {
    if (promoInput === "FOCOFAM20") {
      return {
        discount: { promotion_code: STRIPE_PROMOS.FOCOFAM20.promotionCodeId },
        promoTag: "focofam20",
      };
    }
    if (promoInput === "LAUNCH30") {
      const launch = await reserveLaunch30(uid);
      if (!launch.eligible) {
        throw new HttpsError("failed-precondition", "Launch promo has reached the redemption limit.");
      }
      return {
        discount: { coupon: STRIPE_PROMOS.LAUNCH30.couponId },
        promoTag: "launch30",
      };
    }
    throw new HttpsError("invalid-argument", "Invalid promo code.");
  }
  if (isNew) {
    const launch = await reserveLaunch30(uid);
    if (launch.eligible) {
      return {
        discount: { coupon: STRIPE_PROMOS.LAUNCH30.couponId },
        promoTag: "launch30",
      };
    }
  }
  return { discount: null, promoTag: null };
}

async function getMemberContext(uid) {
  const ref = db.collection("members").doc(uid);
  const snap = await ref.get();
  const data = snap.exists ? snap.data() : {};
  return { ref, data };
}

function assertStripeAllowed(context, memberDocData) {
  if (!context?.auth) {
    throw new HttpsError("unauthenticated", "Auth required");
  }
  if (isStripeExcluded(context.auth.token, memberDocData)) {
    throw stripeExcludedError();
  }
}

async function ensureStripeCustomer({ stripe, memberRef, memberDocData, uid, email, token }) {
  if (isStripeExcluded(token, memberDocData)) {
    throw stripeExcludedError();
  }
  if (memberDocData?.stripeCustomerId) return memberDocData.stripeCustomerId;
  const customer = await stripe.customers.create({
    email: email || undefined,
    metadata: { uid }
  });
  await memberRef.set({
    stripeCustomerId: customer.id,
    billingProvider: "stripe",
  }, { merge: true });
  return customer.id;
}

function subscriptionPriceDataForTier(tier) {
  const key = normalizeTierKey(tier);
  return {
    currency: "usd",
    unit_amount: priceForTier(key),
    recurring: { interval: "month" },
    product_data: {
      name: `FoCo After Dark ${membershipTierLabel(key)}`,
    },
  };
}

async function retrieveStripeSubscription(stripe, subscriptionId) {
  if (!subscriptionId) return null;
  try {
    return await stripe.subscriptions.retrieve(subscriptionId, {
      expand: ["latest_invoice.payment_intent", "items.data.price"],
    });
  } catch (err) {
    console.warn("Failed to retrieve Stripe subscription", subscriptionId, err?.message || err);
    return null;
  }
}

async function upsertMembershipSubscription({ stripe, memberRef, memberDocData, uid, email, tier, token, discount, promoTag }) {
  const normalizedTier = normalizeTierKey(tier);
  const customerId = await ensureStripeCustomer({
    stripe,
    memberRef,
    memberDocData,
    uid,
    email,
    token,
  });

  let subscription = await retrieveStripeSubscription(stripe, memberDocData?.stripeSubscriptionId);
  const hasActiveSubscription = subscription && !["canceled", "incomplete_expired"].includes(subscription.status);
  const promoMeta = promoTag ? { promo: promoTag } : {};

  if (hasActiveSubscription) {
    const currentItem = subscription.items?.data?.[0];
    const currentTier = normalizeTierKey(subscription.metadata?.tier || memberDocData?.tier || normalizedTier);
    if (currentItem && currentTier !== normalizedTier) {
      subscription = await stripe.subscriptions.update(subscription.id, {
        cancel_at_period_end: false,
        proration_behavior: "none",
        billing_cycle_anchor: "unchanged",
        metadata: { uid, tier: normalizedTier, ...promoMeta },
        items: [{
          id: currentItem.id,
          price_data: subscriptionPriceDataForTier(normalizedTier),
        }],
        expand: ["latest_invoice.payment_intent", "items.data.price"],
      });
    } else if (subscription.metadata?.tier !== normalizedTier) {
      subscription = await stripe.subscriptions.update(subscription.id, {
        metadata: { uid, tier: normalizedTier, ...promoMeta },
        expand: ["latest_invoice.payment_intent", "items.data.price"],
      });
    }
  } else {
    subscription = await stripe.subscriptions.create({
      customer: customerId,
      items: [{ price_data: subscriptionPriceDataForTier(normalizedTier) }],
      payment_behavior: "default_incomplete",
      payment_settings: { save_default_payment_method: "on_subscription" },
      metadata: { uid, tier: normalizedTier, ...promoMeta },
      discounts: discount ? [discount] : undefined,
      expand: ["latest_invoice.payment_intent", "items.data.price"],
    });
  }

  const paymentIntent = subscription?.latest_invoice?.payment_intent || null;
  const currentPeriodEndIso = isoFromUnix(subscription?.current_period_end);
  const updatePayload = {
    tier: normalizedTier,
    membershipTier: membershipTierLabel(normalizedTier),
    membershipStatus: subscription?.status || "active",
    paymentStatus: stripeStatusToPaymentStatus(subscription?.status),
    billingProvider: "stripe",
    stripeCustomerId: subscription?.customer || customerId,
    stripeSubscriptionId: subscription?.id || null,
    currentPeriodEnd: currentPeriodEndIso,
    nextRenewal: currentPeriodEndIso,
    cancelAtPeriodEnd: subscription?.cancel_at_period_end === true,
    lastStripeEvent: "subscription_upsert",
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  await memberRef.set(updatePayload, { merge: true });

  return { subscription, paymentIntent };
}

exports.createMembershipPaymentIntent = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const tier = normalizeTierKey((data?.tier || 'standard').toString());
  const promoCodeInput = (data?.promoCode || "").toString();
  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  assertStripeAllowed(context, memberDocData);
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');

  const promoContext = await resolveMembershipPromo({
    uid,
    memberDocData,
    promoCodeInput,
  });

  const { subscription, paymentIntent } = await upsertMembershipSubscription({
    stripe,
    memberRef,
    memberDocData,
    uid,
    email,
    tier,
    token: context.auth.token,
    discount: promoContext.discount,
    promoTag: promoContext.promoTag,
  });

  if (!paymentIntent?.client_secret) {
    return {
      publishableKey: publishable,
      subscriptionId: subscription?.id || null,
      tier,
      alreadyActive: true,
    };
  }
  return {
    clientSecret: paymentIntent.client_secret,
    publishableKey: publishable,
    subscriptionId: subscription?.id || null,
    tier,
  };
});

exports.confirmMembershipActivation = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const paymentIntentId = (data?.paymentIntentId || '').toString();
  if (!paymentIntentId) throw new HttpsError('invalid-argument', 'paymentIntentId required');
  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  assertStripeAllowed(context, memberDocData);
  const stripe = getStripeClient();
  const intent = await stripe.paymentIntents.retrieve(paymentIntentId);
  if (intent.status !== 'succeeded') throw new HttpsError('failed-precondition', 'Payment not successful');
  const invoiceId = intent.invoice;
  let subscription = intent.subscription ? await retrieveStripeSubscription(stripe, intent.subscription) : null;
  if (!subscription && invoiceId) {
    const invoice = await stripe.invoices.retrieve(invoiceId);
    if (invoice?.subscription) {
      subscription = await retrieveStripeSubscription(stripe, invoice.subscription);
    }
  }
  const tier = normalizeTierKey(intent.metadata?.tier || memberDocData?.tier || 'standard');
  const currentPeriodEndIso = isoFromUnix(subscription?.current_period_end);
  const promoTag = (subscription?.metadata?.promo || intent.metadata?.promo || "").toString().toLowerCase();
  const updates = {
    tier,
    membershipTier: membershipTierLabel(tier),
    billingProvider: "stripe",
    membershipStatus: subscription?.status || "active",
    paymentStatus: stripeStatusToPaymentStatus(subscription?.status || "active"),
    stripeCustomerId: intent.customer || subscription?.customer || memberDocData?.stripeCustomerId || null,
    stripeSubscriptionId: subscription?.id || memberDocData?.stripeSubscriptionId || null,
    defaultPaymentMethodId: intent.payment_method || memberDocData?.defaultPaymentMethodId || null,
    lastCharge: new Date().toISOString(),
    membershipActivatedAt: memberDocData?.membershipActivatedAt || new Date().toISOString(),
    currentPeriodEnd: currentPeriodEndIso,
    nextRenewal: currentPeriodEndIso,
    lastStripeEvent: "membership_confirmed",
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  await memberRef.set(updates, { merge: true });
  if (promoTag === "launch30") {
    await finalizeLaunch30(uid, true);
  }
  return { ok: true, tier, subscriptionId: updates.stripeSubscriptionId };
});

function nextMonthlyRenewalISO(fromDate = new Date()) {
  const next = new Date(fromDate);
  next.setMonth(next.getMonth() + 1);
  return next.toISOString();
}

async function findMemberByStripeCustomerId(customerId) {
  if (!customerId) return null;
  try {
    const snap = await db.collection("members")
      .where("stripeCustomerId", "==", customerId)
      .limit(1)
      .get();
    if (snap.empty) return null;
    const docSnap = snap.docs[0];
    return { uid: docSnap.id, ref: docSnap.ref, data: docSnap.data() || {} };
  } catch (err) {
    console.warn("findMemberByStripeCustomerId failed", customerId, err?.message || err);
    return null;
  }
}

async function resolveMemberContextFromStripeObject(obj = {}) {
  const uidFromMeta = (obj?.metadata?.uid || "").toString();
  if (uidFromMeta) {
    const ctx = await getMemberContext(uidFromMeta);
    return { uid: uidFromMeta, ref: ctx.ref, data: ctx.data };
  }
  const customerId = (obj?.customer || "").toString();
  return findMemberByStripeCustomerId(customerId);
}

async function retrieveSubscriptionFromIntent(stripe, intent) {
  if (!intent) return null;
  const subId = (intent.subscription || "").toString();
  if (subId) {
    return retrieveStripeSubscription(stripe, subId);
  }
  const invoiceId = (intent.invoice || "").toString();
  if (!invoiceId) return null;
  try {
    const invoice = await stripe.invoices.retrieve(invoiceId);
    const invoiceSubId = (invoice?.subscription || "").toString();
    if (!invoiceSubId) return null;
    return retrieveStripeSubscription(stripe, invoiceSubId);
  } catch (err) {
    console.warn("Failed to resolve subscription from invoice", invoiceId, err?.message || err);
    return null;
  }
}

async function applyStripeSubscriptionUpdate(subscription, memberCtx, eventType = "subscription_update") {
  if (!subscription || !memberCtx?.uid || !memberCtx?.ref) return;
  const { uid, ref: memberRef, data: memberDocData } = memberCtx;
  if (isStripeExcluded({}, memberDocData)) {
    await memberRef.set({
      billingProvider: "none",
      membershipStatus: "active",
      paymentStatus: "active",
      cancelAtPeriodEnd: false,
      stripeCustomerId: admin.firestore.FieldValue.delete(),
      stripeSubscriptionId: admin.firestore.FieldValue.delete(),
      lastStripeEvent: `${eventType}:excluded`,
      lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return;
  }

  const tier = normalizeTierKey(subscription?.metadata?.tier || memberDocData?.tier || "standard");
  const currentPeriodEndIso = isoFromUnix(subscription?.current_period_end);
  const updates = {
    tier,
    membershipTier: membershipTierLabel(tier),
    billingProvider: "stripe",
    membershipStatus: subscription?.status || "active",
    paymentStatus: stripeStatusToPaymentStatus(subscription?.status),
    stripeCustomerId: subscription?.customer || memberDocData?.stripeCustomerId || null,
    stripeSubscriptionId: subscription?.id || memberDocData?.stripeSubscriptionId || null,
    currentPeriodEnd: currentPeriodEndIso,
    nextRenewal: currentPeriodEndIso,
    cancelAtPeriodEnd: subscription?.cancel_at_period_end === true,
    canceledAt: isoFromUnix(subscription?.canceled_at) || null,
    lastStripeEvent: eventType,
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  await memberRef.set(updates, { merge: true });
}

async function applyStripePaymentIntentUpdate(intent, status) {
  const uid = (intent?.metadata?.uid || "").toString();
  if (!uid) return;
  const memberCtx = await getMemberContext(uid);
  if (isStripeExcluded({}, memberCtx.data)) {
    await memberCtx.ref.set({
      billingProvider: "none",
      membershipStatus: "active",
      paymentStatus: "active",
      stripeCustomerId: admin.firestore.FieldValue.delete(),
      stripeSubscriptionId: admin.firestore.FieldValue.delete(),
      lastStripeEvent: `payment_intent.${status}:excluded`,
      lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return;
  }
  const updates = {
    lastStripeEvent: status,
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp()
  };
  if (status === "succeeded") {
    const tier = normalizeTierKey((intent?.metadata?.tier || memberCtx.data?.tier || "standard").toString());
    if (tier) updates.tier = tier;
    updates.membershipTier = membershipTierLabel(tier);
    updates.billingProvider = "stripe";
    updates.paymentStatus = "active";
    updates.lastCharge = new Date().toISOString();
    try {
      const stripe = getStripeClient();
      const subscription = await retrieveSubscriptionFromIntent(stripe, intent);
      if (subscription) {
        const periodEndIso = isoFromUnix(subscription.current_period_end);
        updates.membershipStatus = subscription.status || "active";
        updates.paymentStatus = stripeStatusToPaymentStatus(subscription.status);
        updates.currentPeriodEnd = periodEndIso;
        updates.nextRenewal = periodEndIso;
        updates.stripeSubscriptionId = subscription.id;
        updates.cancelAtPeriodEnd = subscription.cancel_at_period_end === true;
      } else {
        updates.nextRenewal = nextMonthlyRenewalISO();
      }
    } catch (err) {
      console.warn("Failed to sync subscription from payment intent", err?.message || err);
      updates.nextRenewal = nextMonthlyRenewalISO();
    }
    if (intent?.payment_method) updates.defaultPaymentMethodId = intent.payment_method;
    if (intent?.customer) updates.stripeCustomerId = intent.customer;
  }
  if (status === "failed" || status === "canceled") {
    updates.paymentStatus = "past_due";
    updates.lastPaymentError = intent?.last_payment_error?.message || "Payment failed";
    updates.lastPaymentErrorAt = admin.firestore.FieldValue.serverTimestamp();
  }
  await db.collection("members").doc(uid).set(updates, { merge: true });
}

exports.stripeWebhook = functions.runWith(stripeWebhookSecrets).https.onRequest(async (req, res) => {
  if (req.method !== "POST") {
    res.status(405).send("Method not allowed");
    return;
  }
  const stripe = getStripeClient();
  const sig = req.headers["stripe-signature"];
  const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.rawBody, sig, webhookSecret);
  } catch (err) {
    console.warn("Stripe webhook signature failed", err?.message || err);
    res.status(400).send("Webhook signature failed");
    return;
  }
  try {
    const intent = event?.data?.object;
    if (event.type === "payment_intent.succeeded") {
      await applyStripePaymentIntentUpdate(intent, "succeeded");
    } else if (event.type === "payment_intent.payment_failed") {
      await applyStripePaymentIntentUpdate(intent, "failed");
    } else if (event.type === "payment_intent.canceled") {
      await applyStripePaymentIntentUpdate(intent, "canceled");
    } else if (event.type === "setup_intent.succeeded") {
      const uid = (intent?.metadata?.uid || "").toString();
      if (uid) {
        const updates = {
          lastStripeEvent: "setup_intent.succeeded",
          lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp()
        };
        if (intent?.payment_method) updates.defaultPaymentMethodId = intent.payment_method;
        if (intent?.customer) updates.stripeCustomerId = intent.customer;
        await db.collection("members").doc(uid).set(updates, { merge: true });
      }
    } else if (event.type === "customer.subscription.created"
      || event.type === "customer.subscription.updated"
      || event.type === "customer.subscription.deleted") {
      const memberCtx = await resolveMemberContextFromStripeObject(intent);
      await applyStripeSubscriptionUpdate(intent, memberCtx, event.type);
    } else if (event.type === "invoice.payment_succeeded"
      || event.type === "invoice.payment_failed") {
      const memberCtx = await resolveMemberContextFromStripeObject(intent);
      if (memberCtx?.ref && !isStripeExcluded({}, memberCtx.data)) {
        const paid = event.type === "invoice.payment_succeeded";
        const periodEnd = intent?.lines?.data?.[0]?.period?.end;
        const periodEndIso = isoFromUnix(periodEnd) || memberCtx.data?.nextRenewal || null;
        const invoiceUpdates = {
          billingProvider: "stripe",
          lastInvoiceId: intent?.id || null,
          lastInvoiceStatus: intent?.status || null,
          lastInvoicePaid: paid,
          lastStripeEvent: event.type,
          lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
        };
        if (paid) {
          invoiceUpdates.paymentStatus = "active";
          invoiceUpdates.membershipStatus = "active";
          invoiceUpdates.lastCharge = new Date().toISOString();
          if (periodEndIso) {
            invoiceUpdates.currentPeriodEnd = periodEndIso;
            invoiceUpdates.nextRenewal = periodEndIso;
          }
        } else {
          invoiceUpdates.paymentStatus = "past_due";
          invoiceUpdates.membershipStatus = "past_due";
        }
        await memberCtx.ref.set(invoiceUpdates, { merge: true });
      }
    } else if (event.type === "checkout.session.completed") {
      const memberCtx = await resolveMemberContextFromStripeObject(intent);
      if (memberCtx?.ref && isStripeExcluded({}, memberCtx.data)) {
        await memberCtx.ref.set({
          billingProvider: "none",
          membershipStatus: "active",
          paymentStatus: "active",
          stripeCustomerId: admin.firestore.FieldValue.delete(),
          stripeSubscriptionId: admin.firestore.FieldValue.delete(),
          lastStripeEvent: "checkout.session.completed:excluded",
          lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
        }, { merge: true });
      }
    }
  } catch (err) {
    console.warn("Stripe webhook handler failed", err?.message || err);
    res.status(500).send("Webhook handler failed");
    return;
  }
  res.json({ received: true });
});

exports.createSetupIntent = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  assertStripeAllowed(context, memberDocData);
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');
  const customerId = await ensureStripeCustomer({
    stripe,
    memberRef,
    memberDocData,
    uid,
    email,
    token: context.auth.token,
  });
  const setupIntent = await stripe.setupIntents.create({
    customer: customerId,
    payment_method_types: ['card'],
    metadata: { uid }
  });
  return { clientSecret: setupIntent.client_secret, publishableKey: publishable };
});

exports.createBillingPortalSession = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  assertStripeAllowed(context, memberDocData);
  const stripe = getStripeClient();
  const customerId = await ensureStripeCustomer({
    stripe,
    memberRef,
    memberDocData,
    uid,
    email,
    token: context.auth.token,
  });
  const returnUrl = (data?.returnUrl || 'https://foco-after-dark.web.app').toString();
  const session = await stripe.billingPortal.sessions.create({
    customer: customerId,
    return_url: returnUrl
  });
  return { url: session.url };
});

exports.chargeMembershipOnFile = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const tier = normalizeTierKey((data?.tier || 'standard').toString());
  const promoCodeInput = (data?.promoCode || "").toString();

  const { ref: memberRef, data: profile } = await getMemberContext(uid);
  assertStripeAllowed(context, profile);
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');

  const promoContext = await resolveMembershipPromo({
    uid,
    memberDocData: profile,
    promoCodeInput,
  });
  const promoMeta = promoContext.promoTag ? { promo: promoContext.promoTag } : {};

  const customerId = await ensureStripeCustomer({
    stripe,
    memberRef,
    memberDocData: profile,
    uid,
    email,
    token: context.auth.token,
  });
  let defaultPm = profile.defaultPaymentMethodId || null;
  if (!defaultPm) {
    const customer = await stripe.customers.retrieve(customerId);
    defaultPm = customer?.invoice_settings?.default_payment_method || null;
    if (defaultPm) {
      await memberRef.set({ defaultPaymentMethodId: defaultPm }, { merge: true });
    }
  }
  if (!defaultPm) {
    throw new HttpsError('failed-precondition', 'No card on file');
  }

  let subscription = await retrieveStripeSubscription(stripe, profile?.stripeSubscriptionId);
  const hasActiveSubscription = subscription && !["canceled", "incomplete_expired"].includes(subscription.status);
  if (hasActiveSubscription) {
    const currentTier = normalizeTierKey(subscription.metadata?.tier || profile?.tier || tier);
    const sameTierActive = currentTier === tier && subscription.status === "active" && subscription.cancel_at_period_end !== true;
    if (sameTierActive) {
      const currentPeriodEndIso = isoFromUnix(subscription?.current_period_end);
      await memberRef.set({
        tier,
        membershipTier: membershipTierLabel(tier),
        billingProvider: "stripe",
        membershipStatus: subscription.status || "active",
        paymentStatus: stripeStatusToPaymentStatus(subscription.status || "active"),
        stripeCustomerId: subscription.customer || customerId,
        stripeSubscriptionId: subscription.id,
        currentPeriodEnd: currentPeriodEndIso,
        nextRenewal: currentPeriodEndIso,
        cancelAtPeriodEnd: false,
        lastStripeEvent: "chargeMembershipOnFile:already_active",
        lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return { ok: true, tier, subscriptionId: subscription.id, alreadyActive: true };
    }
    const currentItem = subscription.items?.data?.[0];
    if (currentItem) {
      subscription = await stripe.subscriptions.update(subscription.id, {
        cancel_at_period_end: false,
        proration_behavior: "none",
        billing_cycle_anchor: "unchanged",
        default_payment_method: defaultPm,
        metadata: { uid, tier, ...promoMeta },
        items: [{
          id: currentItem.id,
          price_data: subscriptionPriceDataForTier(tier),
        }],
        expand: ["latest_invoice.payment_intent", "items.data.price"],
      });
    }
  } else {
    subscription = await stripe.subscriptions.create({
      customer: customerId,
      default_payment_method: defaultPm,
      items: [{ price_data: subscriptionPriceDataForTier(tier) }],
      payment_behavior: "default_incomplete",
      payment_settings: { save_default_payment_method: "on_subscription" },
      metadata: { uid, tier, ...promoMeta },
      discounts: promoContext.discount ? [promoContext.discount] : undefined,
      expand: ["latest_invoice.payment_intent", "items.data.price"],
    });
  }

  const paymentIntent = subscription?.latest_invoice?.payment_intent || null;
  if (paymentIntent?.status === "requires_action" && paymentIntent.client_secret) {
    return {
      requiresAction: true,
      clientSecret: paymentIntent.client_secret,
      publishableKey: publishable,
      paymentIntentId: paymentIntent.id,
      subscriptionId: subscription.id,
      tier,
    };
  }
  if (paymentIntent && paymentIntent.status !== "succeeded") {
    throw new HttpsError("failed-precondition", "Payment did not complete");
  }

  const currentPeriodEndIso = isoFromUnix(subscription?.current_period_end);
  await memberRef.set({
    tier,
    membershipTier: membershipTierLabel(tier),
    billingProvider: "stripe",
    membershipStatus: subscription?.status || "active",
    paymentStatus: stripeStatusToPaymentStatus(subscription?.status || "active"),
    stripeCustomerId: customerId,
    stripeSubscriptionId: subscription?.id || profile?.stripeSubscriptionId || null,
    defaultPaymentMethodId: defaultPm || paymentIntent?.payment_method || null,
    currentPeriodEnd: currentPeriodEndIso,
    nextRenewal: currentPeriodEndIso,
    lastCharge: paymentIntent ? new Date().toISOString() : (profile?.lastCharge || null),
    membershipActivatedAt: profile?.membershipActivatedAt || new Date().toISOString(),
    lastStripeEvent: "chargeMembershipOnFile",
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
  if (promoContext.promoTag === "launch30") {
    await finalizeLaunch30(uid, true);
  }
  return { ok: true, tier, subscriptionId: subscription?.id || null };
});

exports.createVoucherPaymentIntent = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const packId = (data?.packId || '').toString().toLowerCase();
  const { ref: memberRef, data: profile } = await getMemberContext(uid);
  assertStripeAllowed(context, profile);
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');
  const tier = (profile.tier || 'standard').toString();
  const pack = resolveVoucherPack(tier, packId);
  if (!pack) throw new HttpsError('invalid-argument', 'Unknown voucher pack');
  const customerId = await ensureStripeCustomer({
    stripe,
    memberRef,
    memberDocData: profile,
    uid,
    email,
    token: context.auth.token,
  });
  const intent = await stripe.paymentIntents.create({
    amount: pack.priceCents,
    currency: 'usd',
    customer: customerId,
    payment_method_types: ['card'],
    setup_future_usage: 'off_session',
    metadata: { uid, packId: pack.id, tier, perk: pack.perk }
  });
  return {
    clientSecret: intent.client_secret,
    publishableKey: publishable,
    pack: { id: pack.id, amount: pack.amount, perk: pack.perk, priceLabel: pack.priceLabel, label: pack.label }
  };
});

exports.chargeVoucherOnFile = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const packId = (data?.packId || '').toString().toLowerCase();
  const { ref: memberRef, data: profile } = await getMemberContext(uid);
  assertStripeAllowed(context, profile);
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');
  const tier = (profile.tier || 'standard').toString();
  const pack = resolveVoucherPack(tier, packId);
  if (!pack) throw new HttpsError('invalid-argument', 'Unknown voucher pack');
  const customerId = await ensureStripeCustomer({
    stripe,
    memberRef,
    memberDocData: profile,
    uid,
    email,
    token: context.auth.token,
  });
  let defaultPm = profile.defaultPaymentMethodId || null;
  if (!defaultPm) {
    const customer = await stripe.customers.retrieve(customerId);
    defaultPm = customer?.invoice_settings?.default_payment_method || null;
    if (defaultPm) {
      await memberRef.set({ defaultPaymentMethodId: defaultPm }, { merge: true });
    }
  }
  if (!defaultPm) throw new HttpsError('failed-precondition', 'No card on file');
  const intent = await stripe.paymentIntents.create({
    amount: pack.priceCents,
    currency: 'usd',
    customer: customerId,
    payment_method: defaultPm,
    confirm: true,
    off_session: false,
    payment_method_types: ['card'],
    setup_future_usage: 'off_session',
    metadata: { uid, packId: pack.id, tier, perk: pack.perk }
  });
  if (intent.status === 'requires_action' && intent.client_secret) {
    return {
      requiresAction: true,
      clientSecret: intent.client_secret,
      publishableKey: publishable,
      paymentIntentId: intent.id,
      pack: { id: pack.id, amount: pack.amount, perk: pack.perk, priceLabel: pack.priceLabel, label: pack.label }
    };
  }
  if (intent.status !== 'succeeded') {
    throw new HttpsError('failed-precondition', 'Payment did not complete');
  }
  return {
    paymentIntentId: intent.id,
    publishableKey: publishable,
    pack: { id: pack.id, amount: pack.amount, perk: pack.perk, priceLabel: pack.priceLabel, label: pack.label }
  };
});

exports.confirmVoucherPurchase = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const paymentIntentId = (data?.paymentIntentId || '').toString();
  if (!paymentIntentId) throw new HttpsError('invalid-argument', 'paymentIntentId required');
  const memberCtx = await getMemberContext(uid);
  assertStripeAllowed(context, memberCtx.data);
  const stripe = getStripeClient();
  const intent = await stripe.paymentIntents.retrieve(paymentIntentId);
  if (intent.status !== 'succeeded') throw new HttpsError('failed-precondition', 'Payment not successful');
  const metaUid = (intent.metadata?.uid || '').toString();
  if (metaUid && metaUid !== uid) throw new HttpsError('permission-denied', 'Payment does not belong to current user');
  const tier = (intent.metadata?.tier || 'standard').toString();
  const packId = (intent.metadata?.packId || '').toString();
  const pack = resolveVoucherPack(tier, packId) || resolveVoucherPack('standard', packId);
  if (!pack) throw new HttpsError('failed-precondition', 'Pack missing');

  const packResult = await applyVoucherPack(uid, pack);
  const memberRef = db.collection('members').doc(uid);
  if (intent.payment_method) {
    await memberRef.set({ defaultPaymentMethodId: intent.payment_method }, { merge: true });
  }
  return {
    ok: true,
    pack: { id: pack.id, amount: pack.amount, perk: pack.perk, priceLabel: pack.priceLabel, label: pack.label },
    ...packResult
  };
});

exports.setMemberPaused = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const targetUid = (data?.uid || "").toString();
  const paused = data?.paused === true;
  if (!targetUid) throw new HttpsError('invalid-argument', 'uid required');
  const targetRef = db.collection('members').doc(targetUid);
  const targetSnap = await targetRef.get();
  const targetData = targetSnap.exists ? targetSnap.data() : {};
  if (!isCeoContext(context, targetData)) throw new HttpsError('permission-denied', 'CEO only');
  if (targetData.ceo === true || (targetData.passCode || '').toUpperCase() === CEO_PASS_ID) {
    throw new HttpsError('failed-precondition', 'Cannot pause CEO');
  }
  const update = {
    paymentStatus: paused ? 'paused' : (targetData.paymentStatus || 'active'),
    paused: paused
  };
  if (paused) {
    update.pausedAt = new Date().toISOString();
  } else {
    update.pausedAt = null;
    update.resumedAt = new Date().toISOString();
  }
  await targetRef.set(update, { merge: true });
  return { ok: true, paused };
});

exports.getMembersSummary = functions.https.onCall(async (data, context) => {
  try {
    if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
    const requesterSnap = await db.collection('members').doc(context.auth.uid).get();
    const requesterData = requesterSnap.exists ? requesterSnap.data() : {};
    if (!isCeoContext(context, requesterData)) throw new HttpsError('permission-denied', 'CEO only');

    const limit = Math.min(parseInt(data?.limit || "1000", 10) || 1000, 5000);
    let membersSnap;
    try {
      membersSnap = await db.collection('members').orderBy('createdAt', 'desc').limit(limit).get();
      if (membersSnap.empty) {
        membersSnap = await db.collection('members').limit(limit).get();
      }
    } catch (_) {
      membersSnap = await db.collection('members').limit(limit).get();
    }
    const users = [];
    const counts = { total: 0, tier: {}, gender: {}, status: {}, paused: 0 };
    membersSnap.forEach(doc => {
      const d = doc.data() || {};
      const tier = d.tier || 'none';
      const gender = d.gender || 'unspecified';
      const status = d.paymentStatus || 'unknown';
      counts.total += 1;
      counts.tier[tier] = (counts.tier[tier] || 0) + 1;
      counts.gender[gender] = (counts.gender[gender] || 0) + 1;
      counts.status[status] = (counts.status[status] || 0) + 1;
      if (d.paused) counts.paused += 1;
      let createdAt = d.createdAt || null;
      if (createdAt && createdAt.toDate) {
        createdAt = createdAt.toDate().toISOString();
      }
      let lastLogin = d.lastLogin || null;
      if (lastLogin && lastLogin.toDate) {
        lastLogin = lastLogin.toDate().toISOString();
      }
      users.push({
        uid: doc.id,
        username: d.username || d.displayName || null,
        passCode: d.passCode || null,
        gender,
        tier,
        paymentStatus: status,
        paused: !!d.paused,
        createdAt,
        lastLogin
      });
    });
    return { counts, users };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("getMembersSummary failed", err);
    throw new HttpsError('internal', err?.message || 'Failed to load members');
  }
});

exports.recountMembers = functions.https.onCall(async (data, context) => {
  try {
    if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
    const requesterSnap = await db.collection('members').doc(context.auth.uid).get();
    const requesterData = requesterSnap.exists ? requesterSnap.data() : {};
    if (!isCeoContext(context, requesterData)) throw new HttpsError('permission-denied', 'CEO only');

    let total = 0;
    let nextPageToken = undefined;
    do {
      const list = await admin.auth().listUsers(1000, nextPageToken);
      list.users.forEach((user) => {
        const email = (user.email || '').toLowerCase();
        const isAnonymousUser = !email && (!user.providerData || user.providerData.length === 0);
        const isStaffUser = user.uid.startsWith("staff_") || user.customClaims?.staff === true;
        if (!isAnonymousUser && !isStaffUser) {
          total += 1;
        }
      });
      nextPageToken = list.pageToken;
    } while (nextPageToken);

    await db.collection('settings').doc('appStats').set({
      membersCount: total,
      recountedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return { membersCount: total };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("recountMembers failed", err);
    throw new HttpsError('internal', err?.message || 'Failed to recount members');
  }
});

// Shared staff login: validates access code + passphrase, returns custom token per venue
exports.getStaffLoginToken = functions.runWith(staffLoginSecrets).https.onCall(async (data) => {
  const expected = (process.env.STAFF_GATE_CODE || "foco2026").toString().trim().toLowerCase();
  const supplied = (data?.accessCode || "").toString().trim().toLowerCase();
  if (!supplied || supplied !== expected) throw new HttpsError('permission-denied', 'Invalid staff access code');

  const venueInput = (data?.venue || "").toString();
  const passphrase = (data?.passphrase || "").toString();
  const venueId = resolveStaffVenueId(venueInput);
  if (!venueId) throw new HttpsError('invalid-argument', 'Venue required');

  const isBeta = venueId === "beta";
  const passNorm = passphrase.trim().toLowerCase();
  const loginCode = getStaffVenueLoginCode(venueId).toLowerCase();
  const expectedPass = `foco-${loginCode}`.toLowerCase();
  const legacyPass = loginCode;
  const betaLegacyPass = "betastaff";
  if (isBeta) {
    const appSnap = await db.collection('settings').doc('app').get();
    const launched = appSnap.exists ? !!appSnap.data().launched : false;
    if (launched) throw new HttpsError('permission-denied', 'Beta access is disabled in live mode');
    if (passNorm !== expectedPass && passNorm !== betaLegacyPass && passNorm !== legacyPass) {
      throw new HttpsError('permission-denied', 'Invalid passphrase');
    }
  } else {
    if (!STAFF_VENUES[venueId]) throw new HttpsError('not-found', 'Unknown venue');
    if (passNorm !== expectedPass && passNorm !== legacyPass) {
      throw new HttpsError('permission-denied', 'Invalid passphrase');
    }
  }

  const uid = `staff_${venueId}`;
  const venueName = getStaffVenueName(venueId);
  const staffEmail = `staff+${venueId}@focoafterdark.com`;
  let userRecord = null;
  try {
    userRecord = await admin.auth().getUser(uid);
  } catch (err) {
    if (err?.code === "auth/user-not-found") {
      userRecord = await admin.auth().createUser({
        uid,
        email: staffEmail,
        displayName: `${venueName} Staff`
      });
    } else {
      throw err;
    }
  }
  try {
    await admin.auth().updateUser(uid, { password: expectedPass });
  } catch (err) {
    console.warn("Staff password update skipped:", err?.message || err);
  }
  await admin.auth().setCustomUserClaims(uid, { staff: true, venue: venueId, beta: isBeta });

  await db.collection("members").doc(uid).set({
    staff: true,
    role: "staff",
    tier: "staff",
    venueId,
    venueName,
    displayName: userRecord?.displayName || `${venueName} Staff`,
    email: (userRecord?.email || staffEmail).toLowerCase(),
    excludeFromLeaderboards: true,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });

  try {
    const token = await admin.auth().createCustomToken(uid, { staff: true, venue: venueId });
    return { token, venueId, venueName, fallback: false };
  } catch (err) {
    console.warn("Staff custom token failed, falling back to password login:", err?.message || err);
    return { fallback: true, venueId, venueName, email: staffEmail };
  }
});

// CEO login via shared passphrase: returns custom token with CEO claims.
exports.getCeoLoginToken = functions.runWith(ceoLoginSecrets).https.onCall(async (data) => {
  try {
    const expected = (process.env.CEO_LOGIN_CODE || "ceoceo").toString().trim().toLowerCase();
    const supplied = (data?.code || "").toString().trim().toLowerCase();
    if (!supplied || supplied !== expected) {
      throw new HttpsError('permission-denied', 'Invalid CEO access code');
    }

    let userRecord = null;
    let uid = CEO_UID;
    try {
      userRecord = await admin.auth().getUser(CEO_UID);
    } catch (err) {
      if (err?.code === "auth/user-not-found") {
        try {
          userRecord = await admin.auth().getUserByEmail(CEO_EMAIL);
          uid = userRecord.uid;
        } catch (emailErr) {
          userRecord = await admin.auth().createUser({
            uid: CEO_UID,
            email: CEO_EMAIL,
            displayName: "FoCo CEO"
          });
        }
      } else {
        throw err;
      }
    }

    try {
      await admin.auth().updateUser(uid, { password: expected });
    } catch (err) {
      console.warn("CEO password update skipped:", err?.message || err);
    }

    await admin.auth().setCustomUserClaims(uid, { ceo: true, admin: true });

    await db.collection("members").doc(uid).set({
      ceo: true,
      tier: "ceo",
      passCode: CEO_PASS_ID,
      email: (userRecord?.email || CEO_EMAIL).toLowerCase(),
      displayName: userRecord?.displayName || "FoCo CEO",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });

    const token = await admin.auth().createCustomToken(uid, { ceo: true, admin: true });
    return { token, email: CEO_EMAIL, uid };
  } catch (err) {
    console.error("getCeoLoginToken failed", err);
    if (err instanceof HttpsError) throw err;
    throw new HttpsError('internal', err?.message || 'Failed to issue CEO token');
  }
});

// Shared beta demo login: returns a custom token for a single beta user
exports.getBetaLoginToken = functions.runWith(betaLoginSecrets).https.onCall(async (data) => {
  const expected = (process.env.BETA_LOGIN_CODE || "beta").toString().trim().toLowerCase();
  const supplied = (data?.code || "").toString().trim().toLowerCase();
  if (!supplied || supplied !== expected) throw new HttpsError('permission-denied', 'Invalid beta access code');
  const fallbackPassword = `${expected}${expected}`;

  let userRecord = null;
  let betaUid = BETA_UID;
  try {
    userRecord = await admin.auth().getUser(BETA_UID);
  } catch (err) {
    if (err?.code === "auth/user-not-found") {
      try {
        userRecord = await admin.auth().getUserByEmail(BETA_EMAIL);
        betaUid = userRecord.uid;
      } catch (emailErr) {
        if (emailErr?.code === "auth/user-not-found") {
          userRecord = await admin.auth().createUser({
            uid: BETA_UID,
            email: BETA_EMAIL,
            displayName: "FoCo Beta"
          });
          betaUid = BETA_UID;
        } else {
          throw emailErr;
        }
      }
    } else {
      throw err;
    }
  }
  if (userRecord?.disabled) {
    await admin.auth().updateUser(userRecord.uid, { disabled: false });
  }
  try {
    await admin.auth().updateUser(betaUid, { password: fallbackPassword });
  } catch (err) {
    console.warn("Beta password update skipped:", err?.message || err);
  }

  const memberRef = db.collection("members").doc(betaUid);
  const memberSnap = await memberRef.get();
  const memberData = memberSnap.exists ? memberSnap.data() : {};
  const passCode = (memberData.passCode || await ensureUniquePassCode()).toUpperCase();
  const memberSince = memberData.memberSince || new Date().toISOString();
  await memberRef.set({
    email: (userRecord.email || BETA_EMAIL).toLowerCase(),
    username: memberData.username || BETA_USERNAME,
    displayName: memberData.displayName || "FoCo Beta",
    passCode,
    tier: "beta",
    memberSince,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAt: memberData.createdAt || admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });

  await db.collection("usernames").doc(BETA_USERNAME).set({
    uid: betaUid,
    email: (userRecord.email || BETA_EMAIL).toLowerCase(),
    passCode,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });

  const token = await admin.auth().createCustomToken(betaUid, { beta: true });
  return { token };
});

// Purge anonymous auth users created by beta skip login (and their member docs)
exports.purgeAnonymousUsers = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const requesterSnap = await db.collection('members').doc(context.auth.uid).get();
  const requesterData = requesterSnap.exists ? requesterSnap.data() : {};
  if (!isCeoContext(context, requesterData)) throw new HttpsError('permission-denied', 'CEO only');

  const limit = Math.min(parseInt(data?.limit || "1000", 10) || 1000, 1000);
  const dryRun = data?.dryRun === true;
  const summary = await purgeAnonymousUsersInternal({ dryRun, limit });
  return { ok: true, ...summary, dryRun };
});

// Reset beta/demo data safely (CEO/admin only). Stripe is never touched.
exports.resetBetaData = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const requesterSnap = await db.collection('members').doc(context.auth.uid).get();
  const requesterData = requesterSnap.exists ? requesterSnap.data() : {};
  if (!isCeoContext(context, requesterData)) throw new HttpsError('permission-denied', 'CEO only');

  const selections = data?.selections || {};
  const dryRun = data?.dryRun === true;
  const counts = {};
  const started = Date.now();
  const docIdField = admin.firestore.FieldPath.documentId();

  async function resetMembersStats() {
    const q = db.collection('members').orderBy(docIdField);
    let total = 0;
    let snap = await q.limit(400).get();
    while (!snap.empty) {
      let hasWrites = false;
      const batch = db.batch();
      snap.docs.forEach(docSnap => {
        const data = docSnap.data() || {};
        if (isCeoMemberDoc(data, docSnap.id)) return;
        total += 1;
        if (dryRun) return;
        hasWrites = true;
        batch.set(docSnap.ref, {
          points: 0,
          rewards: {},
          badges: {},
          streak: 0,
          totalSavings: 0,
          venuesVisited: 0,
          redemptions: 0,
          extraVouchers: {},
          extraRedemptionTokens: 0,
          nightWheel: { spinsLeft: 1, lastSpin: null },
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
      });
      if (!dryRun && hasWrites) {
        await batch.commit();
      }
      if (snap.size < 400) break;
      const lastDoc = snap.docs[snap.docs.length - 1];
      snap = await q.startAfter(lastDoc).limit(400).get();
    }
    return total;
  }

  async function deleteCollectionByName(name) {
    const q = db.collection(name).orderBy(docIdField);
    return deleteQueryInBatches(q, dryRun);
  }

  if (selections.redemptions) {
    let total = 0;
    const q = db.collectionGroup('redemptions').orderBy(docIdField);
    let snap = await q.limit(400).get();
    while (!snap.empty) {
      let hasWrites = false;
      const batch = db.batch();
      snap.docs.forEach(docSnap => {
        const data = docSnap.data() || {};
        const pass = (data.passCode || data.passId || "").toUpperCase();
        if (data.ceo === true || pass === CEO_PASS_ID) return;
        total += 1;
        if (dryRun) return;
        hasWrites = true;
        batch.delete(docSnap.ref);
      });
      if (!dryRun && hasWrites) {
        await batch.commit();
      }
      if (snap.size < 400) break;
      const lastDoc = snap.docs[snap.docs.length - 1];
      snap = await q.startAfter(lastDoc).limit(400).get();
    }
    counts.redemptions = total;
  }
  if (selections.alerts) {
    counts.alerts = await deleteCollectionByName('alerts');
  }
  if (selections.vipDeals) {
    counts.vipDeals = await deleteCollectionByName('deals');
  }
  if (selections.closeouts) {
    counts.closeouts = await deleteCollectionByName('closeOutReports');
  }
  if (selections.freeMemberships) {
    let total = 0;
    const q = db.collection('freeMemberships').orderBy(docIdField);
    let snap = await q.limit(400).get();
    while (!snap.empty) {
      let hasWrites = false;
      const batch = db.batch();
      snap.docs.forEach(docSnap => {
        const id = (docSnap.id || "").toUpperCase();
        if (id === CEO_PASS_ID) return;
        total += 1;
        if (dryRun) return;
        hasWrites = true;
        batch.delete(docSnap.ref);
      });
      if (!dryRun && hasWrites) {
        await batch.commit();
      }
      if (snap.size < 400) break;
      const lastDoc = snap.docs[snap.docs.length - 1];
      snap = await q.startAfter(lastDoc).limit(400).get();
    }
    counts.freeMemberships = total;
  }
  if (selections.members) {
    counts.members = await resetMembersStats();
  }
  if (selections.anonUsers) {
    const summary = await purgeAnonymousUsersInternal({ dryRun, limit: 1000 });
    counts.anonUsers = dryRun ? summary.matchedUsers : summary.deletedUsers;
    counts.anonMemberDocs = summary.deletedMemberDocs || 0;
  }

  return {
    ok: true,
    dryRun,
    deletedCountsByCollection: counts,
    durationMs: Date.now() - started
  };
});

// Cancel membership: stop renewals and optionally clear non-CEO profile stats
exports.cancelMembership = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const ref = db.collection('members').doc(uid);
  const snap = await ref.get();
  const docData = snap.exists ? snap.data() : {};
  const isCeo = docData.ceo === true || email === 'ceo@gmail.com' || (docData.passCode || '').toUpperCase() === CEO_PASS_ID;
  if (isCeo) throw new HttpsError('failed-precondition', 'CEO account cannot be canceled.');

  const wipe = data?.wipe === true;
  const nowIso = new Date().toISOString();
  let currentPeriodEndIso = docData.currentPeriodEnd || docData.nextRenewal || null;

  // For paid members, cancel the Stripe subscription at period end (Netflix-style cadence).
  if (!isStripeExcluded(context.auth.token, docData) && docData.stripeSubscriptionId) {
    try {
      const stripe = getStripeClient();
      const sub = await stripe.subscriptions.update(docData.stripeSubscriptionId, {
        cancel_at_period_end: true,
      });
      currentPeriodEndIso = isoFromUnix(sub.current_period_end) || currentPeriodEndIso;
      await ref.set({
        stripeCustomerId: sub.customer || docData.stripeCustomerId || null,
        stripeSubscriptionId: sub.id,
      }, { merge: true });
    } catch (err) {
      console.warn("Failed to set cancel_at_period_end on Stripe subscription", err?.message || err);
    }
  }

  const updates = {
    cancelAtPeriodEnd: true,
    cancelRequestedAt: nowIso,
    membershipStatus: "canceling",
    paymentStatus: docData.paymentStatus || 'active',
    currentPeriodEnd: currentPeriodEndIso,
    nextRenewal: currentPeriodEndIso,
  };
  if (currentPeriodEndIso) {
    updates.canceledAt = currentPeriodEndIso;
  }

  // Apply updates
  await ref.set(updates, { merge: true });

  // Optional data wipe (lightweight; keeps identity but clears usage stats)
  if (wipe) {
    await ref.set({
      totalRedemptions: admin.firestore.FieldValue.delete(),
      totalSavings: admin.firestore.FieldValue.delete(),
      venuesVisited: admin.firestore.FieldValue.delete(),
      points: admin.firestore.FieldValue.delete(),
      vibe: admin.firestore.FieldValue.delete(),
      clearedAt: new Date().toISOString()
    }, { merge: true });
  }
  return { ok: true, canceled: true, cancelAtPeriodEnd: true, currentPeriodEnd: currentPeriodEndIso, wiped: wipe };
});

// Launch mode toggle (CEO only) to switch between beta and live UI/flows
exports.getLaunchMode = functions.https.onCall(async (data, context) => {
  try {
    if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
    const requesterSnap = await db.collection('members').doc(context.auth.uid).get();
    const requesterData = requesterSnap.exists ? requesterSnap.data() : {};
    if (!isCeoContext(context, requesterData)) throw new HttpsError('permission-denied', 'CEO only');
    const snap = await db.collection('settings').doc('app').get();
    const launched = snap.exists ? !!snap.data().launched : false;
    return { launched };
  } catch (err) {
    console.warn("getLaunchMode failed", err);
    throw new HttpsError('internal', err?.message || 'Failed to load launch mode');
  }
});

exports.setLaunchMode = functions.https.onCall(async (data, context) => {
  try {
    if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
    const requesterSnap = await db.collection('members').doc(context.auth.uid).get();
    const requesterData = requesterSnap.exists ? requesterSnap.data() : {};
    if (!isCeoContext(context, requesterData)) throw new HttpsError('permission-denied', 'CEO only');
    const launched = data?.launched === true;
    await db.collection('settings').doc('app').set({
      launched,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return { launched };
  } catch (err) {
    console.warn("setLaunchMode failed", err);
    throw new HttpsError('internal', err?.message || 'Failed to update launch mode');
  }
});

// App lock toggle (CEO only) with passcode stored in Secret Manager
exports.setAppLock = functions.runWith(appLockSecrets).https.onCall(async (data, context) => {
  try {
    if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
    const requesterSnap = await db.collection('members').doc(context.auth.uid).get();
    const requesterData = requesterSnap.exists ? requesterSnap.data() : {};
    if (!isCeoContext(context, requesterData)) throw new HttpsError('permission-denied', 'CEO only');
    const secret = (process.env.APP_LOCK_CODE || '').trim();
    if (!secret) throw new HttpsError('failed-precondition', 'App lock code not configured');
    const code = (data?.code || '').toString().trim();
    if (!code || code !== secret) throw new HttpsError('permission-denied', 'Invalid lock code');
    const appLocked = data?.locked === true;
    await db.collection('settings').doc('app').set({
      appLocked,
      appLockedAt: admin.firestore.FieldValue.serverTimestamp(),
      appLockedBy: context.auth.uid
    }, { merge: true });
    return { ok: true, appLocked };
  } catch (err) {
    console.warn("setAppLock failed", err);
    throw err instanceof HttpsError ? err : new HttpsError('internal', err?.message || 'Failed to update app lock');
  }
});

// CEO access gate while app is locked (password stored in Secret Manager)
exports.verifyCeoAccess = functions.runWith(ceoAccessSecrets).https.onCall(async (data) => {
  try {
    const secret = (process.env.CEO_ACCESS_PASSWORD || '').trim();
    if (!secret) throw new HttpsError('failed-precondition', 'CEO access password not configured');
    const code = (data?.code || '').toString().trim();
    if (!code || code !== secret) throw new HttpsError('permission-denied', 'Invalid password');
    return { ok: true };
  } catch (err) {
    console.warn("verifyCeoAccess failed", err);
    throw err instanceof HttpsError ? err : new HttpsError('internal', err?.message || 'Failed to verify CEO access');
  }
});

exports.processRenewals = functions.runWith({ secrets: ["STRIPE_SECRET"] }).pubsub.schedule('0 9 * * *').timeZone('America/Denver').onRun(async () => {
  let stripe;
  try { stripe = getStripeClient(); } catch (err) {
    console.warn('Stripe not configured; skipping renewals');
    return null;
  }
  const snap = await db.collection('members')
    .where('stripeSubscriptionId', '!=', null)
    .orderBy('stripeSubscriptionId')
    .limit(200)
    .get();
  for (const docSnap of snap.docs) {
    const data = docSnap.data() || {};
    const uid = docSnap.id;
    if (data.paused === true || !data.stripeSubscriptionId) continue;
    if (isStripeExcluded({}, data)) continue;
    try {
      const subscription = await retrieveStripeSubscription(stripe, data.stripeSubscriptionId);
      await applyStripeSubscriptionUpdate(subscription, { uid, ref: docSnap.ref, data }, "scheduled.sync");
    } catch (err) {
      console.warn('Subscription sync failed for', uid, err?.message || err);
    }
  }
  return null;
});

exports.ceoChat = functions.runWith({ secrets: ["OPENAI_API_KEY"] }).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const email = (context.auth.token.email || '').toLowerCase();
  const uid = context.auth.uid;
  let isCeo = context.auth.token.ceo === true || email === 'ceo@gmail.com';
  if (!isCeo && uid) {
    try {
      const snap = await db.collection('members').doc(uid).get();
      const data = snap.exists ? snap.data() : {};
      const pass = (data.passCode || "").toUpperCase();
      if (data.ceo === true || pass === CEO_PASS_ID) {
        isCeo = true;
      }
    } catch (_) {}
  }
  if (!isCeo) throw new HttpsError('permission-denied', 'CEO only');
  const liveKey = process.env.OPENAI_API_KEY || openAiKey;
  if (!liveKey) throw new HttpsError('failed-precondition', 'OpenAI not configured');
  const prompt = (data?.prompt || '').toString().trim();
  if (!prompt) throw new HttpsError('invalid-argument', 'Prompt required');
  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${liveKey}`
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'You are the FoCo After Dark CEO assistant. Be concise, action-oriented, and focus on product, ops, and rollout guidance. Keep replies short.' },
          { role: 'user', content: prompt }
        ],
        max_tokens: 400,
        temperature: 0.4
      })
    });
    const txt = await resp.text();
    if (!resp.ok) {
      console.warn('OpenAI error', resp.status, txt);
      throw new HttpsError('internal', `OpenAI request failed (${resp.status})`);
    }
    const json = JSON.parse(txt);
    const reply = json?.choices?.[0]?.message?.content || 'No reply';
    return { reply, status: resp.status };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn('ceoChat failed', err);
    throw new HttpsError('internal', err?.message || 'Chat failed');
  }
});
function getStripeClient() {
  if (stripeClient) return stripeClient;
  const secret = process.env.STRIPE_SECRET;
  if (!secret) throw new Error("Stripe not configured");
  stripeClient = require("stripe")(secret);
  return stripeClient;
}

function getStripeConfig() {
  return {
    secret: process.env.STRIPE_SECRET || null,
    publishable: process.env.STRIPE_PUBLISHABLE || null,
  };
}
