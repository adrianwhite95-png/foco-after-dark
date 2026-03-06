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
const WAITLIST_DRY_RUN_EMAIL = "focoafterdark@gmail.com";
const CEO_UID = "ceo_master";
const BETA_UID = "foco-beta-demo";
const BETA_EMAIL = "beta@focoafterdark.com";
const BETA_USERNAME = "focobeta";
const CEO_FREE_CODE_COLLECTION = "ceoFreeSignupCodes";
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
  "elliott's": "elliotts",
  townpump: "town_pump",
  "town pump": "town_pump",
  mayor: "mayor_old_town",
  "the mayor": "mayor_old_town",
  "mayor of old town": "mayor_old_town",
  "the mayor of old town": "mayor_old_town"
};
const APP_PUBLIC_CONFIG_DOC_PATH = "appConfig/public";
const GLOBAL_CONFIG_DOC_PATH = "config/global";
const PERFORMANCE_CONFIG_DOC_PATH = "config/performance";
const WAITLIST_COLLECTION = "waitlist";
const WAITLIST_COUNTER_DOC_PATH = "counters/waitlist";
const WAITLIST_MAIL_QUEUE_COLLECTION = "mailQueue";
const APP_PUBLIC_CONFIG_SECTIONS = new Set(["flags", "text", "numbers", "lists", "style"]);
const VENUE_STATUS_VALUES = new Set(["active", "hidden", "paused", "archived"]);
const VENUE_SOFT_CONTROL_DEFAULTS = Object.freeze({
  status: "active",
  showInHomeFeed: true,
  showLivePill: true,
  priority: 50
});
const ADMIN_VENUE_MUTABLE_KEYS = new Set(["status", "showInHomeFeed", "showLivePill", "priority", "adminNote", "reason"]);
const WAITLIST_STATUSES = new Set(["active", "emailed", "unsubscribed"]);
const PERFORMANCE_CONFIG_DEFAULTS = Object.freeze({
  degradeMode: false,
  dealsLimit: 25,
  vipDealsLimit: 25,
  alertsLimit: 40,
  pollIntervalMs: 60000
});
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
const tokenIssuerServiceAccount = "foco-after-dark@appspot.gserviceaccount.com";
const staffLoginRunConfig = { ...staffLoginSecrets, serviceAccount: tokenIssuerServiceAccount };
const ceoLoginRunConfig = { ...ceoLoginSecrets, serviceAccount: tokenIssuerServiceAccount };
const betaLoginRunConfig = { ...betaLoginSecrets, serviceAccount: tokenIssuerServiceAccount };

admin.initializeApp();
const db = admin.firestore();
const messaging = admin.messaging();
const PUSH_STALE_ERROR_CODES = new Set([
  "messaging/registration-token-not-registered",
  "messaging/invalid-registration-token",
  "messaging/invalid-argument"
]);

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
  const from = process.env.REPORTS_SMTP_FROM || process.env.REPORTS_SMTP_USER || "focoafterdark@gmail.com";
  const to = opts.to || REPORTS_TO_EMAIL;
  try {
    const payload = {
      from: `FoCo After Dark <${from}>`,
      to,
      subject,
      text
    };
    if (opts.html) payload.html = opts.html;
    await transporter.sendMail(payload);
    return { sent: true };
  } catch (err) {
    return { sent: false, error: err?.message || "SMTP send failed" };
  }
}

async function queueMembershipConfirmationEmail({
  to = "",
  tier = "standard",
  nextRenewalIso = "",
}) {
  const email = String(to || "").trim().toLowerCase();
  if (!email || !email.includes("@")) return;
  const tierLabel = membershipTierLabel(normalizeTierKey(tier));
  const renewalText = nextRenewalIso ? new Date(nextRenewalIso).toLocaleDateString("en-US") : "your renewal date";
  const logoUrl = "https://foco-after-dark.web.app/foco-logo.png";
  const billingLine = `Your membership is active. Billing renews on ${renewalText}.`;
  const subject = `FoCo After Dark • ${tierLabel} membership active`;
  const text = [
    `Welcome to FoCo After Dark — your ${tierLabel} membership is active.`,
    "Congrats on joining the pass.",
    billingLine,
    "",
    "If you did not authorize this, contact support immediately."
  ].join("\n");
  const html = `
    <div style="font-family:Arial,sans-serif;background:#070d1f;color:#eaf2ff;padding:24px;border-radius:14px;max-width:560px;margin:0 auto;border:1px solid rgba(34,211,238,0.28);">
      <div style="text-align:center;margin-bottom:12px;">
        <img src="${logoUrl}" alt="FoCo After Dark logo" width="112" height="112" style="display:inline-block;border-radius:20px;box-shadow:0 0 22px rgba(34,211,238,0.45);" />
      </div>
      <h2 style="margin:0 0 8px 0;font-size:24px;line-height:1.3;color:#f7fbff;text-align:center;">Welcome to FoCo After Dark</h2>
      <p style="margin:0 0 10px 0;font-size:16px;line-height:1.5;text-align:center;">Congrats on signing up — your <strong>${tierLabel}</strong> membership is now active.</p>
      <p style="margin:0 0 16px 0;font-size:15px;line-height:1.6;text-align:center;color:#cbd5f5;">${billingLine}</p>
      <div style="margin:14px 0 0 0;padding:12px 14px;background:rgba(15,23,42,0.7);border:1px solid rgba(148,163,184,0.25);border-radius:12px;">
        <div style="font-size:13px;line-height:1.6;color:#dbe8ff;">
          <strong>Plan:</strong> ${tierLabel}<br />
          <strong>Renewal:</strong> ${renewalText}
        </div>
      </div>
      <p style="margin:16px 0 0 0;font-size:12px;line-height:1.6;color:#94a3b8;">If you did not authorize this, contact support immediately.</p>
    </div>
  `.trim();
  try {
    await db.collection("mail").add({
      to: email,
      message: { subject, text, html },
      meta: {
        type: "membership_confirmation",
        tier: normalizeTierKey(tier),
      },
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });
  } catch (err) {
    console.warn("queueMembershipConfirmationEmail failed", err?.message || err);
  }
  if (process.env.REPORTS_SMTP_USER && process.env.REPORTS_SMTP_PASS) {
    try {
      await sendReportEmail(subject, text, { to: email, html });
    } catch (err) {
      console.warn("directMembershipConfirmationEmail failed", err?.message || err);
    }
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

function getVenueResolutionCandidates(rawVenueId = "") {
  const base = String(rawVenueId || "").trim().toLowerCase();
  const candidates = new Set();
  const add = (value) => {
    const v = String(value || "").trim().toLowerCase();
    if (v) candidates.add(v);
  };
  add(base);
  add(resolveStaffVenueId(base));
  add(base.replace(/[\s-]+/g, "_"));
  add(base.replace(/[^a-z0-9_]+/g, ""));
  add(base.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, ""));
  if (base === "beta") add("bar_district");
  if (base === "bar_district") add("beta");
  return Array.from(candidates);
}

// Helper: enforce simple per-issuer rate limits to reduce abuse
async function checkRateLimit(uid, opts = {}) {
  const maxPerMin = opts.maxPerMin || 20;
  const maxPerDay = opts.maxPerDay || 2000;
  const burstAllowance = Number.isFinite(Number(opts.burstAllowance))
    ? Math.max(0, Number(opts.burstAllowance))
    : Math.max(3, Math.ceil(maxPerMin * 0.2));
  const now = admin.firestore.Timestamp.now();
  const nowMs = now.toMillis();
  const toMs = (value) => {
    if (!value) return 0;
    if (typeof value.toMillis === "function") return value.toMillis();
    if (typeof value.seconds === "number") {
      return (value.seconds * 1000) + Math.floor((Number(value.nanoseconds || 0) || 0) / 1e6);
    }
    const parsed = new Date(value).getTime();
    return Number.isFinite(parsed) ? parsed : 0;
  };
  const ref = db.collection('rateLimits').doc(uid);
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const data = snap.exists ? (snap.data() || {}) : {};
    const minuteWindowStartRaw = data.minuteWindowStart || now;
    const dayWindowStartRaw = data.dayWindowStart || now;
    const minuteWindowMs = toMs(minuteWindowStartRaw) || nowMs;
    const dayWindowMs = toMs(dayWindowStartRaw) || nowMs;
    let perMin = Number(data.perMin || 0) || 0;
    let perDay = Number(data.perDay || 0) || 0;
    let nextMinuteStart = admin.firestore.Timestamp.fromMillis(minuteWindowMs);
    let nextDayStart = admin.firestore.Timestamp.fromMillis(dayWindowMs);
    // Reset minute window if older than 60s
    if ((nowMs - minuteWindowMs) >= 60 * 1000) {
      perMin = 0;
      nextMinuteStart = now;
    }
    // Reset day window if a day passed
    if (new Date(nowMs).toDateString() !== new Date(dayWindowMs).toDateString()) {
      perDay = 0;
      nextDayStart = now;
    }
    if (perMin + 1 > (maxPerMin + burstAllowance)) {
      throw new HttpsError(
        'resource-exhausted',
        'Rate limit exceeded (per minute)',
        {
          limiter: "app_throttle",
          scope: "per_minute",
          maxPerMin,
          burstAllowance,
          perMin
        }
      );
    }
    if (perDay + 1 > maxPerDay) {
      throw new HttpsError(
        'resource-exhausted',
        'Rate limit exceeded (per day)',
        {
          limiter: "app_throttle",
          scope: "per_day",
          maxPerDay,
          perDay
        }
      );
    }
    // increment counters
    tx.set(ref, {
      perMin: perMin + 1,
      perDay: perDay + 1,
      minuteWindowStart: nextMinuteStart,
      dayWindowStart: nextDayStart,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
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

function getPublicRateLimitKey(context, scope = "generic") {
  const ip = getRequestIp(context) || "unknown";
  return `public_${scope}_${hashLookupKey(ip)}`;
}

async function enforcePublicCallableRateLimit(context, scope, opts = {}) {
  const limit = opts.limit || 20;
  const windowMs = opts.windowMs || (10 * 60 * 1000);
  const key = getPublicRateLimitKey(context, scope);
  await enforceLookupRateLimit({ key, limit, windowMs });
}

function readRequiredSecret(secretValue, secretName) {
  const value = (secretValue || "").toString().trim();
  if (!value) {
    throw new HttpsError("failed-precondition", `${secretName} is not configured`);
  }
  return value;
}

const SECURITY_SETTINGS_DOC = "settings/security";
const SECURITY_CACHE_TTL_MS = 2 * 60 * 1000;
let securitySettingsCache = { loadedAt: 0, data: {} };

function normalizeAppCheckMode(modeRaw) {
  const mode = String(modeRaw || "").trim().toLowerCase();
  if (mode === "required") return "required";
  if (mode === "off" || mode === "disabled") return "off";
  return "optional";
}

async function getSecuritySettingsCached() {
  const now = Date.now();
  if (securitySettingsCache.loadedAt && (now - securitySettingsCache.loadedAt) < SECURITY_CACHE_TTL_MS) {
    return securitySettingsCache.data || {};
  }
  try {
    const snap = await db.doc(SECURITY_SETTINGS_DOC).get();
    const data = snap.exists ? (snap.data() || {}) : {};
    securitySettingsCache = { loadedAt: now, data };
    return data;
  } catch (err) {
    console.warn("security settings read failed", err?.message || err);
    return securitySettingsCache.data || {};
  }
}

async function enforceCallableSecurity(context, opts = {}) {
  const requireAuth = opts.requireAuth !== false;
  if (requireAuth && !context?.auth) {
    throw new HttpsError("unauthenticated", "Auth required");
  }

  const useSettings = opts.useSecuritySettings !== false;
  const settings = useSettings ? await getSecuritySettingsCached() : {};
  const mode = normalizeAppCheckMode(opts.appCheckMode || settings.appCheckMode || "optional");
  if (mode === "required" && !context?.app) {
    throw new HttpsError("failed-precondition", "App integrity check required.");
  }

  if (opts.rateLimit) {
    if (context?.auth?.uid) {
      const rateOpts = (opts.rateLimit === true ? {} : (opts.rateLimit || {}));
      if (Number.isFinite(Number(opts.maxPerMin)) && !Number.isFinite(Number(rateOpts.maxPerMin))) {
        rateOpts.maxPerMin = Number(opts.maxPerMin);
      }
      if (Number.isFinite(Number(opts.maxPerDay)) && !Number.isFinite(Number(rateOpts.maxPerDay))) {
        rateOpts.maxPerDay = Number(opts.maxPerDay);
      }
      if (Number.isFinite(Number(opts.burstAllowance)) && !Number.isFinite(Number(rateOpts.burstAllowance))) {
        rateOpts.burstAllowance = Number(opts.burstAllowance);
      }
      try {
        await checkRateLimit(context.auth.uid, rateOpts);
      } catch (err) {
        if (err instanceof HttpsError && err.code === "resource-exhausted") {
          const details = { ...(err.details || {}), limiter: "app_throttle" };
          throw new HttpsError(err.code, err.message, details);
        }
        throw err;
      }
    } else if (opts.publicRateLimit) {
      await enforcePublicCallableRateLimit(context, opts.publicScope || "public", opts.publicRateLimit);
    }
  }
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
const BUSINESS_TIME_ZONE = "America/Denver";
const SHIFT_START_HOUR = 5;
const SHIFT_END_HOUR = 2;
const SHIFT_END_MINUTE = 45;

const DENVER_PARTS_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: BUSINESS_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false
});

function getTimeZoneParts(value = new Date()) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const parts = DENVER_PARTS_FORMATTER.formatToParts(date);
  const mapped = {};
  parts.forEach((part) => {
    if (part.type === "literal") return;
    mapped[part.type] = part.value;
  });
  const year = Number(mapped.year);
  const month = Number(mapped.month);
  const day = Number(mapped.day);
  const hour = Number(mapped.hour);
  const minute = Number(mapped.minute);
  const second = Number(mapped.second);
  if (
    !Number.isFinite(year)
    || !Number.isFinite(month)
    || !Number.isFinite(day)
    || !Number.isFinite(hour)
    || !Number.isFinite(minute)
    || !Number.isFinite(second)
  ) {
    return null;
  }
  return { year, month, day, hour, minute, second };
}

function shiftCalendarDay(parts, deltaDays = 0) {
  const base = Date.UTC(parts.year, parts.month - 1, parts.day);
  const shifted = new Date(base + (deltaDays * 24 * 60 * 60 * 1000));
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate()
  };
}

function getTimeZoneOffsetMs(date) {
  const parts = getTimeZoneParts(date);
  if (!parts) return 0;
  const asUtc = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
    0
  );
  return asUtc - date.getTime();
}

function toUtcMillisForTimeZone(parts) {
  const baseUtc = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    Number(parts.hour || 0),
    Number(parts.minute || 0),
    Number(parts.second || 0),
    Number(parts.millisecond || 0)
  );
  if (!Number.isFinite(baseUtc)) return NaN;
  let utcMs = baseUtc;
  for (let i = 0; i < 3; i += 1) {
    const offset = getTimeZoneOffsetMs(new Date(utcMs));
    const next = baseUtc - offset;
    if (Math.abs(next - utcMs) < 1) {
      utcMs = next;
      break;
    }
    utcMs = next;
  }
  return utcMs;
}

function getPendingRedemptionExpiryMs(nowValue = new Date()) {
  const now = nowValue instanceof Date ? nowValue : new Date(nowValue);
  if (Number.isNaN(now.getTime())) {
    return Date.now() + PENDING_REDEMPTION_TIMEOUT_MS;
  }
  const localNow = getTimeZoneParts(now);
  if (!localNow) {
    return now.getTime() + PENDING_REDEMPTION_TIMEOUT_MS;
  }
  const shiftStartDay = shiftCalendarDay(localNow, localNow.hour < SHIFT_START_HOUR ? -1 : 0);
  let shiftEndDay = shiftCalendarDay(shiftStartDay, 1);
  let expiryMs = toUtcMillisForTimeZone({
    ...shiftEndDay,
    hour: SHIFT_END_HOUR,
    minute: SHIFT_END_MINUTE,
    second: 0,
    millisecond: 0
  });
  if (!Number.isFinite(expiryMs)) {
    return now.getTime() + PENDING_REDEMPTION_TIMEOUT_MS;
  }
  if (expiryMs <= now.getTime()) {
    shiftEndDay = shiftCalendarDay(shiftEndDay, 1);
    expiryMs = toUtcMillisForTimeZone({
      ...shiftEndDay,
      hour: SHIFT_END_HOUR,
      minute: SHIFT_END_MINUTE,
      second: 0,
      millisecond: 0
    });
  }
  return Number.isFinite(expiryMs) ? expiryMs : (now.getTime() + PENDING_REDEMPTION_TIMEOUT_MS);
}

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

function getCurrentVoucherBillingMonthKeyForWallet(memberData = {}, now = new Date()) {
  const billing = (memberData?.billing && typeof memberData.billing === "object") ? memberData.billing : {};
  const cycle = getVoucherCycleWindowForWallet(memberData, billing, now)?.current;
  const anchor = cycle?.start || now;
  return getMonthTokenFromDate(anchor);
}

function getCurrentCyclePurchasedTokenTotalForWallet(memberData = {}, now = new Date()) {
  const billingMonthKey = getCurrentVoucherBillingMonthKeyForWallet(memberData, now);
  const buckets = memberData?.extraVoucherBuckets;
  if (!buckets || typeof buckets !== "object") return 0;
  const bucket = buckets[billingMonthKey];
  const value = Number(bucket?.redemptionTokens || 0);
  return Number.isFinite(value) ? Math.max(0, value) : 0;
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
  const nowDate = now instanceof Date ? now : new Date(now);
  const nowMs = Number.isFinite(nowDate.getTime()) ? nowDate.getTime() : Date.now();
  const expiresAtMs = toMillis(entry?.expiresAt);
  if (Number.isFinite(expiresAtMs)) {
    return expiresAtMs > nowMs;
  }
  const stamp = getRedemptionEntryDate(entry);
  if (!stamp) return false;
  return (nowMs - stamp.getTime()) <= maxMs;
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

function hasActiveLaunchTrialForWallet(memberData = {}, now = new Date()) {
  const promoTag = String(memberData?.membershipPromoTag || "").toLowerCase();
  const membershipStatus = String(memberData?.membershipStatus || "").toLowerCase();
  const trialDays = Number(memberData?.membershipTrialDays || 0);
  const trialEndsAt = toDateSafe(
    memberData?.membershipTrialEndsAt
    || memberData?.currentPeriodEnd
    || memberData?.nextRenewal
  );
  const trialFlag = membershipStatus === "trialing" || trialDays > 0;
  if (!trialFlag) return false;
  if (trialEndsAt && trialEndsAt < now) return false;
  return true;
}

function getPointsMonthlyTokenGrantTotal(memberData = {}, now = new Date()) {
  void memberData;
  void now;
  return 0;
}

function hasSuccessfulPaymentThisCycleForWallet(tier, billing = {}, memberData = {}, now = new Date()) {
  if (tier !== "standard" && tier !== "vip") return true;
  const status = String(memberData?.paymentStatus || "active").toLowerCase();
  if (status !== "active") return false;
  if (hasActiveLaunchTrialForWallet(memberData, now)) return true;
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
  const purchasedTotal = getCurrentCyclePurchasedTokenTotalForWallet(memberData, now);
  const grantKey = getMonthTokenFromDate(now);
  const monthlyGrantRaw = memberData?.ceoMonthlyTokenGrants && typeof memberData.ceoMonthlyTokenGrants === "object"
    ? memberData.ceoMonthlyTokenGrants[grantKey]
    : 0;
  const ceoMonthlyGrant = Math.max(0, Number(monthlyGrantRaw || 0));
  const pointsMonthlyGrant = getPointsMonthlyTokenGrantTotal(memberData, now);
  const monthlyGrant = ceoMonthlyGrant + pointsMonthlyGrant;
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
      { includePending: false, now, pendingMaxMs: PENDING_REDEMPTION_TIMEOUT_MS }
    );
    const regularAllowance = limit + carryover;
    const regularAllowanceWithGrant = regularAllowance + (isCurrent ? monthlyGrant : 0);
    const regularUnused = Math.max(0, regularAllowance - cycleUsed);
    const nextCarryover = Math.min(MAX_VOUCHER_CARRYOVER, regularUnused);
    if (isCurrent) {
      const consumedTokens = Math.max(0, cycleUsed - regularAllowanceWithGrant);
      const purchasedAfter = Math.max(0, purchasedTotal - consumedTokens);
      const remainingRegular = Math.max(0, regularAllowanceWithGrant - cycleUsed);
      const remaining = remainingRegular + purchasedAfter;
      result = {
        unlimited: false,
        remaining,
        limit,
        carryover,
        usedCurrent: cycleUsed,
        total: regularAllowanceWithGrant + purchasedTotal,
        extraTokens: purchasedAfter,
        paymentRequired: false
      };
    }
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
    await enforceCallableSecurity(context, {
      rateLimit: { maxPerMin: 240, maxPerDay: 12000 }
    });
    const passCode = String(data?.passCode || "").trim().toUpperCase();
    const venueIdInput = String(data?.venueId || "").trim().toLowerCase();
    const venueIdCandidates = getVenueResolutionCandidates(venueIdInput);
    let venueId = venueIdCandidates[0] || "";
    if (venueIdCandidates.length > 1) {
      for (const candidate of venueIdCandidates) {
        const candidateSnap = await db.collection("venues").doc(candidate).get();
        if (candidateSnap.exists) {
          venueId = candidate;
          break;
        }
      }
    }
    const perkId = String(data?.perkId || "").trim();
    const perkLabel = String(data?.perkLabel || "").trim();
    const perkKey = String(data?.perkKey || "venue_perk").trim();
    const vibeLabelInput = String(data?.vibeLabel || "").trim();
    const vibeLabel = vibeLabelInput ? vibeLabelInput.slice(0, 28) : null;
    const vibeKeyInput = String(data?.vibeKey || "").trim().toLowerCase();
    const vibeKey = ["flirty", "blacking", "social", "casual", "justone", "custom"].includes(vibeKeyInput)
      ? vibeKeyInput
      : null;
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
    const callerClaims = context.auth?.token || {};
    const callerUid = String(context.auth?.uid || "");
    const callerEmail = String(callerClaims.email || "").toLowerCase();
    const callerIsStaffLike = callerClaims.staff === true
      || callerClaims.admin === true
      || callerClaims.ceo === true
      || callerUid.startsWith("staff_")
      || callerEmail.startsWith("staff+");
    const publicConfigRef = db.doc(APP_PUBLIC_CONFIG_DOC_PATH);
    const memberDisplayName = (member) =>
      (member?.displayName || member?.name || member?.fullName || member?.username || "FoCo member");

    for (let attempt = 0; attempt < 5; attempt += 1) {
      const redemptionId = generateCode(6).toUpperCase();
      try {
        const result = await db.runTransaction(async (tx) => {
          const configSnap = await tx.get(publicConfigRef);
          const configData = configSnap.exists ? (configSnap.data() || {}) : {};
          const redeemEnabled = configFlagEnabled(configData, "redeem.enabled", true);
          const memberRedeemDisabledMessage = "Redemptions are temporarily unavailable. Please try again shortly.";
          const staffRedeemDisabledMessage = "Redemptions are disabled by HQ control.";
          if (!redeemEnabled) {
            console.info("[createRedemption] blocked", { reason: "REDEEM_DISABLED", venueId });
            throw new HttpsError(
              "failed-precondition",
              callerIsStaffLike ? staffRedeemDisabledMessage : memberRedeemDisabledMessage,
              {
                success: false,
                reason: "REDEEM_DISABLED",
                venueId,
                memberMessage: memberRedeemDisabledMessage,
                staffMessage: staffRedeemDisabledMessage,
              }
            );
          }

          const configText = configData?.text && typeof configData.text === "object" ? configData.text : {};
          const pausedMessage = String(configText["venue.paused.message"] || "This venue is temporarily paused for redemptions.");
          let activeShiftKey = "";
          let effectiveVenueId = "";
          let effectiveVenueData = {};
          let sawPausedVenue = false;
          for (const candidateVenueId of venueIdCandidates) {
            const candidateVenueRef = db.collection("venues").doc(candidateVenueId);
            const candidateShiftStateRef = candidateVenueRef.collection("shiftState").doc("current");
            const [candidateVenueSnap, candidateShiftStateSnap] = await Promise.all([
              tx.get(candidateVenueRef),
              tx.get(candidateShiftStateRef),
            ]);
            const candidateVenueData = candidateVenueSnap.exists ? (candidateVenueSnap.data() || {}) : {};
            const candidateVenueStatus = normalizeVenueStatusValue(candidateVenueData.status || VENUE_SOFT_CONTROL_DEFAULTS.status);
            if (candidateVenueStatus === "paused") sawPausedVenue = true;
            if (candidateVenueStatus !== "active") continue;
            const shiftState = candidateShiftStateSnap.exists ? (candidateShiftStateSnap.data() || {}) : {};
            let shiftKey = (shiftState.active === true && shiftState.shiftKey)
              ? String(shiftState.shiftKey || "").trim()
              : "";
            if (!shiftKey) {
              const activeSessionQuery = candidateVenueRef
                .collection("shiftSessions")
                .where("active", "==", true)
                .limit(1);
              const activeSessionSnap = await tx.get(activeSessionQuery);
              if (!activeSessionSnap.empty) {
                const firstSession = activeSessionSnap.docs[0];
                const firstSessionData = firstSession.data() || {};
                shiftKey = String(firstSessionData.shiftKey || firstSession.id || "").trim();
              }
            }
            if (!shiftKey) continue;
            const candidateLockSnap = await tx.get(candidateVenueRef.collection("shiftCloseouts").doc(shiftKey));
            const candidateLocked = candidateLockSnap.exists && candidateLockSnap.data()?.shiftClosed === true;
            if (candidateLocked) continue;
            effectiveVenueId = candidateVenueId;
            effectiveVenueData = candidateVenueData;
            activeShiftKey = shiftKey;
            break;
          }
          if (!effectiveVenueId) {
            const betaBypassRequested = venueIdCandidates.includes("beta") || venueIdCandidates.includes("bar_district");
            if (betaBypassRequested) {
              const fallbackVenueId = venueIdCandidates.find(Boolean) || "bar_district";
              const fallbackVenueRef = db.collection("venues").doc(fallbackVenueId);
              const fallbackVenueSnap = await tx.get(fallbackVenueRef);
              effectiveVenueId = fallbackVenueId;
              effectiveVenueData = fallbackVenueSnap.exists ? (fallbackVenueSnap.data() || {}) : {};
            }
          }
          if (!effectiveVenueId) {
            const venueStatus = sawPausedVenue ? "paused" : "inactive";
            const memberVenueUnavailableMessage = sawPausedVenue
              ? pausedMessage
              : "This venue is temporarily unavailable for redemptions.";
            const reason = sawPausedVenue ? "VENUE_PAUSED" : "SHIFT_INACTIVE";
            const staffVenueUnavailableMessage = sawPausedVenue
              ? "Venue is paused. Redemptions are blocked until unpaused."
              : "Shift is inactive or closed. Start a new shift before accepting redemptions.";
            console.info("[createRedemption] blocked", { reason, venueId, venueStatus, candidates: venueIdCandidates });
            throw new HttpsError(
              "failed-precondition",
              callerIsStaffLike ? staffVenueUnavailableMessage : memberVenueUnavailableMessage,
              {
                success: false,
                reason,
                venueId,
                venueStatus,
                memberMessage: memberVenueUnavailableMessage,
                staffMessage: staffVenueUnavailableMessage,
              }
            );
          }
          venueId = effectiveVenueId;
          const venueData = effectiveVenueData || {};
          const perkRef = db.collection("venues").doc(venueId).collection("perks").doc(perkId);
          const memberShiftInactiveMessage = "Sorry, you\u2019re using a voucher outside of this venue\u2019s shift schedule \u2014 try again tomorrow!";
          const staffShiftInactiveMessage = "Shift is inactive or closed. Start a new shift before accepting redemptions.";
          if (activeShiftKey) {
            const shiftSnap = await tx.get(db.collection("venues").doc(venueId).collection("shiftCloseouts").doc(activeShiftKey));
            const shiftData = shiftSnap.exists ? (shiftSnap.data() || {}) : {};
            if (shiftData.shiftClosed === true) {
              console.info("[createRedemption] blocked", { reason: "SHIFT_INACTIVE", venueId });
              throw new HttpsError(
                "failed-precondition",
                callerIsStaffLike ? staffShiftInactiveMessage : memberShiftInactiveMessage,
                {
                  success: false,
                  reason: "SHIFT_INACTIVE",
                  venueId,
                  shiftKey: activeShiftKey,
                  memberMessage: memberShiftInactiveMessage,
                  staffMessage: staffShiftInactiveMessage
                }
              );
            }
          }
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
            const allowBetaFallback = venueId === "beta"
              || fallbackId.startsWith("beta_")
              || callerClaims.beta === true;
            const allowFallback = allowBetaFallback
              || perkKey === "reward_shot"
              || fallbackId === "drink"
              || fallbackId === "shot"
              || fallbackId === "cover";
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
          const expiresAtMs = getPendingRedemptionExpiryMs(now.toDate());
          const expiresAt = admin.firestore.Timestamp.fromMillis(expiresAtMs);
          const basePayload = {
            redemptionId,
            passCode: resolvedPassCode || passCode,
            memberUid: resolved.uid,
            memberName: memberDisplayName(memberData),
            memberUsername: String(memberData.username || data?.memberUsername || memberData.handle || "").trim() || null,
            tier: memberData.tier || "standard",
            venueId,
            perkId,
            perkKey,
            perkLabel: perkLabel || perkData.label || "Perk",
            vibeLabel,
            vibeKey,
            status: "pending",
            expiresAt,
            createdAt: serverNow,
            updatedAt: serverNow,
            timestamp: serverNow,
            requestedVenue: venueId
          };
          if (activeShiftKey) basePayload.shiftKey = activeShiftKey;
          const venuePayload = { ...basePayload, queueScope: "venue" };
          const memberPayload = { ...basePayload, queueScope: "member" };
          const returnPayload = {
            ...venuePayload,
            createdAt: now,
            updatedAt: now,
            timestamp: now
          };
          tx.set(venueRedRef, venuePayload);
          tx.set(memberRedRef, memberPayload);
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

exports.checkInAlert = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 800 }
  });
  const uid = String(context?.auth?.uid || "");
  const alertId = String(data?.alertId || "").trim();
  const passCodeInput = String(data?.passCode || "").trim().toUpperCase();
  if (!uid || !alertId) {
    throw new HttpsError("invalid-argument", "Missing alertId.");
  }

  const alertRef = db.collection("alerts").doc(alertId);
  const memberRef = db.collection("members").doc(uid);
  const memberCheckinRef = memberRef.collection("alertCheckins").doc(alertId);
  const now = admin.firestore.Timestamp.now();
  const serverNow = admin.firestore.FieldValue.serverTimestamp();

  const result = await db.runTransaction(async (tx) => {
    const [alertSnap, memberSnap] = await Promise.all([
      tx.get(alertRef),
      tx.get(memberRef)
    ]);
    if (!alertSnap.exists) {
      throw new HttpsError("not-found", "Alert not found.");
    }
    const alertData = alertSnap.data() || {};
    const expiresAtMs = alertData.expiresAt?.toMillis
      ? alertData.expiresAt.toMillis()
      : (alertData.expiresAt ? new Date(alertData.expiresAt).getTime() : null);
    if (expiresAtMs && expiresAtMs <= Date.now()) {
      throw new HttpsError("failed-precondition", "Alert expired.");
    }

    const memberData = memberSnap.exists ? (memberSnap.data() || {}) : {};
    const memberPassCode = String(
      memberData.passCode ||
      memberData.passId ||
      passCodeInput ||
      ""
    ).trim().toUpperCase();
    const checkinDocId = String(
      memberPassCode ? `pass_${memberPassCode}` : `uid_${uid}`
    ).replace(/[\/\s]+/g, "_");
    const checkinRef = alertRef.collection("checkins").doc(checkinDocId);
    const legacyUidRef = alertRef.collection("checkins").doc(uid);

    const [primaryCheckinSnap, legacyCheckinSnap, existingPassCodeSnap] = await Promise.all([
      tx.get(checkinRef),
      legacyUidRef.path === checkinRef.path ? Promise.resolve(null) : tx.get(legacyUidRef),
      memberPassCode
        ? tx.get(alertRef.collection("checkins").where("passCode", "==", memberPassCode).limit(1))
        : Promise.resolve(null)
    ]);

    const currentCount = Math.max(0, Number(alertData.checkInCount || 0));
    const alreadyCheckedIn = Boolean(
      primaryCheckinSnap?.exists ||
      legacyCheckinSnap?.exists ||
      (existingPassCodeSnap && !existingPassCodeSnap.empty)
    );
    if (alreadyCheckedIn) {
      return {
        alreadyCheckedIn: true,
        checkInCount: currentCount,
        alertId
      };
    }

    const venueId = String(alertData.venueId || "").toLowerCase();
    const venueName = String(alertData.venueName || "").trim();
    const nextCount = currentCount + 1;

    tx.set(checkinRef, {
      uid,
      alertId,
      venueId,
      venueName,
      passCode: memberPassCode || null,
      checkinKey: checkinDocId,
      checkedInAt: serverNow,
      expiresAt: alertData.expiresAt || null
    }, { merge: true });
    tx.set(memberCheckinRef, {
      alertId,
      venueId,
      venueName,
      passCode: memberPassCode || null,
      checkinKey: checkinDocId,
      checkedInAt: serverNow,
      expiresAt: alertData.expiresAt || null
    }, { merge: true });
    tx.set(alertRef, {
      checkInCount: nextCount,
      lastCheckInAt: serverNow,
      updatedAt: serverNow
    }, { merge: true });
    tx.set(db.collection("members").doc(uid), {
      lastAlertCheckInAt: serverNow
    }, { merge: true });
    return {
      alreadyCheckedIn: false,
      checkInCount: nextCount,
      alertId
    };
  });

  return {
    ok: true,
    alertId: result.alertId,
    alreadyCheckedIn: !!result.alreadyCheckedIn,
    checkInCount: Math.max(0, Number(result.checkInCount || 0)),
    checkedInAt: now
  };
});

exports.getMemberAlertCheckins = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 800 }
  });
  try {
    const uid = String(context?.auth?.uid || "").trim();
    if (!uid) {
      throw new HttpsError("unauthenticated", "Auth required.");
    }
    const passCodeInput = String(data?.passCode || "").trim().toUpperCase();
    const memberSnap = await db.collection("members").doc(uid).get();
    const memberData = memberSnap.exists ? (memberSnap.data() || {}) : {};
    const passCode = String(
      memberData.passCode ||
      memberData.passId ||
      passCodeInput ||
      ""
    ).trim().toUpperCase();

    const nowMs = Date.now();
    const alertIds = new Set();
    const memberCheckinsSnap = await db
      .collection("members")
      .doc(uid)
      .collection("alertCheckins")
      .limit(500)
      .get();
    if (!memberCheckinsSnap.empty) {
      memberCheckinsSnap.docs.forEach((docSnap) => {
        const row = docSnap.data() || {};
        const expiresMs = toMillisSafe(row.expiresAt);
        if (expiresMs && expiresMs <= nowMs) return;
        const alertId = String(row.alertId || docSnap.id || "").trim();
        if (alertId) alertIds.add(alertId);
      });
    }

    // Legacy backfill fallback (older check-in docs before member subcollection).
    if (alertIds.size === 0) {
      try {
        const legacyQueries = [];
        if (passCode) {
          legacyQueries.push(db.collectionGroup("checkins").where("passCode", "==", passCode).limit(400).get());
        }
        legacyQueries.push(db.collectionGroup("checkins").where("uid", "==", uid).limit(400).get());
        const snaps = await Promise.all(legacyQueries);
        for (const snap of snaps) {
          if (!snap || snap.empty) continue;
          snap.docs.forEach((docSnap) => {
            const row = docSnap.data() || {};
            const expiresMs = toMillisSafe(row.expiresAt);
            if (expiresMs && expiresMs <= nowMs) return;
            const resolvedAlertId = String(
              row.alertId ||
              docSnap.ref?.parent?.parent?.id ||
              ""
            ).trim();
            if (resolvedAlertId) alertIds.add(resolvedAlertId);
          });
        }
      } catch (legacyErr) {
        console.warn("[getMemberAlertCheckins] legacy lookup failed", legacyErr?.message || legacyErr);
      }
    }

    return {
      ok: true,
      passCode: passCode || null,
      alertIds: Array.from(alertIds)
    };
  } catch (err) {
    console.error("[getMemberAlertCheckins]", err?.message || err);
    if (err instanceof HttpsError) throw err;
    return { ok: true, passCode: null, alertIds: [] };
  }
});

exports.verifyRedemption = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 1500 }
  });
  const venueId = String(data?.venueId || "").trim().toLowerCase();
  const redemptionId = String(data?.redemptionId || "").trim().toUpperCase();
  const action = String(data?.action || "confirm").trim().toLowerCase();
  if (!["confirm", "deny", "pending"].includes(action)) {
    throw new HttpsError("invalid-argument", "Unsupported action.");
  }
  const reasonCode = String(data?.reasonCode || "").trim().slice(0, 80) || null;
  const reasonNote = String(data?.reasonNote || "").trim().slice(0, 500) || null;
  const staffActionId = String(data?.staffId || "").trim().slice(0, 20) || null;
  const staffActionName = String(data?.staffName || "").trim().slice(0, 120) || null;
  const shiftKey = String(data?.shiftKey || "").trim();
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
  const shiftLockRef = shiftKey
    ? db.collection("venues").doc(venueId).collection("shiftCloseouts").doc(shiftKey)
    : null;

  const result = await db.runTransaction(async (tx) => {
    if (shiftLockRef) {
      const shiftSnap = await tx.get(shiftLockRef);
      const shiftData = shiftSnap.exists ? (shiftSnap.data() || {}) : {};
      if (shiftData.shiftClosed === true) {
        throw new HttpsError("failed-precondition", "Shift is already closed.");
      }
    }
    const redSnap = await tx.get(venueRedRef);
    if (!redSnap.exists) {
      if (action !== "confirm") {
        throw new HttpsError("not-found", "Code not found.");
      }
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
        lastActionType: "verify_redemption",
        lastActionAt: now,
        lastActionStaffId: staffActionId,
        lastActionStaffName: staffActionName,
        timestamp: ceoSnap.data()?.createdAt || now,
        requestedVenue: venueId,
        shiftKey: shiftKey || null,
        ceo: true,
        queueScope: "venue"
      };
      tx.set(venueRedRef, ceoPayload, { merge: true });
      tx.set(ceoVoucherRef, { used: true, usedAt: now, venueId }, { merge: true });
      return ceoPayload;
    }
    const dataSnap = redSnap.data() || {};
    if (dataSnap.venueId && dataSnap.venueId !== venueId) {
      throw new HttpsError("permission-denied", "This code belongs to another venue.");
    }
    if (dataSnap.status === "verified" && action === "confirm") {
      return dataSnap;
    }
    if (String(dataSnap.status || "").toLowerCase() === "pending" && action === "pending") {
      return dataSnap;
    }
    if (String(dataSnap.status || "").toLowerCase() === "denied" && action === "deny") {
      return dataSnap;
    }
    const memberUid = dataSnap.memberUid;
    const memberRef = memberUid ? db.collection("members").doc(memberUid) : null;
    const memberSnap = memberRef ? await tx.get(memberRef) : null;
    const prevStatus = String(dataSnap.status || "").toLowerCase();
    const wasVerified = ["verified", "approved", "confirmed"].includes(prevStatus);
    const nextStatus = action === "deny" ? "denied" : action === "pending" ? "pending" : "verified";
    const storedUpdates = {
      status: nextStatus,
      updatedAt: serverNow,
      lastActionType: action === "confirm" ? "verify_redemption" : action === "deny" ? "deny_redemption" : "set_pending",
      lastActionAt: serverNow,
      lastActionStaffId: staffActionId,
      lastActionStaffName: staffActionName,
      queueScope: "venue"
    };
    if (shiftKey) {
      storedUpdates.shiftKey = shiftKey;
    }
    if (nextStatus === "verified") {
      storedUpdates.verifiedAt = serverNow;
      storedUpdates.deniedAt = admin.firestore.FieldValue.delete();
      storedUpdates.deniedReasonCode = admin.firestore.FieldValue.delete();
      storedUpdates.deniedReasonNote = admin.firestore.FieldValue.delete();
      storedUpdates.pendingReasonCode = admin.firestore.FieldValue.delete();
      storedUpdates.pendingReasonNote = admin.firestore.FieldValue.delete();
    } else {
      storedUpdates.verifiedAt = admin.firestore.FieldValue.delete();
      if (nextStatus === "denied") {
        storedUpdates.deniedAt = serverNow;
        storedUpdates.deniedReasonCode = reasonCode;
        storedUpdates.deniedReasonNote = reasonNote;
      } else {
        storedUpdates.deniedAt = admin.firestore.FieldValue.delete();
        storedUpdates.pendingReasonCode = reasonCode;
        storedUpdates.pendingReasonNote = reasonNote;
        storedUpdates.deniedReasonCode = admin.firestore.FieldValue.delete();
        storedUpdates.deniedReasonNote = admin.firestore.FieldValue.delete();
      }
    }
    tx.update(venueRedRef, storedUpdates);

    if (memberUid) {
      const memberRedRef = db.collection("members").doc(memberUid).collection("redemptions").doc(redemptionId);
      tx.set(memberRedRef, { ...storedUpdates, queueScope: "member" }, { merge: true });
      if (memberSnap && memberSnap.exists) {
        const memberData = memberSnap.data() || {};
        const remaining = memberData.perksRemaining;
        const memberUpdates = { updatedAt: serverNow };
        if (nextStatus === "verified" && !wasVerified) {
          memberUpdates.lastVerifiedAt = serverNow;
          memberUpdates.totalRedemptions = admin.firestore.FieldValue.increment(1);
          memberUpdates[`venuesVisited.${venueId}`] = true;
          if (remaining && typeof remaining.tokens === "number") {
            memberUpdates["perksRemaining.tokens"] = Math.max(0, remaining.tokens - 1);
          }
        } else if (wasVerified && nextStatus !== "verified") {
          memberUpdates.totalRedemptions = admin.firestore.FieldValue.increment(-1);
          if (remaining && typeof remaining.tokens === "number") {
            memberUpdates["perksRemaining.tokens"] = remaining.tokens + 1;
          }
        }
        tx.set(memberRef, memberUpdates, { merge: true });
      }
    }
    const returnUpdates = {
      status: nextStatus,
      updatedAt: now
    };
    if (shiftKey) {
      returnUpdates.shiftKey = shiftKey;
    }
    if (nextStatus === "verified") {
      returnUpdates.verifiedAt = now;
      returnUpdates.deniedAt = null;
      returnUpdates.pendingReasonCode = null;
      returnUpdates.pendingReasonNote = null;
      returnUpdates.deniedReasonCode = null;
      returnUpdates.deniedReasonNote = null;
    } else {
      returnUpdates.verifiedAt = null;
      if (nextStatus === "denied") {
        returnUpdates.deniedAt = now;
        returnUpdates.deniedReasonCode = reasonCode;
        returnUpdates.deniedReasonNote = reasonNote;
      } else {
        returnUpdates.deniedAt = null;
        returnUpdates.pendingReasonCode = reasonCode;
        returnUpdates.pendingReasonNote = reasonNote;
        returnUpdates.deniedReasonCode = null;
        returnUpdates.deniedReasonNote = null;
      }
    }
    returnUpdates.lastActionType = storedUpdates.lastActionType;
    returnUpdates.lastActionAt = now;
    returnUpdates.lastActionStaffId = staffActionId;
    returnUpdates.lastActionStaffName = staffActionName;
    return { ...dataSnap, ...returnUpdates };
  });

  return { ok: true, redemption: result };
});

// Resolve username to email (public, no auth required)
exports.resolveUsernameToEmail = functions.https.onCall(async (data, context) => {
  const raw = (data?.username || "").toString().trim().replace(/^@/, "").toLowerCase();
  if (!raw) {
    throw new HttpsError("invalid-argument", "Username required.");
  }
  if (!/^[a-z0-9_]{3,24}$/.test(raw)) {
    throw new HttpsError("invalid-argument", "Invalid username.");
  }
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
    // Username is explicitly claimed via reserveUsername from the client flow.
    username: existing.exists && existing.data().username ? existing.data().username : '',
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
    const unameRef = db.collection('usernames').doc(uname);
    await db.runTransaction(async (tx) => {
      const unameSnap = await tx.get(unameRef);
      if (!unameSnap.exists || unameSnap.data()?.uid === uid) {
        tx.set(unameRef, {
          uid,
          email,
          createdAt: unameSnap.exists
            ? (unameSnap.data()?.createdAt || admin.firestore.FieldValue.serverTimestamp())
            : admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
      }
    });
  }
  await db.collection('settings').doc('appStats').set({
    membersCount: admin.firestore.FieldValue.increment(1),
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return true;
});

exports.cleanupDeletedAuthUser = functions.auth.user().onDelete(async (user) => {
  const uid = user?.uid;
  if (!uid) return null;
  try {
    await purgeMemberFirestoreRecords(uid);
  } catch (err) {
    console.warn("cleanupDeletedAuthUser failed", uid, err?.message || err);
  }
  return null;
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
    lucky_joes: "Lucky Joe's Sidewalk Saloon",
    trail_head: "Trail Head Tavern",
    steak_out: "Steak-Out Saloon",
    road_34: "Road 34 Bike Bar",
    brothers: "Brothers",
    pour_brothers: "Pour Brothers Community Tavern",
    tap_handle: "Tap and Handle",
    high_point: "High Point",
    pinball_jones: "Pinball Jones",
    elliotts: "Elliott's Martini Bar",
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
      .where('queueScope', '==', 'venue')
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

  // NOTE: settings/health.lastCloseoutEmail is now reserved for successful staff close-out emails only.
  // Do not update that field from nightly/manual batch jobs.

  return null;
}

exports.nightlyCloseOut = functions.runWith(reportEmailSecrets)
  .pubsub.schedule('0 3 * * *')
  .timeZone('America/Denver')
  .onRun(async () => {
    console.log("nightlyCloseOut disabled by config: auto emails are off.");
    return null;
  });

exports.runCloseOutNow = functions.runWith(reportEmailSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const uid = context.auth.uid;
  let requesterData = {};
  try {
    const snap = await db.collection('members').doc(uid).get();
    requesterData = snap.exists ? (snap.data() || {}) : {};
  } catch (_) {}
  if (!isCeoContext(context)) {
    throw new HttpsError('permission-denied', 'CEO only');
  }
  return { ok: true, disabled: true, message: "Manual nightly close-out email run is disabled. Staff close-out sends reports." };
});


// Atomic username reservation/update
exports.checkUsernameAvailability = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    requireAuth: false,
    rateLimit: true,
    publicScope: "checkUsernameAvailability",
    publicRateLimit: { limit: 40, windowMs: 10 * 60 * 1000 }
  });
  const requesterUid = String(context?.auth?.uid || "").trim();
  const desired = (data && data.username ? String(data.username) : '').trim().toLowerCase();
  if (!desired || desired.length < 3 || desired.length > 15 || !/^[a-z0-9_]+$/.test(desired)) {
    throw new HttpsError('invalid-argument', 'Invalid username format');
  }
  const unameSnap = await db.collection("usernames").doc(desired).get();
  if (unameSnap.exists) {
    const ownerUid = String(unameSnap.data()?.uid || "").trim();
    if (!ownerUid || ownerUid !== requesterUid) {
      return { available: false, username: desired };
    }
  }
  const memberMatch = await db.collection("members").where("username", "==", desired).limit(2).get();
  const takenByOtherMember = memberMatch.docs.some((docSnap) => docSnap.id !== requesterUid);
  return { available: !takenByOtherMember, username: desired };
});

exports.reserveUsername = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 12, maxPerDay: 200 }
  });
  const uid = context.auth.uid;
  const desired = (data && data.username ? String(data.username) : '').trim().toLowerCase();
  if (!desired || desired.length < 3 || desired.length > 15 || !/^[a-z0-9_]+$/.test(desired)) {
    throw new HttpsError('invalid-argument', 'Invalid username format');
  }
  const memberRef = db.collection('members').doc(uid);
  return db.runTransaction(async (tx) => {
    const unameRef = db.collection('usernames').doc(desired);
    const existingUname = await tx.get(unameRef);
    if (existingUname.exists && existingUname.data().uid !== uid) {
      throw new HttpsError('already-exists', 'Username already taken');
    }
    const memberMatch = await tx.get(db.collection("members").where("username", "==", desired).limit(2));
    const takenByOtherMember = memberMatch.docs.some((docSnap) => docSnap.id !== uid);
    if (takenByOtherMember) {
      throw new HttpsError('already-exists', 'Username already taken');
    }
    const memberSnap = await tx.get(memberRef);
    if (!memberSnap.exists) {
      throw new HttpsError('failed-precondition', 'Member profile missing');
    }
    const member = memberSnap.data() || {};
    const currentUsername = String(member.username || "").trim().toLowerCase();
    if (currentUsername && currentUsername !== desired) {
      const oldRef = db.collection("usernames").doc(currentUsername);
      const oldSnap = await tx.get(oldRef);
      if (oldSnap.exists && oldSnap.data()?.uid === uid) {
        tx.delete(oldRef);
      }
    }
    tx.set(memberRef, { username: desired, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    tx.set(unameRef, {
      uid,
      email: member.email || context.auth.token.email || '',
      passCode: (member.passCode || '').toUpperCase(),
      createdAt: existingUname.exists ? (existingUname.data()?.createdAt || admin.firestore.FieldValue.serverTimestamp()) : admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return { success: true, username: desired };
  });
});

exports.ensureMemberPassCode = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 10, maxPerDay: 120 }
  });
  const uid = context.auth.uid;
  const memberRef = db.collection("members").doc(uid);
  const memberSnap = await memberRef.get();
  if (!memberSnap.exists) {
    throw new HttpsError("failed-precondition", "Member profile missing");
  }
  const memberData = memberSnap.data() || {};
  let passCode = String(memberData.passCode || "").trim().toUpperCase();
  if (!passCode) {
    passCode = await ensureUniquePassCode();
    await memberRef.set({
      passCode,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
  }

  await db.collection("passes").doc(passCode).set({
    uid,
    passCode,
    tier: memberData.tier || memberData.membershipTier || "standard",
    status: memberData.revoked ? "revoked" : "active",
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });

  return { passCode };
});

exports.createCeoFreeSignupCode = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 400 }
  });
  if (!isCeoContext(context)) throw new HttpsError("permission-denied", "CEO only");

  const providedCode = normalizeCeoFreeCodeInput(data?.code || "");
  const code = providedCode || `FREE-${generateCode(8)}`;
  if (!code || code.length < 6) {
    throw new HttpsError("invalid-argument", "Invalid code format.");
  }

  const ref = db.collection(CEO_FREE_CODE_COLLECTION).doc(code);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (snap.exists) {
      const existing = snap.data() || {};
      const stillActive = existing.revoked !== true && existing.consumed !== true;
      if (stillActive) {
        throw new HttpsError("already-exists", "Code already exists.");
      }
    }
    tx.set(ref, buildCeoFreeCodeDoc(code, context), { merge: true });
  });

  return { ok: true, code };
});

exports.listCeoFreeSignupCodes = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 1000 }
  });
  if (!isCeoContext(context)) throw new HttpsError("permission-denied", "CEO only");
  const limit = Math.min(Math.max(Number(data?.limit || 40) || 40, 1), 200);
  let snap;
  try {
    snap = await db.collection(CEO_FREE_CODE_COLLECTION).orderBy("createdAt", "desc").limit(limit).get();
  } catch (_) {
    snap = await db.collection(CEO_FREE_CODE_COLLECTION).limit(limit).get();
  }
  const codes = [];
  snap.forEach((docSnap) => {
    const d = docSnap.data() || {};
    codes.push({
      code: docSnap.id,
      revoked: d.revoked === true,
      consumed: d.consumed === true,
      createdAt: d.createdAt || null,
      updatedAt: d.updatedAt || null,
      consumedAt: d.consumedAt || null,
      consumedByUid: d.consumedByUid || null,
      consumedPassCode: d.consumedPassCode || null
    });
  });
  return { ok: true, codes };
});

exports.revokeCeoFreeSignupCode = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 500 }
  });
  if (!isCeoContext(context)) throw new HttpsError("permission-denied", "CEO only");
  const code = normalizeCeoFreeCodeInput(data?.code || "");
  if (!code) throw new HttpsError("invalid-argument", "Code required");
  const ref = db.collection(CEO_FREE_CODE_COLLECTION).doc(code);
  const snap = await ref.get();
  if (!snap.exists) throw new HttpsError("not-found", "Code not found");
  await ref.set({
    revoked: true,
    revokedAt: admin.firestore.FieldValue.serverTimestamp(),
    revokedByUid: context.auth?.uid || null,
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return { ok: true, code, revoked: true };
});

exports.previewCeoFreeSignupCode = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    requireAuth: false,
    rateLimit: true,
    publicScope: "previewCeoFreeSignupCode",
    publicRateLimit: { limit: 30, windowMs: 10 * 60 * 1000 }
  });
  const code = normalizeCeoFreeCodeInput(data?.code || "");
  if (!code) {
    return { ok: true, valid: false, message: "Enter a valid invite code." };
  }
  const ref = db.collection(CEO_FREE_CODE_COLLECTION).doc(code);
  const snap = await ref.get();
  if (!snap.exists) {
    return { ok: true, valid: false, message: "Invite code not found." };
  }
  const payload = snap.data() || {};
  if (payload.revoked === true) {
    return { ok: true, valid: false, message: "Invite code is no longer active." };
  }
  if (payload.consumed === true) {
    return { ok: true, valid: false, message: "Invite code has already been used." };
  }
  if (payload.expiresAt && typeof payload.expiresAt.toMillis === "function" && payload.expiresAt.toMillis() < Date.now()) {
    return { ok: true, valid: false, message: "Invite code has expired." };
  }
  return { ok: true, valid: true, code, message: "Invite code accepted." };
});

exports.claimCeoFreeSignupCode = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 12, maxPerDay: 120 }
  });
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");

  const code = normalizeCeoFreeCodeInput(data?.code || "");
  if (!code) throw new HttpsError("invalid-argument", "Code required");

  const uid = context.auth.uid;
  const email = (context.auth.token?.email || "").toLowerCase();
  const prepared = await ensureMemberProfileForCeoFree(uid, email);
  const { memberRef, passCode } = prepared;
  const codeRef = db.collection(CEO_FREE_CODE_COLLECTION).doc(code);
  const freeRef = db.collection("freeMemberships").doc(passCode);

  const result = await db.runTransaction(async (tx) => {
    const [memberSnap, codeSnap] = await Promise.all([
      tx.get(memberRef),
      tx.get(codeRef)
    ]);
    if (!codeSnap.exists) {
      throw new HttpsError("not-found", "Code not found.");
    }
    const memberData = memberSnap.exists ? (memberSnap.data() || {}) : {};
    const codeData = codeSnap.data() || {};
    const priorTier = normalizeLookupTier(memberData) || "standard";
    const safePriorTier = ["vip", "standard", "explorer"].includes(priorTier) ? priorTier : "standard";
    const override = String(memberData.membershipOverride || memberData.override || "").toUpperCase();
    const alreadyFree = memberData.freeMembership === true || override === "CEO_FREE";
    if (alreadyFree) {
      return { ok: true, applied: false, alreadyFree: true, passCode };
    }

    if (codeData.revoked === true) {
      throw new HttpsError("failed-precondition", "Code is no longer active.");
    }
    if (codeData.expiresAt && typeof codeData.expiresAt.toMillis === "function" && codeData.expiresAt.toMillis() < Date.now()) {
      throw new HttpsError("failed-precondition", "Code has expired.");
    }
    if (codeData.consumed === true) {
      throw new HttpsError("already-exists", "Code already used.");
    }

    tx.set(memberRef, {
      freeMembership: true,
      membershipOverride: "CEO_FREE",
      tier: "ceo_free",
      membershipTier: "ceo_free",
      requestedTier: "ceo_free",
      baseTierBeforeCeoFree: safePriorTier,
      priorTierBeforeFree: safePriorTier,
      preCeoFreePaymentStatus: String(memberData.paymentStatus || "active"),
      preCeoFreeMembershipStatus: String(memberData.membershipStatus || "active"),
      membershipStatus: "active",
      paymentStatus: "active",
      revoked: false,
      validUntil: "never",
      freeGrantedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    tx.set(freeRef, {
      active: true,
      passCode,
      uid,
      email,
      oneTimeCode: code,
      freeMembership: true,
      membershipOverride: "CEO_FREE",
      tier: "ceo_free",
      grantedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    tx.set(codeRef, {
      consumed: true,
      uses: 1,
      consumedAt: admin.firestore.FieldValue.serverTimestamp(),
      consumedByUid: uid,
      consumedByEmail: email,
      consumedPassCode: passCode,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return { ok: true, applied: true, passCode };
  });

  return result;
});

exports.setCeoFreeMembershipForPass = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    requireAuth: true,
    rateLimit: true,
    maxPerMin: 30,
    maxPerDay: 600
  });
  if (!isCeoContext(context)) throw new HttpsError("permission-denied", "CEO only");
  const passCode = String(data?.passCode || "").trim().toUpperCase();
  if (!passCode) throw new HttpsError("invalid-argument", "Pass code required");
  const enable = data?.enable !== false;

  const memberSnap = await db.collection("members").where("passCode", "==", passCode).limit(1).get();
  if (memberSnap.empty) throw new HttpsError("not-found", "Member not found");
  const memberDoc = memberSnap.docs[0];
  const memberData = memberDoc.data() || {};
  const freeRef = db.collection("freeMemberships").doc(passCode);
  const priorTier = normalizeLookupTier(memberData) || "standard";
  const safePriorTier = ["vip", "standard", "explorer"].includes(priorTier) ? priorTier : "standard";

  await db.runTransaction(async (tx) => {
    const liveSnap = await tx.get(memberDoc.ref);
    const live = liveSnap.exists ? (liveSnap.data() || {}) : {};
    if (enable) {
      const liveTier = normalizeLookupTier(live) || "standard";
      const baseTier = ["vip", "standard", "explorer"].includes(liveTier) ? liveTier : "standard";
      tx.set(memberDoc.ref, {
        freeMembership: true,
        membershipOverride: "CEO_FREE",
        tier: "ceo_free",
        membershipTier: "ceo_free",
        requestedTier: "ceo_free",
        baseTierBeforeCeoFree: live.baseTierBeforeCeoFree || baseTier,
        priorTierBeforeFree: live.priorTierBeforeFree || baseTier,
        preCeoFreePaymentStatus: String(live.paymentStatus || "active"),
        preCeoFreeMembershipStatus: String(live.membershipStatus || "active"),
        membershipStatus: "active",
        paymentStatus: "active",
        revoked: false,
        validUntil: "never",
        freeGrantedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
      tx.set(freeRef, {
        active: true,
        passCode,
        uid: memberDoc.id,
        email: String(live.email || "").toLowerCase() || null,
        freeMembership: true,
        membershipOverride: "CEO_FREE",
        tier: "ceo_free",
        grantedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
      return;
    }
    const restoreTier = normalizeLookupTier(
      live.baseTierBeforeCeoFree
      || live.priorTierBeforeFree
      || live.requestedTier
      || live.membershipTier
      || safePriorTier
    ) || "standard";
    const restorePayment = String(live.preCeoFreePaymentStatus || live.paymentStatus || "active").toLowerCase();
    const restoreMembership = String(live.preCeoFreeMembershipStatus || live.membershipStatus || "active").toLowerCase();
    tx.set(memberDoc.ref, {
      freeMembership: false,
      membershipOverride: null,
      tier: restoreTier,
      membershipTier: restoreTier,
      requestedTier: restoreTier,
      paymentStatus: restoreTier === "explorer" ? "active" : restorePayment,
      membershipStatus: restoreTier === "explorer" ? "explorer" : restoreMembership,
      revoked: false,
      validUntil: null,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    tx.set(freeRef, {
      active: false,
      revoked: true,
      revokedAt: admin.firestore.FieldValue.serverTimestamp(),
      revokedByUid: context.auth.uid,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
  });

  return {
    ok: true,
    passCode,
    enabled: enable
  };
});

function normalizePointAwardKey(raw = "") {
  return String(raw || "")
    .toLowerCase()
    .replace(/[^a-z0-9:_-]/g, "_")
    .slice(0, 80);
}

function extractAchievementIdFromAwardKey(awardKey = "") {
  const normalized = normalizePointAwardKey(awardKey);
  if (!normalized.startsWith("achievement:")) return "";
  return normalized.slice("achievement:".length);
}

function isPointsLockedMember(member = {}) {
  const tier = String(member?.tier || member?.membershipTier || "").toLowerCase();
  const requestedTier = String(member?.requestedTier || "").toLowerCase();
  const membershipStatus = String(member?.membershipStatus || "").toLowerCase();
  return (
    tier === "explorer"
    || requestedTier === "explorer"
    || membershipStatus === "explorer"
  );
}

// Server-side points adjust with idempotency support for one-time awards.
exports.awardPoints = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 600 }
  });
  const points = Math.floor(Number(data?.points || 0));
  const reason = (data?.reason || 'adjustment').toString().slice(0, 120);
  const venue = String(data?.venue || "").trim().toLowerCase();
  const awardKey = normalizePointAwardKey(data?.awardKey || "");
  const achievementId = extractAchievementIdFromAwardKey(awardKey);
  if (!Number.isFinite(points) || points <= 0 || points > 2000) {
    throw new HttpsError('invalid-argument', 'points must be between 1 and 2000');
  }

  const uid = context.auth.uid;
  const memberRef = db.collection('members').doc(uid);
  let result = { success: true, awarded: false, skipped: true, points: 0, venuesVisited: {} };

  await db.runTransaction(async (tx) => {
    const snap = await tx.get(memberRef);
    if (!snap.exists) throw new HttpsError('failed-precondition', 'Member missing');
    const member = snap.data() || {};
    const pointsLocked = isPointsLockedMember(member);
    const current = Math.max(0, Number(member.points || 0));
    const venuesVisited = { ...(member.venuesVisited || {}) };
    if (pointsLocked) {
      if (current !== 0) {
        tx.set(memberRef, {
          points: 0,
          pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
      }
      result = {
        success: true,
        awarded: false,
        skipped: true,
        points: 0,
        pointsLocked: true,
        venuesVisited,
        awardKey: awardKey || null
      };
      return;
    }
    if (venue) {
      venuesVisited[venue] = true;
    }

    const alreadyAwarded = awardKey ? Boolean(member.pointAwardKeys?.[awardKey]) : false;
    const achievementAlreadyAwarded = achievementId
      ? Boolean(member.achievementPointsAwarded?.[achievementId])
      : false;
    const wasAwarded = alreadyAwarded || achievementAlreadyAwarded;
    const awarded = !wasAwarded;
    const next = awarded ? (current + points) : current;
    const updates = {
      points: next,
      pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp()
    };
    if (venue) {
      updates[`venuesVisited.${venue}`] = true;
    }
    if (awarded) {
      if (awardKey) {
        updates[`pointAwardKeys.${awardKey}`] = admin.firestore.FieldValue.serverTimestamp();
      }
      if (achievementId) {
        updates[`achievementPointsAwarded.${achievementId}`] = admin.firestore.FieldValue.serverTimestamp();
      }
      updates.lastPointsReason = reason || null;
    }
    tx.set(memberRef, updates, { merge: true });
    result = { success: true, awarded, skipped: !awarded, points: next, venuesVisited, awardKey: awardKey || null };
  });

  if (result.awarded) {
    try {
      await db.collection('auditLogs').add({
        action: 'awardPoints',
        uid,
        delta: points,
        reason,
        awardKey: awardKey || null,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    } catch (err) {
      console.warn('awardPoints audit log failed', err?.message || err);
    }
  }
  return result;
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
    "Toast to a stranger's night",
    "Start a mini shuffle",
    "Teach someone a handshake",
    "Challenge a friend to a dance battle",
    "Wear sunglasses inside for 1 minute",
    "Start a slow-mo walk",
    "Start a “cheers” wave",
    "Find someone from your hometown",
    "Compliment someone's outfit",
    "Get a selfie with 3 people",
    "Spell FOCO with friends' bodies",
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
    const pointsLocked = isPointsLockedMember(member);
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
    const points = pointsLocked ? 0 : Math.max(0, Number(member.points || 0) + 20);
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
      pointsAwarded: pointsLocked ? 0 : 20,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      result: { bar, drink, challenge }
    });
    return { bar, drink, challenge, remaining: allowance === Infinity ? Infinity : allowance - nextSpins, points };
  });
});

// Push notifications: register token per user
exports.registerPushToken = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 300 }
  });
  const token = (data?.token || '').trim();
  const clientKey = (data?.clientKey || '').trim().slice(0, 120);
  if (!token) throw new HttpsError('invalid-argument', 'Token required');
  if (token.length < 80 || /\s/.test(token)) {
    throw new HttpsError('invalid-argument', 'Invalid token');
  }
  const uid = context.auth.uid;
  const tokenDocId = crypto.createHash('sha256').update(token).digest('hex');
  // Keep one active token per client key within the same user account.
  if (clientKey) {
    const staleForClient = await db.collection('pushTokens').doc(uid).collection('tokens')
      .where('clientKey', '==', clientKey)
      .get();
    if (!staleForClient.empty) {
      const staleBatch = db.batch();
      staleForClient.docs.forEach((docSnap) => {
        if (docSnap.id !== tokenDocId) {
          staleBatch.delete(docSnap.ref);
        }
      });
      await staleBatch.commit();
    }
  } else {
    // Legacy clients without a client key keep one active token per user.
    const ownTokens = await db.collection('pushTokens').doc(uid).collection('tokens').get();
    if (!ownTokens.empty) {
      const ownBatch = db.batch();
      ownTokens.docs.forEach((docSnap) => {
        if (docSnap.id !== tokenDocId) ownBatch.delete(docSnap.ref);
      });
      await ownBatch.commit();
    }
  }
  const ref = db.collection('pushTokens').doc(uid).collection('tokens').doc(tokenDocId);
  await ref.set({
    token,
    uid,
    clientKey: clientKey || null,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return { success: true };
});

exports.registerPushTokenPublic = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    requireAuth: false,
    publicRateLimit: { limit: 60, windowMs: 10 * 60 * 1000 },
    publicScope: "push-register",
  });
  const token = String(data?.token || "").trim();
  const clientKey = String(data?.clientKey || "").trim().slice(0, 120);
  if (!isLikelyPushToken(token)) {
    throw new HttpsError("invalid-argument", "Invalid token");
  }
  const tokenDocId = crypto.createHash('sha256').update(token).digest('hex');
  const bucketRef = db.collection("pushTokensPublic").doc("global").collection("tokens");
  if (clientKey) {
    const stale = await bucketRef.where("clientKey", "==", clientKey).get();
    if (!stale.empty) {
      const batch = db.batch();
      stale.docs.forEach((docSnap) => {
        if (docSnap.id !== tokenDocId) batch.delete(docSnap.ref);
      });
      await batch.commit();
    }
  }
  await bucketRef.doc(tokenDocId).set({
    token,
    uid: null,
    clientKey: clientKey || null,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return { success: true };
});

function getTokenFromPushDoc(docSnap) {
  const data = docSnap.data ? (docSnap.data() || {}) : {};
  const value = String(data.token || docSnap.id || "").trim();
  return isLikelyPushToken(value) ? value : "";
}

function isLikelyPushToken(token) {
  const value = String(token || "").trim();
  if (!value) return false;
  if (value.length < 80) return false;
  if (/\s/.test(value)) return false;
  return true;
}

function getMessagingErrorCode(err) {
  return String(
    err?.code ||
    err?.errorInfo?.code ||
    err?.details?.code ||
    "unknown"
  ).trim();
}

async function getAllPushTokens() {
  const snap = await db.collectionGroup('tokens').get();
  const byClientKey = new Map();
  const byUidWithoutClientKey = new Map();
  snap.docs.forEach((docSnap) => {
    const token = getTokenFromPushDoc(docSnap);
    if (!token) return;
    const data = docSnap.data() || {};
    const clientKey = String(data.clientKey || "").trim();
    const uid = String(data.uid || "").trim();
    if (!uid) return;
    const updatedMs = toMillisSafe(data.updatedAt || data.createdAt);
    if (!clientKey) {
      const prev = byUidWithoutClientKey.get(uid);
      if (!prev || updatedMs >= prev.updatedMs) {
        byUidWithoutClientKey.set(uid, { token, updatedMs });
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
  return Array.from(new Set([...uniqueClientTokens, ...uniqueLegacyTokens]));
}

async function getPublicPushTokens() {
  const snap = await db.collection("pushTokensPublic").doc("global").collection("tokens").get();
  if (snap.empty) return [];
  const byClientKey = new Map();
  const noClient = new Set();
  snap.docs.forEach((docSnap) => {
    const token = getTokenFromPushDoc(docSnap);
    if (!token) return;
    const data = docSnap.data() || {};
    const clientKey = String(data.clientKey || "").trim();
    const updatedMs = toMillisSafe(data.updatedAt || data.createdAt);
    if (!clientKey) {
      noClient.add(token);
      return;
    }
    const prev = byClientKey.get(clientKey);
    if (!prev || updatedMs >= prev.updatedMs) {
      byClientKey.set(clientKey, { token, updatedMs });
    }
  });
  const keyed = Array.from(byClientKey.values()).map((entry) => entry.token);
  return Array.from(new Set([...keyed, ...Array.from(noClient)]));
}

async function getUserPushTokens(uid) {
  const targetUid = String(uid || "").trim();
  if (!targetUid) return [];
  const tokenSnap = await db.collection('pushTokens').doc(targetUid).collection('tokens').get();
  if (tokenSnap.empty) return [];
  const byClientKey = new Map();
  let latestLegacy = null;
  tokenSnap.docs.forEach((docSnap) => {
    const token = getTokenFromPushDoc(docSnap);
    if (!token) return;
    const data = docSnap.data() || {};
    const clientKey = String(data.clientKey || "").trim();
    const updatedMs = toMillisSafe(data.updatedAt || data.createdAt);
    if (clientKey) {
      const prev = byClientKey.get(clientKey);
      if (!prev || updatedMs >= prev.updatedMs) {
        byClientKey.set(clientKey, { token, updatedMs });
      }
      return;
    }
    if (!latestLegacy || updatedMs >= latestLegacy.updatedMs) {
      latestLegacy = { token, updatedMs };
    }
  });
  const tokens = Array.from(byClientKey.values()).map((entry) => entry.token);
  if (latestLegacy?.token) tokens.push(latestLegacy.token);
  return Array.from(new Set(tokens));
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

function isStalePushError(code) {
  return PUSH_STALE_ERROR_CODES.has(String(code || "").trim());
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

async function cleanupInvalidPushToken(token, opts = {}) {
  const normalizedToken = String(token || "").trim();
  if (!normalizedToken) return;
  const uidHint = String(opts.uid || "").trim();
  try {
    const refs = new Map();
    if (uidHint) {
      const directRef = db.collection("pushTokens").doc(uidHint).collection("tokens").doc(normalizedToken);
      refs.set(directRef.path, directRef);
    }
    const [existingByField, existingByDocId] = await Promise.all([
      db.collectionGroup("tokens").where("token", "==", normalizedToken).get(),
      db.collectionGroup("tokens").where(admin.firestore.FieldPath.documentId(), "==", normalizedToken).get()
    ]);
    existingByField.docs.forEach((docSnap) => refs.set(docSnap.ref.path, docSnap.ref));
    existingByDocId.docs.forEach((docSnap) => refs.set(docSnap.ref.path, docSnap.ref));
    if (!refs.size) return;
    const batch = db.batch();
    refs.forEach((ref) => batch.delete(ref));
    await batch.commit();
  } catch (err) {
    console.warn("push cleanup failed", normalizedToken.slice(-8), err?.message || err);
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
  const [privateTokens, publicTokens] = await Promise.all([
    getAllPushTokens(),
    getPublicPushTokens()
  ]);
  const tokens = Array.from(new Set([...(privateTokens || []), ...(publicTokens || [])])).filter(isLikelyPushToken);
  if (!tokens.length) return { success: false, reason: 'no_tokens' };
  const batches = chunkTokens(tokens, 500);
  let sent = 0;
  let failedRaw = 0;
  let staleFailureCount = 0;
  const sentAt = String(Date.now());
  const safeTitle = String(title || "FoCo After Dark").slice(0, 80);
  const safeBody = String(body || "New update from a venue.").slice(0, 180);
  const invalidTokens = new Set();
  const errorCodes = {};
  let firstErrorCode = null;
  let firstHardErrorCode = null;
  for (const batch of batches) {
    const message = {
      data: {
        title: safeTitle,
        body: safeBody,
        link: safeLink,
        sentAt
      },
      webpush: {
        headers: {
          Urgency: "high",
          TTL: "2419200"
        },
        notification: {
          title: safeTitle,
          body: safeBody
        },
        fcmOptions: safeLink ? { link: safeLink } : undefined
      },
      tokens: batch
    };
    let res = null;
    try {
      res = await messaging.sendEachForMulticast(message);
    } catch (err) {
      const code = getMessagingErrorCode(err);
      failedRaw += batch.length;
      errorCodes[code] = (errorCodes[code] || 0) + batch.length;
      if (!firstErrorCode) firstErrorCode = code;
      if (!isStalePushError(code) && !firstHardErrorCode) firstHardErrorCode = code;
      if (code === "messaging/registration-token-not-registered") {
        batch.forEach((badToken) => {
          if (badToken) invalidTokens.add(badToken);
        });
      }
      continue;
    }
    sent += res.successCount || 0;
    failedRaw += res.failureCount || 0;
    res.responses.forEach((response, idx) => {
      if (response?.success) return;
      const code = response?.error?.code || "";
      if (code) {
        errorCodes[code] = (errorCodes[code] || 0) + 1;
        if (!firstErrorCode) firstErrorCode = code;
        if (!isStalePushError(code) && !firstHardErrorCode) firstHardErrorCode = code;
      }
      if (isStalePushError(code)) {
        staleFailureCount += 1;
        const badToken = batch[idx];
        if (badToken) invalidTokens.add(badToken);
      }
    });
  }
  const failed = Math.max(0, failedRaw - staleFailureCount);
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
    failedRaw,
    staleFailures: staleFailureCount,
    failed,
    firstErrorCode: firstHardErrorCode || firstErrorCode || null,
    errorCodes,
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  }).catch(() => {});
  return {
    success: true,
    sent,
    failed,
    failedRaw,
    staleFailures: staleFailureCount,
    cleaned: invalidTokens.size,
    firstErrorCode: firstHardErrorCode || null,
    firstRawErrorCode: firstErrorCode || null,
    errorCodes
  };
}

async function sendPushToUid(uid, { title, body, link, source = "direct" } = {}) {
  const targetUid = String(uid || "").trim();
  if (!targetUid) return { success: false, reason: "missing_uid", sent: 0, failed: 0 };
  const tokens = (await getUserPushTokens(targetUid)).filter(isLikelyPushToken);
  if (!tokens.length) return { success: false, reason: 'no_tokens', sent: 0, failed: 0 };
  const safeLink = normalizePushLink((link || '').toString());
  const sentAt = String(Date.now());
  const safeTitle = String(title || "FoCo After Dark").slice(0, 80);
  const safeBody = String(body || "New update from a venue.").slice(0, 180);
  const message = {
    data: {
      title: safeTitle,
      body: safeBody,
      link: safeLink,
      sentAt
    },
    webpush: {
      headers: {
        Urgency: "high",
        TTL: "2419200"
      },
      notification: {
        title: safeTitle,
        body: safeBody
      },
      fcmOptions: safeLink ? { link: safeLink } : undefined
    },
    tokens
  };
  let res = null;
  try {
    res = await messaging.sendEachForMulticast(message);
  } catch (err) {
    const code = getMessagingErrorCode(err);
    await db.collection("pushAudit").add({
      source,
      uid: targetUid,
      title: String(title || "").slice(0, 120),
      body: String(body || "").slice(0, 220),
      link: String(link || "").slice(0, 220),
      tokenCount: tokens.length,
      sent: 0,
      failedRaw: tokens.length,
      staleFailures: 0,
      failed: tokens.length,
      firstErrorCode: code,
      errorCodes: { [code]: tokens.length },
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    }).catch(() => {});
    return {
      success: false,
      reason: "send_failed",
      sent: 0,
      failed: tokens.length,
      failedRaw: tokens.length,
      staleFailures: 0,
      cleaned: 0,
      firstErrorCode: code,
      firstRawErrorCode: code
    };
  }
  const invalidTokens = [];
  const errorCodes = {};
  let firstErrorCode = null;
  let firstHardErrorCode = null;
  let staleFailureCount = 0;
  res.responses.forEach((r, idx) => {
    if (r?.success) return;
    const code = r?.error?.code || "unknown";
    errorCodes[code] = (errorCodes[code] || 0) + 1;
    if (!firstErrorCode) firstErrorCode = code;
    if (!isStalePushError(code) && !firstHardErrorCode) firstHardErrorCode = code;
    if (isStalePushError(code)) {
      staleFailureCount += 1;
      const badToken = tokens[idx];
      if (badToken) invalidTokens.push(badToken);
    }
  });
  const failedRaw = res.failureCount || 0;
  const failed = Math.max(0, failedRaw - staleFailureCount);
  if (invalidTokens.length) {
    await Promise.all(invalidTokens.map((token) => cleanupInvalidPushToken(token, { uid: targetUid })));
  }
  await db.collection("pushAudit").add({
    source,
    uid: targetUid,
    title: String(title || "").slice(0, 120),
    body: String(body || "").slice(0, 220),
    link: String(link || "").slice(0, 220),
    tokenCount: tokens.length,
    sent: res.successCount || 0,
    failedRaw,
    staleFailures: staleFailureCount,
    failed,
    firstErrorCode: firstHardErrorCode || firstErrorCode || null,
    errorCodes,
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  }).catch(() => {});
  return {
    success: true,
    sent: res.successCount || 0,
    failed,
    failedRaw,
    staleFailures: staleFailureCount,
    cleaned: invalidTokens.length,
    firstErrorCode: firstHardErrorCode || null,
    firstRawErrorCode: firstErrorCode || null
  };
}

// Push notifications: send to a user by uid
exports.sendPushToUser = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 20, maxPerDay: 500 }
  });
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
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 12, maxPerDay: 250 }
  });
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
  const body = String(data?.body || "").trim().slice(0, 160) || "New update from FoCo After Dark.";
  const link = String(data?.link || "/").trim().slice(0, 220);
  const source = String(data?.source || "manual").trim().slice(0, 60);
  try {
    return await sendPushToAll({
      title,
      body,
      link,
      source,
      dedupeKey: String(data?.dedupeKey || "").trim().slice(0, 240),
      dedupeWindowMs: Number(data?.dedupeWindowMs || 10000) || 10000,
      force: data?.force === true
    });
  } catch (err) {
    const code = getMessagingErrorCode(err);
    console.error("pushBroadcastNow failed", code, err?.message || err);
    return {
      success: false,
      reason: "dispatch_error",
      sent: 0,
      failed: 0,
      firstErrorCode: code
    };
  }
});

exports.getPushDebugStatus = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const uid = context.auth.uid;
  const tokens = await getUserPushTokens(uid);
  const tokenSuffixes = tokens.map((token) => token.slice(-12));
  const debugSnap = await db.collection('pushDebug').doc(uid).get();
  const lastSentAt = debugSnap.exists && debugSnap.data().lastSentAt
    ? debugSnap.data().lastSentAt.toMillis()
    : null;
  const lastResult = debugSnap.exists ? (debugSnap.data().lastResult || null) : null;
  return {
    tokenCount: tokens.length || 0,
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
const VOUCHER_TOKEN_PRICE_MAP = Object.freeze({
  standard: Object.freeze({
    2: "price_1T7sK8Q4Ij3ax7mavvWYm7mu",
    4: "price_1T7sLJQ4Ij3ax7maJBKbLXG6",
    6: "price_1T7sMIQ4Ij3ax7ma0blEmPhU",
  }),
  vip: Object.freeze({
    2: "price_1T7sP8Q4Ij3ax7maYl93Rw4J",
    4: "price_1T7sPuQ4Ij3ax7maztQ2rOm4",
    6: "price_1T7sRnQ4Ij3ax7maPPntTxP5",
  }),
});

const VOUCHER_TOKEN_PACK_DETAILS = Object.freeze({
  standard: Object.freeze({
    2: { tokenAmount: 2, priceCents: 600, priceLabel: "$6.00" },
    4: { tokenAmount: 4, priceCents: 1200, priceLabel: "$12.00" },
    6: { tokenAmount: 6, priceCents: 1800, priceLabel: "$18.00" },
  }),
  vip: Object.freeze({
    2: { tokenAmount: 2, priceCents: 500, priceLabel: "$5.00" },
    4: { tokenAmount: 4, priceCents: 1000, priceLabel: "$10.00" },
    6: { tokenAmount: 6, priceCents: 1500, priceLabel: "$15.00" },
  }),
});

function normalizeVoucherTokenPackId(rawPackId) {
  const parsed = Number(rawPackId);
  if (!Number.isFinite(parsed)) return 0;
  const amount = Math.trunc(parsed);
  if (amount !== 2 && amount !== 4 && amount !== 6) return 0;
  return amount;
}

function resolveVoucherTokenPackForTier(tierInput = "standard", rawPackId = 0) {
  const tier = normalizeTierKey(tierInput);
  if (tier !== "standard" && tier !== "vip") return null;
  const packId = normalizeVoucherTokenPackId(rawPackId);
  if (!packId) return null;
  const priceId = VOUCHER_TOKEN_PRICE_MAP[tier]?.[packId] || null;
  if (!priceId) return null;
  const details = VOUCHER_TOKEN_PACK_DETAILS[tier]?.[packId] || {};
  return {
    tier,
    packId,
    tokenAmount: Number(details.tokenAmount || packId),
    priceId,
    priceCents: Number(details.priceCents || 0),
    priceLabel: String(details.priceLabel || ""),
  };
}

async function applyVoucherTokenCheckoutGrant(session, eventType = "checkout.session.completed") {
  const sessionId = String(session?.id || "").trim();
  if (!sessionId) return { applied: false, reason: "missing_session_id" };
  const metadata = session?.metadata || {};
  const uid = String(metadata.uid || "").trim();
  if (!uid) return { applied: false, reason: "missing_uid" };

  const { ref: memberRef } = await getMemberContext(uid);
  return db.runTransaction(async (tx) => {
    const memberSnap = await tx.get(memberRef);
    const memberData = memberSnap.exists ? (memberSnap.data() || {}) : {};
    const memberTier = normalizeTierKey(memberData?.tier || memberData?.membershipTier || "standard");
    if (memberTier !== "standard" && memberTier !== "vip") {
      return { applied: false, reason: "tier_not_eligible", memberTier };
    }
    const pack = resolveVoucherTokenPackForTier(memberTier, metadata.packId || metadata.tokenAmount);
    if (!pack) {
      return { applied: false, reason: "pack_not_allowed", memberTier };
    }
    const requestedBillingMonthKey = String(metadata.billingMonthKey || "").trim();
    const billingMonthKey = /^\d{4}-\d{2}$/.test(requestedBillingMonthKey)
      ? requestedBillingMonthKey
      : getCurrentVoucherBillingMonthKeyForWallet(memberData, new Date());
    const existingPurchase = memberData?.extraVoucherBuckets?.[billingMonthKey]?.purchases?.[sessionId];
    if (existingPurchase) {
      return { applied: false, reason: "already_applied", billingMonthKey, tokenAmount: pack.tokenAmount };
    }
    const paymentIntentId = String(session?.payment_intent || "").trim() || null;
    const purchasePath = `extraVoucherBuckets.${billingMonthKey}.purchases.${sessionId}`;
    const updates = {
      [`extraVoucherBuckets.${billingMonthKey}.redemptionTokens`]: admin.firestore.FieldValue.increment(pack.tokenAmount),
      [`extraVoucherBuckets.${billingMonthKey}.updatedAt`]: admin.firestore.FieldValue.serverTimestamp(),
      [purchasePath]: {
        tokenAmount: pack.tokenAmount,
        packId: String(pack.packId),
        stripeSessionId: sessionId,
        stripePaymentIntentId: paymentIntentId,
        purchasedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      lastVoucherPurchase: admin.firestore.FieldValue.serverTimestamp(),
      lastStripeEvent: eventType,
      lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    tx.set(memberRef, updates, { merge: true });
    return { applied: true, billingMonthKey, tokenAmount: pack.tokenAmount };
  });
}

const stripeSecrets = {
  secrets: ["STRIPE_SECRET", "STRIPE_PUBLISHABLE", "REPORTS_SMTP_USER", "REPORTS_SMTP_PASS"]
};
const STRIPE_MEMBERSHIP_PRICE_IDS = Object.freeze({
  standard: Object.freeze({
    month: "price_1T4Z1KQ4Ij3ax7ma27cvoyeO",
    year: "price_1T7KlbQ4Ij3ax7maGJmhVJjk",
  }),
  vip: Object.freeze({
    month: "price_1T4Z2mQ4Ij3ax7maznTyPfl4",
    year: "price_1T7KjOQ4Ij3ax7ma60cwQBXm",
  }),
});
const STRIPE_PROMOS = {
  FOCOFAM20: {
    promotionCodeId: "promo_1Sz1GSQ4Ij3ax7macXklpdje",
  },
  CSU30: {
    code: "CSU30",
    legacyCode: "CSU50",
    promotionCodeId: "promo_1T7pq1Q4Ij3ax7maqhmQn3wn",
  },
};
const POINTS_TOKEN_REDEMPTIONS = Object.freeze([
  Object.freeze({ id: "pts_500_2", pointsCost: 500, tokenAmount: 2 }),
  Object.freeze({ id: "pts_700_3", pointsCost: 700, tokenAmount: 3 }),
  Object.freeze({ id: "pts_1000_5", pointsCost: 1000, tokenAmount: 5 }),
]);
function getPointsTokenRedemptionOption(redemptionId = "") {
  const normalized = String(redemptionId || "").trim().toLowerCase();
  return POINTS_TOKEN_REDEMPTIONS.find((item) => item.id === normalized) || null;
}
const stripeWebhookSecrets = { secrets: ["STRIPE_SECRET", "STRIPE_WEBHOOK_SECRET"] };

exports.redeemPoints = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const redemptionOptionId = String(data?.redemptionId || "").trim().toLowerCase();
  const option = getPointsTokenRedemptionOption(redemptionOptionId);
  if (!option) throw new HttpsError("invalid-argument", "Invalid points redemption option.");
  const operationIdRaw = String(data?.operationId || "").trim().slice(0, 80);
  const operationId = operationIdRaw || `pts_${uid}_${Date.now()}`;
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
  if (available < option.pointsCost) {
    throw new HttpsError("failed-precondition", "Not enough points.");
  }
  const result = await db.runTransaction(async (tx) => {
    const snap = await tx.get(memberRef);
    const dataSnap = snap.exists ? (snap.data() || {}) : {};
    const current = Math.max(0, Number(dataSnap.points || 0));
    if (current < option.pointsCost) throw new HttpsError("failed-precondition", "Not enough points.");

    const billingMonthKey = getCurrentVoucherBillingMonthKeyForWallet(dataSnap, new Date());
    const existing = dataSnap?.extraVoucherBuckets?.[billingMonthKey]?.pointRedemptions?.[operationId];
    const existingBucketTokens = Math.max(0, Number(dataSnap?.extraVoucherBuckets?.[billingMonthKey]?.redemptionTokens || 0));
    if (existing) {
      return {
        alreadyApplied: true,
        billingMonthKey,
        points: current,
        tokenGrant: Number(existing.tokenAmount || option.tokenAmount),
        currentCycleTokens: existingBucketTokens
      };
    }

    const nextPoints = current - option.pointsCost;
    const redemptionPath = `extraVoucherBuckets.${billingMonthKey}.pointRedemptions.${operationId}`;
    tx.set(memberRef, {
      points: nextPoints,
      pointsUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
      lastPointsRedemptionAt: admin.firestore.FieldValue.serverTimestamp(),
      lastPointsRedemptionType: "tokens",
      pointsRedeemedTotal: admin.firestore.FieldValue.increment(option.pointsCost),
      [`extraVoucherBuckets.${billingMonthKey}.redemptionTokens`]: admin.firestore.FieldValue.increment(option.tokenAmount),
      [`extraVoucherBuckets.${billingMonthKey}.updatedAt`]: admin.firestore.FieldValue.serverTimestamp(),
      [redemptionPath]: {
        redemptionOptionId: option.id,
        pointsCost: option.pointsCost,
        tokenAmount: option.tokenAmount,
        redeemedAt: admin.firestore.FieldValue.serverTimestamp()
      }
    }, { merge: true });
    return {
      alreadyApplied: false,
      billingMonthKey,
      points: nextPoints,
      tokenGrant: option.tokenAmount,
      currentCycleTokens: existingBucketTokens + option.tokenAmount
    };
  });
  return {
    ok: true,
    type: "tokens",
    redemptionOptionId: option.id,
    ...result
  };
});

function isCeoContext(context) {
  if (!context?.auth) return false;
  const email = (context.auth.token?.email || "").toLowerCase();
  return (
    context.auth.token?.ceo === true ||
    context.auth.token?.admin === true ||
    context.auth.uid === CEO_UID ||
    email === CEO_EMAIL
  );
}

function requireAdminClaim(context) {
  if (!context?.auth) {
    throw new HttpsError("unauthenticated", "Auth required");
  }
  if (!isCeoContext(context)) {
    throw new HttpsError("permission-denied", "Admin claim required");
  }
}

function normalizeNameInput(value = "", maxLen = 60) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, maxLen);
}

function normalizeEmailInput(value = "") {
  return String(value || "").trim().toLowerCase().slice(0, 254);
}

function isValidEmailFormat(value = "") {
  return /^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/i.test(String(value || ""));
}

function buildWaitlistDocId(email = "") {
  return hashLookupKey(normalizeEmailInput(email));
}

function htmlToText(html = "") {
  return String(html || "")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 12000);
}

function sleep(ms = 0) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

async function queueOrSendEmail({ to = "", subject = "", html = "", text = "" }) {
  const email = normalizeEmailInput(to);
  if (!email || !subject) {
    return { sent: false, error: "Missing email or subject" };
  }
  const safeHtml = String(html || "").slice(0, 40000);
  const safeText = String(text || htmlToText(safeHtml) || "").slice(0, 12000);
  const smtpConfigured = !!(process.env.REPORTS_SMTP_USER && process.env.REPORTS_SMTP_PASS);
  if (smtpConfigured) {
    return sendReportEmail(subject, safeText, { to: email, html: safeHtml });
  }
  try {
    await db.collection("mail").add({
      to: email,
      message: {
        subject: String(subject).slice(0, 200),
        text: safeText,
        html: safeHtml
      },
      meta: {
        type: "waitlist_blast"
      },
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });
    return { sent: true, queued: true };
  } catch (err) {
    return { sent: false, error: err?.message || "Queue write failed" };
  }
}

function safeJsonClone(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function sanitizeConfigSectionValue(section = "", value) {
  const normalized = String(section || "").toLowerCase();
  if (!APP_PUBLIC_CONFIG_SECTIONS.has(normalized)) {
    throw new HttpsError("invalid-argument", "Invalid config section");
  }
  if (normalized === "flags") {
    if (typeof value !== "boolean") {
      throw new HttpsError("invalid-argument", "flags values must be boolean");
    }
    return value;
  }
  if (normalized === "text") {
    if (typeof value !== "string") {
      throw new HttpsError("invalid-argument", "text values must be string");
    }
    return value.slice(0, 4000);
  }
  if (normalized === "numbers") {
    const next = Number(value);
    if (!Number.isFinite(next)) {
      throw new HttpsError("invalid-argument", "numbers values must be numeric");
    }
    return next;
  }
  if (normalized === "lists") {
    if (!Array.isArray(value)) {
      throw new HttpsError("invalid-argument", "lists values must be arrays");
    }
    return safeJsonClone(value).slice(0, 500);
  }
  if (normalized === "style") {
    const valueType = typeof value;
    if (valueType === "undefined" || valueType === "function") {
      throw new HttpsError("invalid-argument", "style values must be JSON-safe");
    }
    return safeJsonClone(value);
  }
  throw new HttpsError("invalid-argument", "Unsupported config section");
}

function normalizeVenueStatusValue(value) {
  const status = String(value || "active").trim().toLowerCase();
  return VENUE_STATUS_VALUES.has(status) ? status : "active";
}

function getAdminActorRef(context) {
  return {
    uid: String(context?.auth?.uid || ""),
    email: String(context?.auth?.token?.email || "").toLowerCase()
  };
}

function configFlagEnabled(configData, key, defaultVal = true) {
  const flags = configData && typeof configData.flags === "object" ? configData.flags : {};
  const raw = flags?.[key];
  return typeof raw === "boolean" ? raw : defaultVal;
}

async function assertBillingEnabledOrThrow() {
  const snap = await db.doc(APP_PUBLIC_CONFIG_DOC_PATH).get();
  const data = snap.exists ? (snap.data() || {}) : {};
  const enabled = configFlagEnabled(data, "billing.enabled", true);
  if (!enabled) {
    throw new HttpsError("failed-precondition", "Purchases are temporarily unavailable. Please check back shortly.", {
      reason: "BILLING_DISABLED",
      memberMessage: "Purchases are temporarily unavailable. Please check back shortly.",
      staffMessage: "Billing disabled by Control Center.",
    });
  }
}

function isCeoMemberDoc(data = {}, uid = "") {
  const email = (data.email || "").toLowerCase();
  return (
    data.ceo === true ||
    uid === CEO_UID ||
    email === CEO_EMAIL
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

function normalizeBillingIntervalKey(interval = "month") {
  const key = String(interval || "month").trim().toLowerCase();
  if (key === "year" || key === "annual" || key === "yearly") return "year";
  return "month";
}

function assertValidPurchasableTier(tier = "") {
  const key = String(tier || "").trim().toLowerCase();
  if (!["standard", "vip"].includes(key)) {
    throw new HttpsError("invalid-argument", "Invalid membership tier. Use 'standard' or 'vip'.");
  }
  return key;
}

function priceIdForMembershipTier(tier = "", interval = "month") {
  const key = assertValidPurchasableTier(tier);
  const normalizedInterval = normalizeBillingIntervalKey(interval);
  const tierMap = STRIPE_MEMBERSHIP_PRICE_IDS[key] || {};
  const priceId = String(tierMap[normalizedInterval] || "").trim();
  if (!priceId) {
    throw new HttpsError("failed-precondition", `Missing Stripe price mapping for tier '${key}' interval '${normalizedInterval}'.`);
  }
  return { tier: key, interval: normalizedInterval, priceId };
}

function membershipTierLabel(tier = "standard") {
  return normalizeTierKey(tier).toUpperCase();
}

function getSubscriptionInterval(subscription = null, fallback = "month") {
  const recurringInterval = String(
    subscription?.items?.data?.[0]?.price?.recurring?.interval
    || subscription?.plan?.interval
    || subscription?.metadata?.interval
    || fallback
  ).toLowerCase();
  return normalizeBillingIntervalKey(recurringInterval);
}

function isoFromUnix(seconds) {
  const value = Number(seconds || 0);
  if (!Number.isFinite(value) || value <= 0) return null;
  return new Date(value * 1000).toISOString();
}

function normalizeDobParts(input = null) {
  if (!input || typeof input !== "object") return null;
  const year = Number(input.year || 0);
  const month = Number(input.month || 0);
  const day = Number(input.day || 0);
  if (!Number.isFinite(year) || year < 1900 || year > 2100) return null;
  if (!Number.isFinite(month) || month < 1 || month > 12) return null;
  if (!Number.isFinite(day) || day < 1 || day > 31) return null;
  return { year, month, day };
}

function dobPartsFromDateInput(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return {
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
  };
}

function calculateAgeFromDobParts(dob = null, now = new Date()) {
  if (!dob) return null;
  const year = Number(dob.year || 0);
  const month = Number(dob.month || 0);
  const day = Number(dob.day || 0);
  if (!year || !month || !day) return null;
  let age = now.getUTCFullYear() - year;
  const monthDiff = (now.getUTCMonth() + 1) - month;
  if (monthDiff < 0 || (monthDiff === 0 && now.getUTCDate() < day)) {
    age -= 1;
  }
  return age;
}

function stripeStatusToPaymentStatus(status = "") {
  const key = String(status || "").toLowerCase();
  if (key === "active" || key === "trialing") return "active";
  if (key === "past_due" || key === "unpaid" || key === "incomplete") return "past_due";
  if (key === "canceled" || key === "incomplete_expired") return "canceled";
  return "active";
}

async function deleteCollectionDocs(queryBase, batchSize = 300) {
  let snap = await queryBase.limit(batchSize).get();
  while (!snap.empty) {
    const batch = db.batch();
    snap.docs.forEach((docSnap) => batch.delete(docSnap.ref));
    await batch.commit();
    if (snap.size < batchSize) break;
    snap = await queryBase.limit(batchSize).get();
  }
}

async function purgeMemberFirestoreRecords(uid, memberDataOverride = null) {
  const memberRef = db.collection("members").doc(uid);
  let memberSnap = null;
  let memberData = {};
  if (memberDataOverride && typeof memberDataOverride === "object") {
    memberData = memberDataOverride;
  } else {
    memberSnap = await memberRef.get();
    memberData = memberSnap.exists ? (memberSnap.data() || {}) : {};
  }
  if (isCeoMemberDoc(memberData, uid)) {
    return { skipped: true, reason: "ceo", memberDeleted: false, usernameDeleted: false };
  }
  const username = String(memberData.username || "").trim().toLowerCase();
  const passCode = String(memberData.passCode || memberData.passId || "").trim().toUpperCase();
  let usernameDeleted = false;

  try { await deleteCollectionDocs(memberRef.collection("redemptions")); } catch (_) {}
  try { await deleteCollectionDocs(memberRef.collection("alertCheckins")); } catch (_) {}
  try {
    const pushTokensRef = db.collection("pushTokens").doc(uid);
    await deleteCollectionDocs(pushTokensRef.collection("tokens"));
    await pushTokensRef.delete().catch(() => {});
  } catch (_) {}

  if (username) {
    try {
      const unameRef = db.collection("usernames").doc(username);
      const unameSnap = await unameRef.get();
      if (unameSnap.exists && String(unameSnap.data()?.uid || "") === uid) {
        await unameRef.delete();
        usernameDeleted = true;
      }
    } catch (_) {}
  }
  if (passCode) {
    try { await db.collection("passes").doc(passCode).delete(); } catch (_) {}
  }

  if (!memberSnap) memberSnap = await memberRef.get();
  if (memberSnap.exists) {
    await memberRef.delete().catch(() => {});
    try {
      await db.collection("settings").doc("appStats").set({
        membersCount: admin.firestore.FieldValue.increment(-1),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    } catch (_) {}
  }
  return { skipped: false, memberDeleted: !!memberSnap.exists, usernameDeleted };
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
    const purgeResult = await purgeMemberFirestoreRecords(uid, memberData);
    if (purgeResult.memberDeleted) deletedMemberDocs += 1;
    if (purgeResult.usernameDeleted) deletedUsernames += 1;
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

const stripePromotionCodeIdCache = new Map();

async function getStripePromotionCodeIdByCode(stripe, code = "") {
  const normalizedCode = normalizePromoCodeInput(code);
  if (!normalizedCode) {
    throw new HttpsError("invalid-argument", "Promo code is required.");
  }
  if (stripePromotionCodeIdCache.has(normalizedCode)) {
    return stripePromotionCodeIdCache.get(normalizedCode);
  }
  const list = await stripe.promotionCodes.list({
    code: normalizedCode,
    active: true,
    limit: 20,
  });
  const match = (list?.data || []).find((entry) =>
    entry?.active === true && normalizePromoCodeInput(entry?.code || "") === normalizedCode
  );
  if (!match?.id) {
    throw new HttpsError("failed-precondition", `Promo code '${normalizedCode}' is not configured in Stripe.`);
  }
  stripePromotionCodeIdCache.set(normalizedCode, match.id);
  return match.id;
}

function normalizeCeoFreeCodeInput(raw = "") {
  return String(raw || "")
    .toUpperCase()
    .replace(/[^A-Z0-9-]/g, "")
    .slice(0, 28);
}

function buildCeoFreeCodeDoc(code = "", context = {}) {
  return {
    code,
    oneTime: true,
    maxUses: 1,
    uses: 0,
    consumed: false,
    revoked: false,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdByUid: context?.auth?.uid || null,
    createdByEmail: (context?.auth?.token?.email || "").toLowerCase() || null,
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  };
}

async function ensureMemberProfileForCeoFree(uid = "", email = "") {
  if (!uid) throw new HttpsError("unauthenticated", "Auth required");
  const memberRef = db.collection("members").doc(uid);
  let memberSnap = await memberRef.get();
  let memberData = memberSnap.exists ? (memberSnap.data() || {}) : {};
  if (!memberSnap.exists) {
    const passCode = await ensureUniquePassCode();
    const usernameSeed = String((email || "").split("@")[0] || "member")
      .toLowerCase()
      .replace(/[^a-z0-9_]/g, "")
      .slice(0, 16) || `member_${uid.slice(0, 6).toLowerCase()}`;
    await memberRef.set({
      email: String(email || "").toLowerCase(),
      username: usernameSeed,
      passCode,
      passId: passCode,
      tier: "standard",
      freeMembership: false,
      revoked: false,
      memberSince: new Date().toISOString(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    await db.collection("passes").doc(passCode).set({
      uid,
      passCode,
      tier: "standard",
      status: "active",
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    memberSnap = await memberRef.get();
    memberData = memberSnap.exists ? (memberSnap.data() || {}) : {};
  }

  let passCode = String(memberData.passCode || memberData.passId || "").trim().toUpperCase();
  if (!passCode) {
    passCode = await ensureUniquePassCode();
    await memberRef.set({
      passCode,
      passId: passCode,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    memberData = { ...memberData, passCode, passId: passCode };
  }

  await db.collection("passes").doc(passCode).set({
    uid,
    passCode,
    tier: memberData.tier || memberData.membershipTier || "standard",
    status: memberData.revoked ? "revoked" : "active",
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });

  return { memberRef, memberData, passCode };
}

async function resolveMembershipPromo({
  uid,
  memberDocData,
  promoCodeInput,
  authEmail = "",
  stripe = null,
  interval = "month"
}) {
  const isEduEligibleEmail = (rawEmail = "") => {
    const email = String(rawEmail || "").trim().toLowerCase();
    if (!email || !email.includes("@")) return false;
    const domain = email.split("@").pop() || "";
    return email.endsWith(".edu") || domain.includes("colostate.edu") || domain.includes("csu.edu");
  };
  const promoInput = normalizePromoCodeInput(promoCodeInput);
  const billingInterval = normalizeBillingIntervalKey(interval);
  const resolveCouponFromPromotionCode = async (promotionCodeId) => {
    const trimmedId = String(promotionCodeId || "").trim();
    if (!trimmedId || !stripe) return "";
    try {
      const promo = await stripe.promotionCodes.retrieve(trimmedId);
      const couponId = String(promo?.coupon?.id || "").trim();
      return couponId;
    } catch (err) {
      console.warn("Failed to resolve coupon for promo code", trimmedId, readStripeErrorMessage(err));
      return "";
    }
  };
  if (promoInput) {
    if (promoInput === "FOCOFAM20") {
      return {
        discount: { promotion_code: STRIPE_PROMOS.FOCOFAM20.promotionCodeId },
        promoTag: "focofam20",
        promoCode: promoInput,
        firstMonthOnly: billingInterval === "month",
        promotionCodeId: STRIPE_PROMOS.FOCOFAM20.promotionCodeId,
        couponId: "",
      };
    }
    if (promoInput === STRIPE_PROMOS.CSU30.code || promoInput === STRIPE_PROMOS.CSU30.legacyCode) {
      if (!isEduEligibleEmail(authEmail)) {
        throw new HttpsError(
          "failed-precondition",
          "CSU student promo is only available for approved .edu or CSU email addresses.",
          { reason: "PROMO_EDU_ONLY" }
        );
      }
      const promotionCodeId = String(STRIPE_PROMOS.CSU30.promotionCodeId || "").trim();
      if (!promotionCodeId && !stripe) {
        throw new HttpsError("failed-precondition", "Promo validation requires Stripe to be configured.");
      }
      const resolvedPromotionCodeId = promotionCodeId || await getStripePromotionCodeIdByCode(stripe, STRIPE_PROMOS.CSU30.code);
      const resolvedCouponId = await resolveCouponFromPromotionCode(resolvedPromotionCodeId);
      return {
        // Prefer coupon id to avoid Stripe "new customer only" promotion-code restrictions.
        discount: resolvedCouponId ? { coupon: resolvedCouponId } : { promotion_code: resolvedPromotionCodeId },
        promoTag: "csu30",
        promoCode: STRIPE_PROMOS.CSU30.legacyCode,
        firstMonthOnly: false,
        promotionCodeId: resolvedPromotionCodeId,
        couponId: resolvedCouponId,
      };
    }
    throw new HttpsError("invalid-argument", "Invalid promo code.");
  }
  return {
    discount: null,
    promoTag: null,
    promoCode: "",
    firstMonthOnly: false,
    promotionCodeId: "",
    couponId: "",
  };
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

function isAgeVerificationExempt(token = {}, memberDocData = {}) {
  const overrideRaw = memberDocData?.membershipOverride
    || memberDocData?.override
    || memberDocData?.membership_override
    || memberDocData?.membershipTierOverride
    || "";
  const override = String(overrideRaw || "").toUpperCase();
  const email = String(token?.email || memberDocData?.email || "").toLowerCase();
  return (
    token?.ceo === true
    || token?.admin === true
    || token?.beta === true
    || memberDocData?.ceo === true
    || email === CEO_EMAIL
    || email === BETA_EMAIL
    || email.includes("beta@")
    || override.includes("BETA")
  );
}

async function applyIdentityVerificationUpdate(session, eventType = "identity.verification_session.updated") {
  if (!session) return;
  const memberCtx = await resolveMemberContextFromStripeObject(session);
  if (!memberCtx?.ref) {
    console.warn("Identity webhook: member not found", session?.id || "", eventType);
    return;
  }
  const stripeDob = normalizeDobParts(session?.verified_outputs?.dob || null);
  const fallbackDob = dobPartsFromDateInput(memberCtx.data?.birthDate || null);
  const dob = stripeDob || fallbackDob;
  const age = calculateAgeFromDobParts(dob);
  const baseUpdates = {
    ageVerificationRequired: true,
    ageVerificationSessionId: session?.id || memberCtx.data?.ageVerificationSessionId || null,
    ageVerificationSessionStatus: String(session?.status || "").toLowerCase() || null,
    ageVerificationLastEvent: eventType,
    ageVerificationLastEventAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  const underage = Number.isFinite(age) ? age < 21 : false;
  if (eventType === "identity.verification_session.verified" || String(session?.status || "").toLowerCase() === "verified") {
    if (underage) {
      await memberCtx.ref.set({
        ...baseUpdates,
        ageVerificationStatus: "underage",
        ageVerified21: false,
        ageVerificationFailedReason: "UNDER_21",
        ageVerificationCompletedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return;
    }
    await memberCtx.ref.set({
      ...baseUpdates,
      ageVerificationStatus: "verified",
      ageVerified21: true,
      ageVerificationFailedReason: admin.firestore.FieldValue.delete(),
      ageVerificationCompletedAt: admin.firestore.FieldValue.serverTimestamp(),
      ageVerifiedAt: admin.firestore.FieldValue.serverTimestamp(),
      onboardingEligible: true,
    }, { merge: true });
    return;
  }
  if (eventType === "identity.verification_session.requires_input" || String(session?.status || "").toLowerCase() === "requires_input") {
    await memberCtx.ref.set({
      ...baseUpdates,
      ageVerificationStatus: "requires_input",
      ageVerified21: false,
      ageVerificationFailedReason: "REQUIRES_INPUT",
    }, { merge: true });
    return;
  }
  if (eventType === "identity.verification_session.canceled" || String(session?.status || "").toLowerCase() === "canceled") {
    await memberCtx.ref.set({
      ...baseUpdates,
      ageVerificationStatus: "canceled",
      ageVerified21: false,
      ageVerificationFailedReason: "CANCELED",
    }, { merge: true });
    return;
  }
  await memberCtx.ref.set({
    ...baseUpdates,
    ageVerificationStatus: "processing",
    ageVerified21: false,
    ageVerificationFailedReason: admin.firestore.FieldValue.delete(),
  }, { merge: true });
}

exports.createAgeVerificationSession = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context?.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  await checkRateLimit(uid, { maxPerMin: 30, maxPerDay: 400 });
  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  if (isAgeVerificationExempt(context.auth.token, memberDocData)) {
    return {
      ok: true,
      bypassed: true,
      verified: true,
      status: "exempt"
    };
  }
  if (memberDocData?.ageVerified21 === true || String(memberDocData?.ageVerificationStatus || "").toLowerCase() === "verified") {
    return {
      ok: true,
      verified: true,
      status: "verified"
    };
  }
  const rawTier = normalizeTierKey((data?.tier || memberDocData?.requestedTier || "standard").toString());
  const returnUrlRaw = String(data?.returnUrl || "https://foco-after-dark.web.app").trim();
  if (!/^https?:\/\//i.test(returnUrlRaw)) {
    throw new HttpsError("invalid-argument", "returnUrl must be an absolute URL.");
  }
  const birthDateInput = data?.birthDate || memberDocData?.birthDate || null;
  const birthDob = dobPartsFromDateInput(birthDateInput);
  if (!birthDob) {
    throw new HttpsError("failed-precondition", "Birthdate is required before age verification.");
  }
  const ageFromInput = calculateAgeFromDobParts(birthDob);
  if (Number.isFinite(ageFromInput) && ageFromInput < 21) {
    await memberRef.set({
      ageVerificationRequired: true,
      ageVerificationStatus: "underage",
      ageVerified21: false,
      ageVerificationFailedReason: "UNDER_21_INPUT",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    throw new HttpsError("failed-precondition", "You must be 21+ to use FoCo After Dark.");
  }
  const stripe = getStripeClient();
  const sessionPayload = {
    type: "document",
    metadata: {
      uid,
      tier: rawTier,
    },
    return_url: returnUrlRaw,
  };
  let session;
  try {
    session = await stripe.identity.verificationSessions.create(sessionPayload);
  } catch (err) {
    const message = String(err?.message || "").trim();
    const code = String(err?.code || err?.raw?.code || "").trim();
    console.error("createAgeVerificationSession stripe error", { uid, code, message });
    if (message || code) {
      throw new HttpsError(
        "failed-precondition",
        `ID verification is temporarily unavailable. ${message || code}`
      );
    }
    throw new HttpsError("internal", "Could not start ID verification right now.");
  }
  await memberRef.set({
    requestedTier: rawTier,
    onboardingEligible: false,
    ageVerificationRequired: true,
    ageVerificationStatus: "processing",
    ageVerificationSessionStatus: "processing",
    ageVerified21: false,
    ageVerificationSessionId: session.id,
    ageVerificationStartedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
  return {
    ok: true,
    verified: false,
    status: "processing",
    sessionId: session.id,
    url: session.url,
  };
});

exports.getAgeVerificationStatus = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context?.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  if (isAgeVerificationExempt(context.auth.token, memberDocData)) {
    return {
      exempt: true,
      verified: true,
      status: "exempt",
    };
  }
  const forceSync = data?.forceSync === true;
  const sessionId = String(memberDocData?.ageVerificationSessionId || "").trim();
  if (forceSync && sessionId) {
    try {
      const stripe = getStripeClient();
      const session = await stripe.identity.verificationSessions.retrieve(sessionId);
      if (session) {
        const mappedEvent = `identity.verification_session.${String(session.status || "").toLowerCase() || "processing"}`;
        await applyIdentityVerificationUpdate(session, mappedEvent);
      }
    } catch (err) {
      console.warn("getAgeVerificationStatus forceSync failed", err?.message || err);
    }
  }
  const freshSnap = await memberRef.get();
  const fresh = freshSnap.exists ? (freshSnap.data() || {}) : {};
  const status = String(fresh?.ageVerificationStatus || "required").toLowerCase();
  return {
    exempt: false,
    verified: fresh?.ageVerified21 === true || status === "verified",
    status,
    sessionId: String(fresh?.ageVerificationSessionId || ""),
    required: fresh?.ageVerificationRequired !== false,
    requestedTier: normalizeTierKey((fresh?.requestedTier || "standard").toString()),
  };
});

async function ensureStripeCustomer({ stripe, memberRef, memberDocData, uid, email, token }) {
  if (isStripeExcluded(token, memberDocData)) {
    throw stripeExcludedError();
  }
  const existingCustomerId = String(memberDocData?.stripeCustomerId || "").trim();
  if (existingCustomerId) {
    try {
      const existingCustomer = await stripe.customers.retrieve(existingCustomerId);
      if (existingCustomer && !existingCustomer.deleted) {
        const existingEmail = String(existingCustomer.email || "").toLowerCase();
        if (email && existingEmail !== email) {
          await stripe.customers.update(existingCustomerId, { email });
        }
        return existingCustomerId;
      }
    } catch (err) {
      const errCode = String(err?.code || err?.raw?.code || "").toLowerCase();
      const errType = String(err?.type || err?.raw?.type || "").toLowerCase();
      if (errCode !== "resource_missing" && errType !== "invalid_request_error") {
        throw err;
      }
      console.warn("ensureStripeCustomer: stale stripeCustomerId, creating replacement", existingCustomerId);
    }
  }
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

function membershipStripePriceSelection(rawTier, rawInterval, source = "unknown") {
  const { tier, interval, priceId } = priceIdForMembershipTier(rawTier, rawInterval);
  console.info("membership_price_selection", { source, tier, interval, priceId });
  return { tier, interval, priceId };
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

function readStripeErrorMessage(err) {
  return String(
    err?.raw?.message
    || err?.message
    || err?.error?.message
    || ""
  ).trim();
}

function isStripePromoError(err) {
  const msg = readStripeErrorMessage(err).toLowerCase();
  return (
    msg.includes("promotion code")
    || msg.includes("promotion_code")
    || msg.includes("coupon")
    || msg.includes("discount")
  );
}

function buildMembershipPricingPreview(subscription = null, paymentIntent = null) {
  const invoice = subscription?.latest_invoice || null;
  const itemPriceCents = Number(subscription?.items?.data?.[0]?.price?.unit_amount || 0);
  const subtotalRaw = Number(invoice?.subtotal);
  const totalRaw = Number(invoice?.total);
  const paymentIntentAmount = Number(paymentIntent?.amount);
  const subtotalCents = Number.isFinite(subtotalRaw) && subtotalRaw >= 0
    ? subtotalRaw
    : (itemPriceCents > 0 ? itemPriceCents : (Number.isFinite(paymentIntentAmount) ? paymentIntentAmount : 0));
  const totalCents = Number.isFinite(totalRaw) && totalRaw >= 0
    ? totalRaw
    : (Number.isFinite(paymentIntentAmount) ? paymentIntentAmount : subtotalCents);
  const discountCents = Math.max(0, subtotalCents - totalCents);
  const currency = String(invoice?.currency || paymentIntent?.currency || "usd").toLowerCase();
  return { subtotalCents, totalCents, discountCents, currency };
}

function subscriptionHasAnyDiscount(subscription = null) {
  const directDiscount = subscription?.discount;
  const multiDiscounts = Array.isArray(subscription?.discounts?.data) ? subscription.discounts.data : [];
  return Boolean((directDiscount && (directDiscount.id || directDiscount.coupon || directDiscount.promotion_code)) || multiDiscounts.length > 0);
}

async function upsertMembershipSubscription({ stripe, memberRef, memberDocData, uid, email, tier, interval, token, discount, promoTag }) {
  const {
    tier: normalizedTier,
    interval: normalizedInterval,
    priceId: tierPriceId
  } = membershipStripePriceSelection(tier, interval, "upsertMembershipSubscription");
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
  const createSubscription = async ({ discountInput = discount, promoMetaInput = promoMeta } = {}) => {
    const createPayload = {
      customer: customerId,
      items: [{ price: tierPriceId }],
      payment_behavior: "default_incomplete",
      trial_from_plan: false,
      payment_settings: { save_default_payment_method: "on_subscription" },
      metadata: { uid, tier: normalizedTier, interval: normalizedInterval, ...promoMetaInput },
      discounts: discountInput ? [discountInput] : undefined,
      expand: ["latest_invoice.payment_intent", "items.data.price"],
    };
    try {
      return await stripe.subscriptions.create(createPayload);
    } catch (err) {
      if (discountInput && isStripePromoError(err)) {
        const promoErr = readStripeErrorMessage(err) || "Promo code could not be applied.";
        throw new HttpsError("failed-precondition", promoErr, { reason: "PROMO_APPLY_FAILED" });
      }
      throw err;
    }
  };

  if (hasActiveSubscription) {
    const subscriptionStatus = String(subscription?.status || "").toLowerCase();
    const currentItem = subscription.items?.data?.[0];
    const currentTier = normalizeTierKey(subscription.metadata?.tier || memberDocData?.tier || normalizedTier);
    const currentInterval = getSubscriptionInterval(subscription, memberDocData?.billingInterval || normalizedInterval);
    const currentPriceId = String(currentItem?.price?.id || "").trim();
    const planChanged = currentTier !== normalizedTier
      || currentInterval !== normalizedInterval
      || (currentPriceId && currentPriceId !== tierPriceId);
    const recreateForPendingSubscription = planChanged && ["incomplete", "past_due", "unpaid"].includes(subscriptionStatus);
    if (recreateForPendingSubscription) {
      try {
        await stripe.subscriptions.cancel(subscription.id, { invoice_now: false, prorate: false });
      } catch (cancelErr) {
        console.warn("Failed to cancel pending subscription before recreate", subscription.id, cancelErr?.message || cancelErr);
      }
      subscription = await createSubscription();
    } else if (currentItem && planChanged) {
      try {
        subscription = await stripe.subscriptions.update(subscription.id, {
          cancel_at_period_end: false,
          proration_behavior: "create_prorations",
          billing_cycle_anchor: "unchanged",
          metadata: { uid, tier: normalizedTier, interval: normalizedInterval, ...promoMeta },
          items: [{
            id: currentItem.id,
            price: tierPriceId,
          }],
          discounts: discount ? [discount] : undefined,
          expand: ["latest_invoice.payment_intent", "items.data.price"],
        });
      } catch (err) {
        if (discount && isStripePromoError(err)) {
          const promoErr = readStripeErrorMessage(err) || "Promo code could not be applied.";
          throw new HttpsError("failed-precondition", promoErr, { reason: "PROMO_APPLY_FAILED" });
        }
        throw err;
      }
    } else if (subscription.metadata?.tier !== normalizedTier || subscription.metadata?.interval !== normalizedInterval) {
      subscription = await stripe.subscriptions.update(subscription.id, {
        metadata: { uid, tier: normalizedTier, interval: normalizedInterval, ...promoMeta },
        expand: ["latest_invoice.payment_intent", "items.data.price"],
      });
    } else if (discount) {
      try {
        subscription = await stripe.subscriptions.update(subscription.id, {
          cancel_at_period_end: false,
          metadata: { uid, tier: normalizedTier, interval: normalizedInterval, ...promoMeta },
          discounts: [discount],
          proration_behavior: "none",
          expand: ["latest_invoice.payment_intent", "items.data.price"],
        });
      } catch (err) {
        if (isStripePromoError(err)) {
          const promoErr = readStripeErrorMessage(err) || "Promo code could not be applied.";
          throw new HttpsError("failed-precondition", promoErr, { reason: "PROMO_APPLY_FAILED" });
        }
        throw err;
      }
    }
  } else {
    subscription = await createSubscription();
  }

  const paymentIntent = subscription?.latest_invoice?.payment_intent || null;
  const currentPeriodEndIso = isoFromUnix(subscription?.current_period_end);
  const subscriptionPromoTag = String(subscription?.metadata?.promo || promoTag || "").toLowerCase();
  const billingInterval = getSubscriptionInterval(subscription, normalizedInterval);
  const updatePayload = {
    tier: normalizedTier,
    membershipTier: membershipTierLabel(normalizedTier),
    billingInterval,
    membershipStatus: subscription?.status || "active",
    paymentStatus: stripeStatusToPaymentStatus(subscription?.status),
    billingProvider: "stripe",
    stripeCustomerId: subscription?.customer || customerId,
    stripeSubscriptionId: subscription?.id || null,
    membershipPromoTag: subscriptionPromoTag || null,
    membershipTrialDays: admin.firestore.FieldValue.delete(),
    membershipTrialEndsAt: admin.firestore.FieldValue.delete(),
    currentPeriodEnd: currentPeriodEndIso,
    nextRenewal: currentPeriodEndIso,
    cancelAtPeriodEnd: subscription?.cancel_at_period_end === true,
    accountDeleteAt: admin.firestore.FieldValue.delete(),
    accountDeleteReason: admin.firestore.FieldValue.delete(),
    accountDeleteScheduledAt: admin.firestore.FieldValue.delete(),
    lastStripeEvent: "subscription_upsert",
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  await memberRef.set(updatePayload, { merge: true });

  return { subscription, paymentIntent, billingInterval };
}

exports.createMembershipCheckoutSession = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  await assertBillingEnabledOrThrow();
  const uid = context.auth.uid;
  const email = (context.auth.token.email || "").toLowerCase();
  const {
    tier,
    interval,
    priceId
  } = membershipStripePriceSelection(data?.tier, data?.interval, "createMembershipCheckoutSession");
  const returnUrlRaw = String(data?.returnUrl || "https://foco-after-dark.web.app").trim();
  let returnUrl = returnUrlRaw || "https://foco-after-dark.web.app";
  if (!/^https?:\/\//i.test(returnUrl)) {
    returnUrl = "https://foco-after-dark.web.app";
  }
  if (!returnUrl.includes("{CHECKOUT_SESSION_ID}")) {
    const glue = returnUrl.includes("?") ? "&" : "?";
    returnUrl = `${returnUrl}${glue}checkout_session_id={CHECKOUT_SESSION_ID}`;
  }

  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  assertStripeAllowed(context, memberDocData);
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError("failed-precondition", "Stripe not configured");

  const customerId = await ensureStripeCustomer({
    stripe,
    memberRef,
    memberDocData,
    uid,
    email,
    token: context.auth.token,
  });

  const environment = String(
    data?.environment
    || process.env.FUNCTIONS_EMULATOR && "emulator"
    || process.env.NODE_ENV
    || process.env.GCLOUD_PROJECT
    || "production"
  ).trim().toLowerCase();

  const session = await stripe.checkout.sessions.create({
    mode: "subscription",
    ui_mode: "embedded",
    customer: customerId,
    line_items: [{ price: priceId, quantity: 1 }],
    allow_promotion_codes: true,
    return_url: returnUrl,
    metadata: {
      uid,
      tier,
      interval,
      environment,
    },
    subscription_data: {
      metadata: {
        uid,
        tier,
        interval,
        environment,
      }
    }
  });

  await memberRef.set({
    requestedTier: tier,
    requestedBillingInterval: interval,
    billingProvider: "stripe",
    stripeCustomerId: customerId,
    lastStripeCheckoutSessionId: session.id,
    lastStripeEvent: "checkout.session.created",
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    checkoutSessionId: session.id,
    clientSecret: session.client_secret || null,
    publishableKey: publishable,
    tier,
    interval,
    priceId,
  };
});

function nextRenewalISOForInterval(interval = "month", fromDate = new Date()) {
  const next = new Date(fromDate);
  if (normalizeBillingIntervalKey(interval) === "year") {
    next.setFullYear(next.getFullYear() + 1);
  } else {
    next.setMonth(next.getMonth() + 1);
  }
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

function computeStripeDiscountPresence(obj = {}) {
  const hasSessionDiscount = Number(obj?.total_details?.amount_discount || 0) > 0;
  const hasInvoiceDiscount = Array.isArray(obj?.total_discount_amounts) && obj.total_discount_amounts.length > 0;
  const hasSubDiscount = !!obj?.discount || (Array.isArray(obj?.discounts) && obj.discounts.length > 0);
  return hasSessionDiscount || hasInvoiceDiscount || hasSubDiscount;
}

function logStripeWebhookAudit(event = {}, obj = {}, memberCtx = null) {
  if (event?.livemode === true) return;
  const metadata = obj?.metadata || {};
  const uid = String(memberCtx?.uid || metadata?.uid || "").trim() || null;
  const subscriptionId = String(
    obj?.subscription
      || obj?.id?.startsWith("sub_") && obj?.id
      || ""
  ).trim() || null;
  const invoiceId = String(
    obj?.invoice
      || obj?.id?.startsWith("in_") && obj?.id
      || ""
  ).trim() || null;
  const checkoutSessionId = String(
    obj?.id?.startsWith("cs_") && obj?.id
      || obj?.checkout_session
      || ""
  ).trim() || null;
  const customerId = String(obj?.customer || "").trim() || null;
  const tier = String(metadata?.tier || memberCtx?.data?.tier || "").trim().toLowerCase() || null;
  const interval = String(metadata?.interval || memberCtx?.data?.billingInterval || "").trim().toLowerCase() || null;
  const promoCode = String(metadata?.promo || "").trim() || null;
  console.info("stripe_webhook_audit_test", {
    eventType: event?.type || null,
    livemode: !!event?.livemode,
    checkoutSessionId,
    subscriptionId,
    invoiceId,
    customerId,
    uid,
    tier,
    interval,
    promoCode,
    discountPresent: computeStripeDiscountPresence(obj),
  });
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
  const billingInterval = getSubscriptionInterval(subscription, memberDocData?.billingInterval || "month");
  const currentPeriodEndIso = isoFromUnix(subscription?.current_period_end);
  const promoTag = String(subscription?.metadata?.promo || memberDocData?.membershipPromoTag || "").toLowerCase();
  const updates = {
    tier,
    membershipTier: membershipTierLabel(tier),
    billingInterval,
    billingProvider: "stripe",
    membershipStatus: subscription?.status || "active",
    paymentStatus: stripeStatusToPaymentStatus(subscription?.status),
    stripeCustomerId: subscription?.customer || memberDocData?.stripeCustomerId || null,
    stripeSubscriptionId: subscription?.id || memberDocData?.stripeSubscriptionId || null,
    membershipPromoTag: promoTag || null,
    membershipTrialDays: admin.firestore.FieldValue.delete(),
    membershipTrialEndsAt: admin.firestore.FieldValue.delete(),
    currentPeriodEnd: currentPeriodEndIso,
    nextRenewal: currentPeriodEndIso,
    cancelAtPeriodEnd: subscription?.cancel_at_period_end === true,
    accountDeleteAt: subscription?.cancel_at_period_end === true
      ? (memberDocData?.accountDeleteAt || null)
      : admin.firestore.FieldValue.delete(),
    accountDeleteReason: subscription?.cancel_at_period_end === true
      ? (memberDocData?.accountDeleteReason || null)
      : admin.firestore.FieldValue.delete(),
    accountDeleteScheduledAt: subscription?.cancel_at_period_end === true
      ? (memberDocData?.accountDeleteScheduledAt || null)
      : admin.firestore.FieldValue.delete(),
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
    const interval = normalizeBillingIntervalKey(intent?.metadata?.interval || memberCtx.data?.billingInterval || "month");
    if (tier) updates.tier = tier;
    updates.membershipTier = membershipTierLabel(tier);
    updates.billingInterval = interval;
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
        updates.nextRenewal = nextRenewalISOForInterval(interval);
      }
    } catch (err) {
      console.warn("Failed to sync subscription from payment intent", err?.message || err);
      updates.nextRenewal = nextRenewalISOForInterval(interval);
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
      logStripeWebhookAudit(event, intent, null);
      await applyStripePaymentIntentUpdate(intent, "succeeded");
    } else if (event.type === "payment_intent.payment_failed") {
      logStripeWebhookAudit(event, intent, null);
      await applyStripePaymentIntentUpdate(intent, "failed");
    } else if (event.type === "payment_intent.canceled") {
      logStripeWebhookAudit(event, intent, null);
      await applyStripePaymentIntentUpdate(intent, "canceled");
    } else if (event.type === "setup_intent.succeeded") {
      logStripeWebhookAudit(event, intent, null);
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
      logStripeWebhookAudit(event, intent, memberCtx);
      await applyStripeSubscriptionUpdate(intent, memberCtx, event.type);
    } else if (event.type === "invoice.payment_succeeded"
      || event.type === "invoice.paid"
      || event.type === "invoice.payment_failed") {
      const memberCtx = await resolveMemberContextFromStripeObject(intent);
      logStripeWebhookAudit(event, intent, memberCtx);
      if (memberCtx?.ref && !isStripeExcluded({}, memberCtx.data)) {
        const paid = event.type === "invoice.payment_succeeded" || event.type === "invoice.paid";
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
      logStripeWebhookAudit(event, intent, memberCtx);
      const purchaseType = String(intent?.metadata?.purchaseType || "").toLowerCase();
      if (purchaseType === "voucher_tokens") {
        const grantResult = await applyVoucherTokenCheckoutGrant(intent, "checkout.session.completed");
        console.info("voucher_token_checkout_grant", {
          sessionId: intent?.id || null,
          applied: grantResult?.applied === true,
          reason: grantResult?.reason || null,
          billingMonthKey: grantResult?.billingMonthKey || null,
          tokenAmount: grantResult?.tokenAmount || null,
          livemode: !!event?.livemode,
        });
      } else if (memberCtx?.ref && isStripeExcluded({}, memberCtx.data)) {
        await memberCtx.ref.set({
          billingProvider: "none",
          membershipStatus: "active",
          paymentStatus: "active",
          stripeCustomerId: admin.firestore.FieldValue.delete(),
          stripeSubscriptionId: admin.firestore.FieldValue.delete(),
          lastStripeEvent: "checkout.session.completed:excluded",
          lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
        }, { merge: true });
      } else if (memberCtx?.ref) {
        const updates = {
          lastStripeEvent: "checkout.session.completed",
          lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
          lastStripeCheckoutSessionId: intent?.id || memberCtx?.data?.lastStripeCheckoutSessionId || null,
        };
        if (intent?.customer) updates.stripeCustomerId = intent.customer;
        await memberCtx.ref.set(updates, { merge: true });
        const subscriptionId = String(intent?.subscription || "").trim();
        if (subscriptionId) {
          const subscription = await retrieveStripeSubscription(stripe, subscriptionId);
          if (subscription) {
            await applyStripeSubscriptionUpdate(subscription, memberCtx, "checkout.session.completed");
          }
        }
      }
    } else if (
      event.type === "identity.verification_session.processing"
      || event.type === "identity.verification_session.verified"
      || event.type === "identity.verification_session.requires_input"
      || event.type === "identity.verification_session.canceled"
    ) {
      await applyIdentityVerificationUpdate(intent, event.type);
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
  await assertBillingEnabledOrThrow();
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

exports.createVoucherTokenCheckoutSession = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  await assertBillingEnabledOrThrow();

  const uid = context.auth.uid;
  const email = (context.auth.token.email || "").toLowerCase();
  const requestedPackId = normalizeVoucherTokenPackId(data?.packId);
  if (!requestedPackId) {
    throw new HttpsError("invalid-argument", "Invalid voucher token pack.");
  }

  const { ref: memberRef, data: memberDocData } = await getMemberContext(uid);
  assertStripeAllowed(context, memberDocData);
  const memberTier = normalizeTierKey(memberDocData?.tier || memberDocData?.membershipTier || "standard");
  if (memberTier !== "standard" && memberTier !== "vip") {
    throw new HttpsError("failed-precondition", "Voucher token purchases require an active Standard or VIP membership.");
  }

  const pack = resolveVoucherTokenPackForTier(memberTier, requestedPackId);
  if (!pack) {
    throw new HttpsError("permission-denied", "This token pack is not available for your membership.");
  }

  const stripe = getStripeClient();
  const customerId = await ensureStripeCustomer({
    stripe,
    memberRef,
    memberDocData,
    uid,
    email,
    token: context.auth.token,
  });

  const billingMonthKey = getCurrentVoucherBillingMonthKeyForWallet(memberDocData, new Date());
  const environment = String(
    data?.environment
    || process.env.FUNCTIONS_EMULATOR && "emulator"
    || process.env.NODE_ENV
    || process.env.GCLOUD_PROJECT
    || "production"
  ).trim().toLowerCase();
  const returnUrlRaw = String(data?.returnUrl || "https://foco-after-dark.web.app").trim();
  const fallbackReturnUrl = "https://foco-after-dark.web.app/";
  const baseReturnUrl = /^https?:\/\//i.test(returnUrlRaw) ? returnUrlRaw : fallbackReturnUrl;
  const separator = baseReturnUrl.includes("?") ? "&" : "?";
  const returnUrl = `${baseReturnUrl}${separator}voucher_checkout=1&voucher_status=success&voucher_session_id={CHECKOUT_SESSION_ID}`;

  const session = await stripe.checkout.sessions.create({
    mode: "payment",
    customer: customerId,
    line_items: [{ price: pack.priceId, quantity: 1 }],
    return_url: returnUrl,
    metadata: {
      uid,
      membershipTier: memberTier,
      packId: String(pack.packId),
      tokenAmount: String(pack.tokenAmount),
      billingMonthKey,
      purchaseType: "voucher_tokens",
      environment,
    },
    payment_intent_data: {
      metadata: {
        uid,
        membershipTier: memberTier,
        packId: String(pack.packId),
        tokenAmount: String(pack.tokenAmount),
        billingMonthKey,
        purchaseType: "voucher_tokens",
        environment,
      },
    },
  });

  await memberRef.set({
    billingProvider: "stripe",
    stripeCustomerId: customerId,
    lastStripeCheckoutSessionId: session.id,
    lastStripeEvent: "voucher.checkout.session.created",
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });

  return {
    checkoutSessionId: session.id,
    url: session.url || null,
    mode: "hosted",
    packId: pack.packId,
    tokenAmount: pack.tokenAmount,
    priceLabel: pack.priceLabel,
    membershipTier: memberTier,
    billingMonthKey,
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
  if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');
  if (isCeoMemberDoc(targetData, targetUid)) {
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
    if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');

    const limit = Math.min(parseInt(data?.limit || "1000", 10) || 1000, 5000);
    const authUsers = [];
    let nextPageToken = undefined;
    while (authUsers.length < limit) {
      const pageSize = Math.min(1000, Math.max(1, limit - authUsers.length));
      const page = await admin.auth().listUsers(pageSize, nextPageToken);
      for (const user of (page.users || [])) {
        const email = (user.email || "").toLowerCase();
        const isAnonymousUser = !email && (!user.providerData || user.providerData.length === 0);
        const isStaffUser = user.uid.startsWith("staff_") || user.customClaims?.staff === true;
        if (isAnonymousUser || isStaffUser) continue;
        authUsers.push(user);
        if (authUsers.length >= limit) break;
      }
      nextPageToken = page.pageToken;
      if (!nextPageToken) break;
    }

    const memberMap = new Map();
    if (authUsers.length) {
      const refs = authUsers.map((u) => db.collection("members").doc(u.uid));
      for (let i = 0; i < refs.length; i += 250) {
        const chunk = refs.slice(i, i + 250);
        const snaps = await db.getAll(...chunk);
        snaps.forEach((snap) => {
          if (snap.exists) memberMap.set(snap.id, snap.data() || {});
        });
      }
    }

    const users = [];
    const counts = { total: 0, tier: {}, gender: {}, status: {}, paused: 0 };
    authUsers.forEach((authUser) => {
      const uid = authUser.uid;
      const d = memberMap.get(uid) || {};
      const override = String(d.membershipOverride || d.override || "").toUpperCase();
      const requestedTier = String(d.requestedTier || "").toLowerCase();
      const membershipStatus = String(d.membershipStatus || "").toLowerCase();
      const ageStatus = String(d.ageVerificationStatus || "").toLowerCase();
      const rawTier = String(d.tier || d.membershipTier || d.membership || d.plan || "").toLowerCase();
      let tier = rawTier;
      if (override === "CEO") tier = "ceo";
      else if (override === "CEO_FREE") tier = "ceo_free";
      else if (
        requestedTier === "explorer"
        || membershipStatus === "explorer"
        || ageStatus.includes("explorer")
      ) tier = "explorer";
      else if (!tier && d.freeMembership === true) tier = "free";
      if (!tier) tier = "standard";
      const gender = String(d.gender || "unspecified").toLowerCase();
      const status = d.paused === true
        ? "paused"
        : String(d.paymentStatus || d.membershipStatus || "active").toLowerCase();
      counts.total += 1;
      counts.tier[tier] = (counts.tier[tier] || 0) + 1;
      counts.gender[gender] = (counts.gender[gender] || 0) + 1;
      counts.status[status] = (counts.status[status] || 0) + 1;
      if (d.paused) counts.paused += 1;
      let createdAt = d.createdAt || d.memberSince || d.joinedAt || authUser.metadata?.creationTime || null;
      if (createdAt && createdAt.toDate) {
        createdAt = createdAt.toDate().toISOString();
      }
      let lastLogin = d.lastLoginAt || d.lastLoginDate || d.lastLogin || authUser.metadata?.lastSignInTime || null;
      if (lastLogin && lastLogin.toDate) {
        lastLogin = lastLogin.toDate().toISOString();
      }
      users.push({
        uid,
        username: d.username || d.displayName || authUser.displayName || null,
        passCode: d.passCode || null,
        requestedTier: d.requestedTier || null,
        membershipStatus: d.membershipStatus || null,
        membershipOverride: override || null,
        freeMembership: d.freeMembership === true || override === "CEO_FREE",
        gender,
        tier,
        paymentStatus: status,
        paused: !!d.paused,
        createdAt,
        lastLogin,
        email: d.email || authUser.email || null,
        birthDate: d.birthDate || d.dob || null,
        signUpSource: d.signUpSource || d.signupSource || "App signup"
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
    if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');

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
exports.getStaffLoginToken = functions.runWith(staffLoginRunConfig).https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    requireAuth: false,
    rateLimit: false
  });
  const expected = readRequiredSecret(process.env.STAFF_GATE_CODE, "STAFF_GATE_CODE").toLowerCase();
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
  if (isBeta) {
    const appSnap = await db.collection('settings').doc('app').get();
    const launched = appSnap.exists ? !!appSnap.data().launched : false;
    if (launched) throw new HttpsError('permission-denied', 'Beta access is disabled in live mode');
    if (passNorm !== expectedPass) {
      throw new HttpsError('permission-denied', 'Invalid passphrase');
    }
  } else {
    if (!STAFF_VENUES[venueId]) throw new HttpsError('not-found', 'Unknown venue');
    if (passNorm !== expectedPass) {
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
    await admin.auth().setCustomUserClaims(uid, { staff: true, venue: venueId, beta: isBeta });
  } catch (err) {
    console.warn("Staff claims update failed:", err?.message || err);
    throw new HttpsError("internal", "Staff login unavailable right now");
  }

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
    return { token, venueId, venueName };
  } catch (err) {
    console.warn("Staff custom token failed:", err?.message || err);
    throw new HttpsError("internal", "Staff login unavailable right now");
  }
});

function isStaffContext(context) {
  const claims = context?.auth?.token || {};
  const uid = String(context?.auth?.uid || "");
  return claims.staff === true || uid.startsWith("staff_");
}

function getStaffVenueFromContext(context) {
  const claims = context?.auth?.token || {};
  const claimVenue = String(claims.venue || "").trim().toLowerCase();
  if (claimVenue) return resolveStaffVenueId(claimVenue) || claimVenue;
  const uid = String(context?.auth?.uid || "");
  if (uid.startsWith("staff_")) {
    const raw = uid.slice("staff_".length).trim().toLowerCase();
    return resolveStaffVenueId(raw) || raw;
  }
  return "";
}

function assertVenueMutationAccess(context, venueIdRaw) {
  if (!context?.auth) throw new HttpsError("unauthenticated", "Auth required");
  const venueId = resolveStaffVenueId(String(venueIdRaw || "").trim().toLowerCase());
  if (!venueId) throw new HttpsError("invalid-argument", "Venue required");
  if (isCeoContext(context)) return venueId;
  if (!isStaffContext(context)) throw new HttpsError("permission-denied", "Staff or CEO required");
  const claimVenue = getStaffVenueFromContext(context);
  if (!claimVenue || claimVenue !== venueId) {
    throw new HttpsError("permission-denied", "Staff can only modify their own venue");
  }
  return venueId;
}

function normalizeStaffNameForWrite(value = "") {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, 48);
}

function normalizeStaffRoleForWrite(value = "") {
  return String(value || "").trim().toLowerCase() === "manager" ? "manager" : "staff";
}

function normalizeManagerPinForWrite(value = "") {
  const digits = String(value || "").replace(/\D/g, "");
  if (!digits) return null;
  if (digits.length !== 4) throw new HttpsError("invalid-argument", "Manager code must be 4 digits.");
  return digits;
}

async function listActiveVenueManagers(venueId = "") {
  const ref = db.collection("venues").doc(venueId).collection("staffMembers");
  const snap = await ref.where("isActive", "==", true).where("role", "==", "manager").get();
  const managers = [];
  snap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const rawPin = String(data.managerPin || "").replace(/\D/g, "");
    managers.push({
      staffId: String(data.staffId || docSnap.id || "").replace(/\D/g, "").padStart(5, "0").slice(-5),
      fullName: String(data.fullName || [data.firstName, data.lastName].filter(Boolean).join(" ") || "Manager").trim(),
      managerPin: rawPin.length === 4 ? rawPin : null
    });
  });
  return managers;
}

async function assertManagerApprovalForVenue(venueId = "", data = {}) {
  const managers = await listActiveVenueManagers(venueId);
  if (!managers.length) {
    return { required: false, approvedByStaffId: null, approvedByStaffName: null };
  }
  const managerStaffId = String(data?.managerStaffId || "").replace(/\D/g, "").padStart(5, "0").slice(-5);
  const managerPin = String(data?.managerPin || "").replace(/\D/g, "");
  if (!managerStaffId || managerPin.length !== 4) {
    throw new HttpsError("permission-denied", "Manager approval (4-digit code) is required.");
  }
  const match = managers.find((item) => item.staffId === managerStaffId);
  if (!match || !match.managerPin || match.managerPin !== managerPin) {
    throw new HttpsError("permission-denied", "Manager approval code is invalid.");
  }
  return {
    required: true,
    approvedByStaffId: match.staffId,
    approvedByStaffName: match.fullName
  };
}

function normalizeStaffActionType(value = "") {
  return String(value || "").toLowerCase().replace(/[^a-z0-9_]/g, "_").slice(0, 48) || "unknown_action";
}

exports.staffUpsertVenuePerks = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, { requireAuth: true, appLockEnforced: true, rateLimit: true, maxPerMin: 180, maxPerDay: 2000 });
    const venueId = assertVenueMutationAccess(context, data?.venueId);
    const managerApproval = await assertManagerApprovalForVenue(venueId, data || {});
    const rawPerks = Array.isArray(data?.perks) ? data.perks : [];
    const normalized = [];
    const labels = new Set();
    for (const item of rawPerks) {
      const id = String(item?.id || "").trim().slice(0, 80);
      const label = String(item?.label || "").trim().slice(0, 80);
      const standardQty = Number(item?.standardQty || 0);
      const vipQty = Number(item?.vipQty || 0);
      if (!id || !label) continue;
      if (!Number.isFinite(standardQty) || !Number.isFinite(vipQty) || standardQty <= 0 || vipQty <= 0) {
        throw new HttpsError("invalid-argument", "Perk quantities must be positive numbers.");
      }
      const dedupeKey = label.toLowerCase();
      if (labels.has(dedupeKey)) throw new HttpsError("invalid-argument", "Perk names must be unique.");
      labels.add(dedupeKey);
      normalized.push({ id, label, standardQty: Math.floor(standardQty), vipQty: Math.floor(vipQty) });
    }

    const perksRef = db.collection("venues").doc(venueId).collection("perks");
    const snap = await perksRef.get();
    const keepIds = new Set(normalized.map((perk) => perk.id));
    const batch = db.batch();
    const venueName = getStaffVenueName(venueId);
    normalized.forEach((perk) => {
      const ref = perksRef.doc(perk.id);
      batch.set(ref, {
        venueId,
        venueName,
        label: perk.label,
        standardQty: perk.standardQty,
        vipQty: perk.vipQty,
        approvedByManagerId: managerApproval.approvedByStaffId || null,
        approvedByManagerName: managerApproval.approvedByStaffName || null,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    });
    snap.forEach((docSnap) => {
      if (!keepIds.has(docSnap.id)) batch.delete(docSnap.ref);
    });
    await batch.commit();
    return { ok: true, venueId, saved: normalized.length };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("staffUpsertVenuePerks failed", err);
    throw new HttpsError("internal", err?.message || "Could not save perks");
  }
});

exports.staffManageVenueMember = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      appLockEnforced: true,
      rateLimit: true,
      maxPerMin: 700,
      maxPerDay: 15000,
      burstAllowance: 220
    });
    const venueId = assertVenueMutationAccess(context, data?.venueId);
    const action = String(data?.action || "").toLowerCase();
    if (action !== "add" && action !== "remove") throw new HttpsError("invalid-argument", "Invalid action");
    const staffId = String(data?.staffId || "").replace(/\D/g, "").padStart(5, "0").slice(-5);
    if (!staffId) throw new HttpsError("invalid-argument", "staffId required");
    const staffRef = db.collection("venues").doc(venueId).collection("staffMembers").doc(staffId);
    if (action === "add") {
      const firstName = normalizeStaffNameForWrite(data?.firstName);
      const lastName = normalizeStaffNameForWrite(data?.lastName);
      const role = normalizeStaffRoleForWrite(data?.role);
      const managerPin = role === "manager" ? normalizeManagerPinForWrite(data?.managerPin) : null;
      if (!firstName || !lastName) throw new HttpsError("invalid-argument", "First and last name required");
      if (role === "manager" && !managerPin) throw new HttpsError("invalid-argument", "Manager 4-digit code required");
      await staffRef.set({
        staffId,
        firstName,
        lastName,
        fullName: `${firstName} ${lastName}`.trim(),
        role,
        managerPin,
        isActive: true,
        removedAt: null,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      return { ok: true, action, staffId };
    }
    await staffRef.set({
      isActive: false,
      removedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return { ok: true, action, staffId };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("staffManageVenueMember failed", err);
    throw new HttpsError("internal", err?.message || "Could not update staff member");
  }
});

exports.staffShiftAction = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      appLockEnforced: true,
      rateLimit: true,
      maxPerMin: 420,
      maxPerDay: 12000,
      burstAllowance: 140
    });
    const venueId = assertVenueMutationAccess(context, data?.venueId);
    const action = String(data?.action || "").toLowerCase();
    const shiftKey = String(data?.shiftKey || "").trim();
    const closeoutAttemptId = String(data?.closeoutAttemptId || "").trim().slice(0, 120) || null;
    const closeoutFallbackUsed = data?.closeoutFallbackUsed === true;
    const closeoutFallbackErrorCode = String(data?.closeoutFallbackErrorCode || "").trim().slice(0, 80) || null;
    const actor = {
      staffId: String(data?.staffId || "").replace(/\D/g, "").padStart(5, "0").slice(-5) || null,
      staffName: normalizeStaffNameForWrite(data?.staffName || "") || null
    };
    if (!shiftKey) throw new HttpsError("invalid-argument", "shiftKey required");
    if (action === "start") {
      await db.collection("venues").doc(venueId).collection("shiftState").doc("current").set({
        shiftKey,
        active: true,
        venueId,
        closeoutStatus: "open",
        closeoutLastError: null,
        closeoutLastAttemptId: null,
        closeoutLastAttemptAt: null,
        startedByStaffId: actor.staffId,
        startedByStaffName: actor.staffName,
        closedAt: null,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        startedAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
      await db.collection("venues").doc(venueId).collection("shiftSessions").doc(shiftKey).set({
        shiftKey,
        active: true,
        venueId,
        closeoutStatus: "open",
        closeoutLastError: null,
        closeoutLastAttemptId: null,
        closeoutLastAttemptAt: null,
        startedByStaffId: actor.staffId,
        startedByStaffName: actor.staffName,
        closedAt: null,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        startedAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
      return { ok: true, action, venueId, shiftKey };
    }
    if (action === "close") {
      const currentRef = db.collection("venues").doc(venueId).collection("shiftState").doc("current");
      const sessionRef = db.collection("venues").doc(venueId).collection("shiftSessions").doc(shiftKey);
      const lockRef = db.collection("venues").doc(venueId).collection("shiftCloseouts").doc(shiftKey);
      const attemptId = closeoutAttemptId || `shift-close-${Date.now()}`;
      const result = await db.runTransaction(async (tx) => {
        const [currentSnap, sessionSnap, lockSnap] = await Promise.all([
          tx.get(currentRef),
          tx.get(sessionRef),
          tx.get(lockRef)
        ]);
        const currentData = currentSnap.exists ? (currentSnap.data() || {}) : {};
        const sessionData = sessionSnap.exists ? (sessionSnap.data() || {}) : {};
        const lockData = lockSnap.exists ? (lockSnap.data() || {}) : {};
        const alreadyClosed = currentData.closeoutStatus === "closed"
          || sessionData.closeoutStatus === "closed"
          || sessionData.active === false
          || lockData.shiftClosed === true;
        if (alreadyClosed) {
          return { ok: true, action, venueId, shiftKey, alreadyClosed: true, idempotent: true };
        }
        tx.set(currentRef, {
          shiftKey,
          venueId,
          closeoutStatus: "closed",
          closeoutLastError: null,
          closeoutLastAttemptId: attemptId,
          closeoutLastAttemptAt: admin.firestore.FieldValue.serverTimestamp(),
          closeoutFallbackUsed,
          closeoutFallbackErrorCode: closeoutFallbackErrorCode || null,
          active: false,
          reportId: data?.reportId || null,
          lastClosedByStaffId: actor.staffId,
          lastClosedByStaffName: actor.staffName,
          lastClosedAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        tx.set(sessionRef, {
          shiftKey,
          venueId,
          closeoutStatus: "closed",
          closeoutLastError: null,
          closeoutLastAttemptId: attemptId,
          closeoutLastAttemptAt: admin.firestore.FieldValue.serverTimestamp(),
          closeoutFallbackUsed,
          closeoutFallbackErrorCode: closeoutFallbackErrorCode || null,
          active: false,
          closedAt: admin.firestore.FieldValue.serverTimestamp(),
          closedByStaffId: actor.staffId,
          closedByStaffName: actor.staffName,
          reportId: data?.reportId || null,
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        return { ok: true, action, venueId, shiftKey, closeoutAttemptId: attemptId, idempotent: false };
      });
      return result;
    }
    throw new HttpsError("invalid-argument", "Unsupported action");
  } catch (err) {
    try {
      const venueId = String(data?.venueId || "").trim().toLowerCase();
      const action = String(data?.action || "").toLowerCase();
      if (action === "close" && venueId) {
        await db.collection("venues").doc(venueId).collection("shiftState").doc("current").set({
          closeoutStatus: "open",
          closeoutLastError: String(err?.message || "closeout_failed").slice(0, 300),
          closeoutLastAttemptAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
      }
    } catch (_) {}
    if (err instanceof HttpsError) throw err;
    console.warn("staffShiftAction failed", err);
    throw new HttpsError("internal", err?.message || "Could not update shift");
  }
});

exports.staffAppendActivityLog = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      appLockEnforced: true,
      rateLimit: true,
      maxPerMin: 700,
      maxPerDay: 15000,
      burstAllowance: 220
    });
    const venueId = assertVenueMutationAccess(context, data?.venueId);
    const actionType = normalizeStaffActionType(data?.actionType);
    const payload = {
      timestamp: admin.firestore.FieldValue.serverTimestamp(),
      actionAt: admin.firestore.FieldValue.serverTimestamp(),
      actionType,
      staffId: String(data?.staffId || "").replace(/\D/g, "").padStart(5, "0").slice(-5) || null,
      staffName: normalizeStaffNameForWrite(data?.staffName || "") || null,
      venueId,
      redemptionId: String(data?.redemptionId || "").slice(0, 64) || null,
      userId: String(data?.userId || "").slice(0, 64) || null,
      userName: normalizeStaffNameForWrite(data?.userName || "") || null,
      perkId: String(data?.perkId || "").slice(0, 80) || null,
      perkTitle: String(data?.perkTitle || "").trim().slice(0, 120) || null,
      reasonCode: String(data?.reasonCode || "").trim().slice(0, 40) || null,
      reasonNote: String(data?.reasonNote || "").trim().slice(0, 240) || null,
      source: String(data?.source || "staff_portal").trim().slice(0, 40),
      createdByUid: context.auth.uid
    };
    const ref = await db.collection("venues").doc(venueId).collection("activityLog").add(payload);
    return { ok: true, id: ref.id };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("staffAppendActivityLog failed", err);
    throw new HttpsError("internal", err?.message || "Could not append activity log");
  }
});

function coerceDateInput(value, fieldName = "date") {
  if (!value) throw new HttpsError("invalid-argument", `${fieldName} required`);
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  if (typeof value?.toDate === "function") {
    const d = value.toDate();
    if (!Number.isNaN(d.getTime())) return d;
  }
  if (typeof value === "number") {
    const d = new Date(value);
    if (!Number.isNaN(d.getTime())) return d;
  }
  const d = new Date(String(value));
  if (Number.isNaN(d.getTime())) throw new HttpsError("invalid-argument", `${fieldName} invalid`);
  return d;
}

exports.staffUpsertVenueOffer = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      appLockEnforced: true,
      rateLimit: true,
      maxPerMin: 420,
      maxPerDay: 12000,
      burstAllowance: 140
    });
    const venueId = assertVenueMutationAccess(context, data?.venueId);
    const managerApproval = await assertManagerApprovalForVenue(venueId, data || {});
    const action = String(data?.action || "upsert").trim().toLowerCase();
    const offerTypeRaw = String(data?.offerType || "alert").trim().toLowerCase();
    const venueName = getStaffVenueName(venueId);
    let collectionName = "alerts";
    if (offerTypeRaw === "deal" || offerTypeRaw === "vip" || offerTypeRaw === "vipdeal") {
      collectionName = "deals";
    } else if (offerTypeRaw === "vipdeals") {
      collectionName = "vipDeals";
    }
    let docId = String(data?.docId || "").trim();
    if (!docId && collectionName !== "alerts") docId = venueId;

    const collectionRef = db.collection(collectionName);
    const docRef = docId ? collectionRef.doc(docId) : collectionRef.doc();

    if (action === "delete") {
      await docRef.delete();
      return { ok: true, action, offerType: offerTypeRaw, id: docRef.id, venueId };
    }

    if (action === "expire") {
      await docRef.set({
        venueId,
        venueName,
        expiresAt: admin.firestore.Timestamp.fromDate(new Date(Date.now() - 1000)),
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });
      return { ok: true, action, offerType: offerTypeRaw, id: docRef.id, venueId };
    }

    if (action !== "upsert") throw new HttpsError("invalid-argument", "Unsupported offer action");

    const title = String(data?.title || "").trim().slice(0, 120);
    const detail = String(data?.detail || "").trim().slice(0, 280);
    const meta = String(data?.meta || "").trim().slice(0, 120);
    const audience = String(data?.audience || "all").toLowerCase() === "vip" ? "vip" : "all";
    if (!title || !detail) throw new HttpsError("invalid-argument", "title and detail are required");
    const expiresDate = coerceDateInput(data?.expiresAt, "expiresAt");

    const payload = {
      venueId,
      venueName,
      title,
      detail,
      meta: meta || (audience === "vip" ? `VIP · ${venueName}` : venueName),
      expiresAt: admin.firestore.Timestamp.fromDate(expiresDate),
      approvedByManagerId: managerApproval.approvedByStaffId || null,
      approvedByManagerName: managerApproval.approvedByStaffName || null,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    if (collectionName === "alerts") {
      payload.audience = audience;
      payload.enabled = true;
      payload.createdAt = admin.firestore.FieldValue.serverTimestamp();
    }
    await docRef.set(payload, { merge: true });
    return { ok: true, action, offerType: offerTypeRaw, id: docRef.id, venueId };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("staffUpsertVenueOffer failed", err);
    throw new HttpsError("internal", err?.message || "Could not update venue offer");
  }
});

exports.staffPersistCloseOutReport = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, { requireAuth: true, appLockEnforced: true, rateLimit: true, maxPerMin: 120, maxPerDay: 1200 });
    const report = data?.report || null;
    if (!report || typeof report !== "object") throw new HttpsError("invalid-argument", "report required");
    const venueId = assertVenueMutationAccess(context, report.venue || report.venueId || data?.venueId);
    const closeoutAttemptId = String(data?.closeoutAttemptId || report?.closeoutAttemptId || "").trim().slice(0, 120) || null;
    const reportId = String(report.id || `close-${venueId}-${Date.now()}`).trim().slice(0, 120);
    const generatedAtDate = coerceDateInput(report.generatedAt || new Date(), "generatedAt");
    const closedAtDate = report.closedAt ? coerceDateInput(report.closedAt, "closedAt") : generatedAtDate;
    const shiftNotesAtDate = report.shiftNotesAt ? coerceDateInput(report.shiftNotesAt, "shiftNotesAt") : null;
    const reportRef = db.collection("closeOutReports").doc(reportId);
    const existing = await reportRef.get();
    if (existing.exists) {
      const existingData = existing.data() || {};
      const existingAttemptId = String(existingData.closeoutAttemptId || "").trim();
      const existingShift = String(existingData.shiftKey || "").trim();
      const incomingShift = String(report.shiftKey || "").trim();
      if ((closeoutAttemptId && existingAttemptId === closeoutAttemptId) || (existingShift && incomingShift && existingShift === incomingShift)) {
        return { ok: true, reportId, venueId, idempotent: true };
      }
    }
    const payload = {
      ...report,
      venue: venueId,
      venueId,
      closeoutAttemptId,
      generatedAt: admin.firestore.Timestamp.fromDate(generatedAtDate),
      closedAt: admin.firestore.Timestamp.fromDate(closedAtDate),
      shiftNotesAt: shiftNotesAtDate ? admin.firestore.Timestamp.fromDate(shiftNotesAtDate) : null,
      source: "staff",
      immutable: true,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    };
    await reportRef.set(payload, { merge: false });
    return { ok: true, reportId, venueId };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("staffPersistCloseOutReport failed", err);
    throw new HttpsError("internal", err?.message || "Could not persist close-out report");
  }
});

exports.staffUpsertShiftCloseoutLock = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, { requireAuth: true, appLockEnforced: true, rateLimit: true, maxPerMin: 120, maxPerDay: 1200 });
    const venueId = assertVenueMutationAccess(context, data?.venueId);
    const shiftKey = String(data?.shiftKey || "").trim();
    if (!shiftKey) throw new HttpsError("invalid-argument", "shiftKey required");
    const closeoutAttemptId = String(data?.closeoutAttemptId || "").trim().slice(0, 120) || null;
    const closeoutFallbackUsed = data?.closeoutFallbackUsed === true;
    const closeoutFallbackErrorCode = String(data?.closeoutFallbackErrorCode || "").trim().slice(0, 80) || null;
    const report = data?.report || {};
    const actor = data?.actor || {};
    const lockRef = db.collection("venues").doc(venueId).collection("shiftCloseouts").doc(shiftKey);
    const existing = await lockRef.get();
    if (existing.exists) {
      const existingData = existing.data() || {};
      if (existingData.shiftClosed === true) {
        const existingAttemptId = String(existingData.closeoutAttemptId || "").trim();
        if (!closeoutAttemptId || !existingAttemptId || existingAttemptId === closeoutAttemptId) {
          return { ok: true, venueId, shiftKey, idempotent: true };
        }
      }
    }
    const payload = {
      shiftKey,
      shiftClosed: true,
      closeoutStatus: "closed",
      closeoutLastError: null,
      closeoutAttemptId,
      closeoutLastAttemptAt: admin.firestore.FieldValue.serverTimestamp(),
      closeoutFallbackUsed,
      closeoutFallbackErrorCode: closeoutFallbackErrorCode || null,
      closedAt: admin.firestore.FieldValue.serverTimestamp(),
      closedByStaffId: String(actor?.staffId || "").replace(/\D/g, "").padStart(5, "0").slice(-5) || null,
      closedByStaffName: normalizeStaffNameForWrite(actor?.staffName || "") || null,
      reportId: report?.id || null,
      venueId,
      shiftStartedAt: report?.shiftStartedAt ? admin.firestore.Timestamp.fromDate(coerceDateInput(report.shiftStartedAt, "shiftStartedAt")) : null,
      shiftStartedByStaffId: report?.shiftStartedByStaffId || null,
      shiftStartedByStaffName: normalizeStaffNameForWrite(report?.shiftStartedByStaffName || "") || null,
      totals: report?.totals || {},
      perks: Array.isArray(report?.perks) ? report.perks : [],
      pendingList: Array.isArray(report?.pendingList) ? report.pendingList : [],
      deniedList: Array.isArray(report?.deniedList) ? report.deniedList : [],
      ledger: Array.isArray(report?.ledger) ? report.ledger : [],
      shiftNotes: String(report?.shiftNotes || "").trim(),
      shiftNotesAt: report?.shiftNotesAt ? admin.firestore.Timestamp.fromDate(coerceDateInput(report.shiftNotesAt, "shiftNotesAt")) : null,
      shiftNotesByStaffId: report?.shiftNotesByStaffId || null,
      alerts: report?.alerts || {},
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    };
    await lockRef.set(payload, { merge: true });
    return { ok: true, venueId, shiftKey };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("staffUpsertShiftCloseoutLock failed", err);
    throw new HttpsError("internal", err?.message || "Could not lock shift closeout");
  }
});

function hasStaffLookupAccess(context) {
  const claims = context?.auth?.token || {};
  const uid = String(context?.auth?.uid || "");
  return (
    claims.staff === true ||
    claims.ceo === true ||
    claims.admin === true ||
    uid.startsWith("staff_") ||
    uid === CEO_UID
  );
}

function normalizeLookupPayload(memberData = {}, uid = "") {
  const passCode = String(memberData.passCode || memberData.passId || "").trim().toUpperCase();
  const username = String(memberData.username || "").trim().replace(/^@+/, "").toLowerCase();
  const displayName = String(
    memberData.displayName ||
    memberData.name ||
    memberData.legalName ||
    memberData.fullName ||
    ""
  ).trim();
  let firstName = String(memberData.firstName || "").trim();
  let lastName = String(memberData.lastName || "").trim();
  if ((!firstName || !lastName) && displayName) {
    const parts = displayName.split(/\s+/).filter(Boolean);
    if (!firstName && parts.length) firstName = parts[0];
    if (!lastName && parts.length > 1) lastName = parts.slice(1).join(" ");
  }
  const override = String(memberData.membershipOverride || memberData.override || "").toUpperCase();
  const rawTier = memberData.tier || memberData.membershipTier || memberData.membership || memberData.plan || null;
  const effectiveTier = override === "CEO_FREE" ? "ceo_free" : rawTier;
  return {
    uid: String(uid || "").trim(),
    passCode,
    username,
    name: displayName,
    displayName,
    firstName,
    lastName,
    tier: effectiveTier,
    membershipTier: memberData.membershipTier || memberData.tier || null,
    membershipOverride: override || null,
    paymentStatus: memberData.paymentStatus || null,
    membershipStatus: memberData.membershipStatus || null,
    paused: memberData.paused === true,
    ceo: memberData.ceo === true || passCode === CEO_PASS_ID,
    freeMembership: memberData.freeMembership === true || override === "CEO_FREE",
    blackCard: memberData.blackCard === true
  };
}

async function buildAuthBackedLookupPayload(memberData = {}, uid = "") {
  const memberUid = String(uid || "").trim();
  if (!memberUid) return null;
  let authUser = null;
  try {
    authUser = await admin.auth().getUser(memberUid);
  } catch (err) {
    if (err?.code === "auth/user-not-found") return null;
    throw err;
  }
  const email = String(authUser?.email || "").toLowerCase();
  const isAnonymousUser = !email && (!authUser?.providerData || authUser.providerData.length === 0);
  const isStaffUser = memberUid.startsWith("staff_") || authUser?.customClaims?.staff === true;
  if (isAnonymousUser || isStaffUser) return null;
  return normalizeLookupPayload({
    ...memberData,
    email: memberData.email || authUser.email || null,
    displayName: memberData.displayName || memberData.name || memberData.legalName || authUser.displayName || "",
  }, memberUid);
}

async function findMemberForStaffLookup(rawQuery = "") {
  const raw = String(rawQuery || "").trim();
  if (!raw) return null;
  const passCandidate = raw.replace(/\s+/g, "").toUpperCase();
  const usernameCandidateRaw = raw.replace(/^@+/, "").trim();
  const usernameCandidate = usernameCandidateRaw.toLowerCase();
  const candidates = Array.from(new Set([passCandidate, raw, raw.toLowerCase()].filter(Boolean)));

  const snapshotToMember = async (snap) => {
    if (!snap || snap.empty) return null;
    const docSnap = snap.docs[0];
    const data = docSnap.data() || {};
    return buildAuthBackedLookupPayload(data, docSnap.id);
  };

  for (const value of candidates) {
    const byPassCode = await db.collection("members").where("passCode", "==", value).limit(1).get();
    const passCodeHit = await snapshotToMember(byPassCode);
    if (passCodeHit) return passCodeHit;

    const byPassId = await db.collection("members").where("passId", "==", value).limit(1).get();
    const passIdHit = await snapshotToMember(byPassId);
    if (passIdHit) return passIdHit;
  }

  if (usernameCandidate) {
    const usernameVariants = Array.from(new Set([
      usernameCandidate,
      usernameCandidateRaw,
      `@${usernameCandidate}`,
      `@${usernameCandidateRaw}`
    ].filter(Boolean)));
    for (const candidate of usernameVariants) {
      const byUsername = await db.collection("members").where("username", "==", candidate).limit(1).get();
      const usernameHit = await snapshotToMember(byUsername);
      if (usernameHit) return usernameHit;
    }

    const usernameDoc = await db.collection("usernames").doc(usernameCandidate).get();
    if (usernameDoc.exists) {
      const usernameData = usernameDoc.data() || {};
      const mappedUid = String(usernameData.uid || "").trim();
      if (mappedUid) {
        const memberSnap = await db.collection("members").doc(mappedUid).get();
        if (memberSnap.exists) {
          return buildAuthBackedLookupPayload(memberSnap.data() || {}, mappedUid);
        }
      }
      const mappedPassCode = String(usernameData.passCode || "").trim().toUpperCase();
      if (mappedPassCode) {
        const byMappedPassCode = await db.collection("members").where("passCode", "==", mappedPassCode).limit(1).get();
        const mappedPassHit = await snapshotToMember(byMappedPassCode);
        if (mappedPassHit) return mappedPassHit;
      }
      const mappedEmail = String(usernameData.email || "").trim().toLowerCase();
      if (mappedEmail) {
        const byMappedEmail = await db.collection("members").where("email", "==", mappedEmail).limit(1).get();
        const mappedEmailHit = await snapshotToMember(byMappedEmail);
        if (mappedEmailHit) return mappedEmailHit;
      }
    }
  }

  if (passCandidate) {
    const passSnap = await db.collection("passes").doc(passCandidate).get();
    if (passSnap.exists) {
      const passData = passSnap.data() || {};
      const uid = String(
        passData.uid ||
        passData.memberUid ||
        passData.userId ||
        passData.ownerUid ||
        ""
      ).trim();
      if (uid) {
        const memberSnap = await db.collection("members").doc(uid).get();
        if (memberSnap.exists) {
          return buildAuthBackedLookupPayload(memberSnap.data() || {}, uid);
        }
      }
    }
  }

  return null;
}

exports.staffLookupMember = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 40, maxPerDay: 1200 }
  });
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  let allowed = hasStaffLookupAccess(context);
  if (!allowed) {
    try {
      const memberSnap = await db.collection("members").doc(context.auth.uid).get();
      const member = memberSnap.exists ? (memberSnap.data() || {}) : {};
      const role = String(member.role || "").toLowerCase();
      allowed = member.staff === true || member.ceo === true || role === "staff" || role === "ceo";
    } catch (_) {}
  }
  if (!allowed) throw new HttpsError("permission-denied", "Staff access required");
  const queryText = String(data?.query || "").trim();
  if (!queryText) throw new HttpsError("invalid-argument", "Pass ID or username required");
  try {
    const member = await findMemberForStaffLookup(queryText);
    if (!member) return { found: false };
    return { found: true, ...member };
  } catch (err) {
    console.warn("staffLookupMember failed", err?.message || err);
    throw err instanceof HttpsError ? err : new HttpsError("internal", "Member lookup failed");
  }
});

function inferQueueScopeFromPath(path = "") {
  const p = String(path || "");
  if (p.includes("/venues/") && p.includes("/redemptions/")) return "venue";
  if (p.includes("/members/") && p.includes("/redemptions/")) return "member";
  return "legacy";
}

exports.backfillRedemptionQueueScope = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 6, maxPerDay: 60 }
  });
  if (!context.auth) throw new HttpsError("unauthenticated", "Auth required");
  const claims = context.auth.token || {};
  if (!(claims.admin === true || claims.ceo === true || String(claims.email || "").toLowerCase() === "ceo@gmail.com")) {
    throw new HttpsError("permission-denied", "CEO/admin access required");
  }

  const days = Math.max(1, Math.min(365, Number(data?.days || 90)));
  const maxDocs = Math.max(100, Math.min(20000, Number(data?.maxDocs || 10000)));
  const cutoff = admin.firestore.Timestamp.fromMillis(Date.now() - (days * 24 * 60 * 60 * 1000));

  const snap = await db
    .collectionGroup("redemptions")
    .where("createdAt", ">=", cutoff)
    .limit(maxDocs)
    .get();

  let scanned = 0;
  let updated = 0;
  let unchanged = 0;
  let venueCount = 0;
  let memberCount = 0;
  let legacyCount = 0;

  let batch = db.batch();
  let batchOps = 0;
  const commitBatch = async () => {
    if (!batchOps) return;
    await batch.commit();
    batch = db.batch();
    batchOps = 0;
  };

  for (const docSnap of snap.docs) {
    scanned += 1;
    const expected = inferQueueScopeFromPath(docSnap.ref.path);
    if (expected === "venue") venueCount += 1;
    else if (expected === "member") memberCount += 1;
    else legacyCount += 1;
    const current = String((docSnap.data() || {}).queueScope || "").toLowerCase();
    if (current === expected) {
      unchanged += 1;
      continue;
    }
    batch.set(docSnap.ref, {
      queueScope: expected,
      queueScopeBackfilledAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    batchOps += 1;
    updated += 1;
    if (batchOps >= 400) {
      await commitBatch();
    }
  }
  await commitBatch();

  return {
    ok: true,
    days,
    scanned,
    updated,
    unchanged,
    byScope: {
      venue: venueCount,
      member: memberCount,
      legacy: legacyCount
    },
    cutoffMs: cutoff.toMillis()
  };
});

// CEO login via shared passphrase: returns custom token with CEO claims.
exports.getCeoLoginToken = functions.runWith(ceoLoginRunConfig).https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: false,
      rateLimit: true,
      publicScope: "ceoLogin",
      publicRateLimit: { limit: 15, windowMs: 10 * 60 * 1000 }
    });
    const expected = readRequiredSecret(process.env.CEO_LOGIN_CODE, "CEO_LOGIN_CODE").toLowerCase();
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
exports.getBetaLoginToken = functions.runWith(betaLoginRunConfig).https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    requireAuth: false,
    rateLimit: true,
    publicScope: "betaLogin",
    publicRateLimit: { limit: 30, windowMs: 10 * 60 * 1000 }
  });
  const expected = readRequiredSecret(process.env.BETA_LOGIN_CODE, "BETA_LOGIN_CODE").toLowerCase();
  const supplied = (data?.code || "").toString().trim().toLowerCase();
  if (!supplied || supplied !== expected) throw new HttpsError('permission-denied', 'Invalid beta access code');

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
  if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');

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
  if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');

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
          extraVoucherBuckets: {},
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
  const ref = db.collection('members').doc(uid);
  const snap = await ref.get();
  const docData = snap.exists ? snap.data() : {};
  const isCeo = isCeoMemberDoc(docData, uid) || isCeoContext(context);
  if (isCeo) throw new HttpsError('failed-precondition', 'CEO account cannot be canceled.');

  const wipe = data?.wipe === true;
  const nowIso = new Date().toISOString();
  let currentPeriodEndIso = docData.currentPeriodEnd || docData.nextRenewal || null;
  let subscriptionPromoTag = String(docData.membershipPromoTag || "").toLowerCase();

  // For paid members, cancel the Stripe subscription at period end (Netflix-style cadence).
  if (!isStripeExcluded(context.auth.token, docData) && docData.stripeSubscriptionId) {
    try {
      const stripe = getStripeClient();
      const sub = await stripe.subscriptions.update(docData.stripeSubscriptionId, {
        cancel_at_period_end: true,
      });
      currentPeriodEndIso = isoFromUnix(sub.current_period_end) || currentPeriodEndIso;
      subscriptionPromoTag = String(sub?.metadata?.promo || subscriptionPromoTag || "").toLowerCase();
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
    membershipPromoTag: subscriptionPromoTag || null,
    currentPeriodEnd: currentPeriodEndIso,
    nextRenewal: currentPeriodEndIso,
    accountDeleteAt: admin.firestore.FieldValue.delete(),
    accountDeleteReason: admin.firestore.FieldValue.delete(),
    accountDeleteScheduledAt: admin.firestore.FieldValue.delete(),
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
  return {
    ok: true,
    canceled: true,
    cancelAtPeriodEnd: true,
    currentPeriodEnd: currentPeriodEndIso,
    trialCancellation: false,
    accountDeleteAt: null,
    wiped: wipe
  };
});

// Launch mode toggle (CEO only) to switch between beta and live UI/flows
exports.getLaunchMode = functions.https.onCall(async (data, context) => {
  try {
    if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
    const requesterSnap = await db.collection('members').doc(context.auth.uid).get();
    const requesterData = requesterSnap.exists ? requesterSnap.data() : {};
    if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');
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
    await enforceCallableSecurity(context, {
      rateLimit: { maxPerMin: 10, maxPerDay: 120 }
    });
    if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');
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

// Maintenance mode toggle (CEO only). When enabled, clients should sign out and show maintenance screen.
exports.setMaintenanceMode = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      rateLimit: { maxPerMin: 10, maxPerDay: 120 }
    });
    if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');

    const appRef = db.collection('settings').doc('app');
    const beforeSnap = await appRef.get();
    const wasEnabled = beforeSnap.exists ? beforeSnap.data()?.maintenanceMode === true : false;
    const enabled = data?.enabled === true;
    const messageRaw = (data?.message || '').toString().trim();
    const message = messageRaw || "FoCo After Dark is currently undergoing maintenance.";
    const updates = {
      maintenanceMode: enabled,
      maintenanceMessage: message,
      maintenanceUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
      maintenanceUpdatedBy: context.auth.uid
    };
    if (!enabled) {
      updates.maintenanceUntil = admin.firestore.FieldValue.delete();
    }
    await appRef.set(updates, { merge: true });

    let push = null;
    if (wasEnabled !== enabled) {
      try {
        push = await sendPushToAll({
          title: enabled ? "FoCo After Dark Maintenance" : "FoCo After Dark",
          body: enabled
            ? "FoCo After Dark is currently undergoing maintenance."
            : "Maintenance complete. FoCo After Dark is back online.",
          link: "/",
          source: enabled ? "maintenance-on" : "maintenance-off",
          dedupeKey: `maintenance:${enabled ? "on" : "off"}`,
          dedupeWindowMs: 60000
        });
      } catch (pushErr) {
        console.warn("setMaintenanceMode push failed", pushErr?.message || pushErr);
      }
    }
    return { ok: true, maintenanceMode: enabled, push };
  } catch (err) {
    console.warn("setMaintenanceMode failed", err);
    throw err instanceof HttpsError ? err : new HttpsError('internal', err?.message || 'Failed to update maintenance mode');
  }
});

exports.adminUpdateConfig = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      rateLimit: { maxPerMin: 80, maxPerDay: 2500, burstAllowance: 20 },
    });
    requireAdminClaim(context);
    const section = String(data?.section || "").trim().toLowerCase();
    const key = String(data?.key || "").trim();
    if (!section || !APP_PUBLIC_CONFIG_SECTIONS.has(section)) {
      throw new HttpsError("invalid-argument", "Invalid config section");
    }
    if (!key || key.length > 120 || !/^[a-zA-Z0-9._-]+$/.test(key)) {
      throw new HttpsError("invalid-argument", "Invalid config key");
    }
    const value = sanitizeConfigSectionValue(section, data?.value);
    const actor = getAdminActorRef(context);
    const patch = {
      [`${section}.${key}`]: value,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedBy: actor.uid,
      version: admin.firestore.FieldValue.increment(1),
    };
    if (actor.email) patch.updatedByEmail = actor.email;
    await db.doc(APP_PUBLIC_CONFIG_DOC_PATH).set(patch, { merge: true });
    return { ok: true, section, key, value };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("adminUpdateConfig failed", err);
    throw new HttpsError("internal", err?.message || "Could not update app config");
  }
});

exports.adminUpdateGlobalConfig = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      rateLimit: { maxPerMin: 40, maxPerDay: 1200, burstAllowance: 10 },
    });
    requireAdminClaim(context);
    const patch = data?.patch;
    if (!patch || typeof patch !== "object" || Array.isArray(patch)) {
      throw new HttpsError("invalid-argument", "patch object required");
    }
    const updates = {};
    if (Object.prototype.hasOwnProperty.call(patch, "waitlistMode")) {
      if (typeof patch.waitlistMode !== "boolean") {
        throw new HttpsError("invalid-argument", "waitlistMode must be boolean");
      }
      updates.waitlistMode = patch.waitlistMode;
    }
    if (Object.prototype.hasOwnProperty.call(patch, "waitlistMessage")) {
      updates.waitlistMessage = String(patch.waitlistMessage || "").trim().slice(0, 400) || "Join the waitlist to get first access.";
    }
    const perfPatch = patch?.performance && typeof patch.performance === "object" && !Array.isArray(patch.performance)
      ? patch.performance
      : null;
    const perfUpdates = {};
    if (perfPatch && Object.prototype.hasOwnProperty.call(perfPatch, "degradeMode")) {
      if (typeof perfPatch.degradeMode !== "boolean") {
        throw new HttpsError("invalid-argument", "performance.degradeMode must be boolean");
      }
      perfUpdates.degradeMode = perfPatch.degradeMode;
    }
    if (perfPatch && Object.prototype.hasOwnProperty.call(perfPatch, "dealsLimit")) {
      const value = Number(perfPatch.dealsLimit);
      if (!Number.isFinite(value)) throw new HttpsError("invalid-argument", "performance.dealsLimit must be a number");
      perfUpdates.dealsLimit = Math.max(5, Math.min(200, Math.round(value)));
    }
    if (perfPatch && Object.prototype.hasOwnProperty.call(perfPatch, "vipDealsLimit")) {
      const value = Number(perfPatch.vipDealsLimit);
      if (!Number.isFinite(value)) throw new HttpsError("invalid-argument", "performance.vipDealsLimit must be a number");
      perfUpdates.vipDealsLimit = Math.max(5, Math.min(200, Math.round(value)));
    }
    if (perfPatch && Object.prototype.hasOwnProperty.call(perfPatch, "alertsLimit")) {
      const value = Number(perfPatch.alertsLimit);
      if (!Number.isFinite(value)) throw new HttpsError("invalid-argument", "performance.alertsLimit must be a number");
      perfUpdates.alertsLimit = Math.max(5, Math.min(300, Math.round(value)));
    }
    if (perfPatch && Object.prototype.hasOwnProperty.call(perfPatch, "pollIntervalMs")) {
      const value = Number(perfPatch.pollIntervalMs);
      if (!Number.isFinite(value)) throw new HttpsError("invalid-argument", "performance.pollIntervalMs must be a number");
      perfUpdates.pollIntervalMs = Math.max(15000, Math.min(300000, Math.round(value)));
    }
    const hasGlobalUpdates = Object.keys(updates).length > 0;
    const hasPerfUpdates = Object.keys(perfUpdates).length > 0;
    if (!hasGlobalUpdates && !hasPerfUpdates) {
      throw new HttpsError("invalid-argument", "No valid global config keys in patch");
    }
    if (hasGlobalUpdates) {
      updates.updatedAt = admin.firestore.FieldValue.serverTimestamp();
      updates.updatedByUid = context.auth.uid;
      await db.doc(GLOBAL_CONFIG_DOC_PATH).set(updates, { merge: true });
    }
    if (hasPerfUpdates) {
      const perfWrite = {
        ...perfUpdates,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedByUid: context.auth.uid
      };
      await db.doc(PERFORMANCE_CONFIG_DOC_PATH).set(perfWrite, { merge: true });
    }
    let waitlistConfig = { waitlistMode: false, waitlistMessage: null };
    try {
      const globalSnap = await db.doc(GLOBAL_CONFIG_DOC_PATH).get();
      if (globalSnap.exists) {
        const globalData = globalSnap.data() || {};
        waitlistConfig = {
          waitlistMode: globalData.waitlistMode === true,
          waitlistMessage: typeof globalData.waitlistMessage === "string" ? globalData.waitlistMessage : null
        };
      }
    } catch (_) {}
    let performanceConfig = { ...PERFORMANCE_CONFIG_DEFAULTS };
    try {
      const perfSnap = await db.doc(PERFORMANCE_CONFIG_DOC_PATH).get();
      if (perfSnap.exists) {
        const perfData = perfSnap.data() || {};
        performanceConfig = {
          degradeMode: perfData.degradeMode === true,
          dealsLimit: Number.isFinite(Number(perfData.dealsLimit)) ? Number(perfData.dealsLimit) : PERFORMANCE_CONFIG_DEFAULTS.dealsLimit,
          vipDealsLimit: Number.isFinite(Number(perfData.vipDealsLimit)) ? Number(perfData.vipDealsLimit) : PERFORMANCE_CONFIG_DEFAULTS.vipDealsLimit,
          alertsLimit: Number.isFinite(Number(perfData.alertsLimit)) ? Number(perfData.alertsLimit) : PERFORMANCE_CONFIG_DEFAULTS.alertsLimit,
          pollIntervalMs: Number.isFinite(Number(perfData.pollIntervalMs)) ? Number(perfData.pollIntervalMs) : PERFORMANCE_CONFIG_DEFAULTS.pollIntervalMs,
        };
      }
    } catch (_) {}
    return {
      ok: true,
      config: {
        waitlistMode: waitlistConfig.waitlistMode === true,
        waitlistMessage: waitlistConfig.waitlistMessage || null,
        updatedByUid: context.auth.uid,
        performance: performanceConfig
      }
    };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("adminUpdateGlobalConfig failed", err);
    throw new HttpsError("internal", err?.message || "Could not update global config");
  }
});

exports.joinWaitlist = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: false,
      rateLimit: false
    });
    // Keep abuse protection but avoid hard daily signup caps.
    await enforcePublicCallableRateLimit(context, "waitlist_join", {
      limit: 1200,
      windowMs: 10 * 60 * 1000
    });
    const firstName = normalizeNameInput(data?.firstName, 60);
    const lastName = normalizeNameInput(data?.lastName, 60);
    const email = normalizeEmailInput(data?.email);
    const consent = data?.consent === true;
    const source = String(data?.source || "app").trim().slice(0, 30) || "app";
    const userAgent = String(data?.userAgent || "").trim().slice(0, 280);
    if (!firstName || !lastName) {
      throw new HttpsError("invalid-argument", "First and last name are required");
    }
    if (!isValidEmailFormat(email)) {
      throw new HttpsError("invalid-argument", "Valid email required");
    }
    if (!consent) {
      throw new HttpsError("failed-precondition", "Consent required");
    }
    const docId = buildWaitlistDocId(email);
    const ref = db.collection(WAITLIST_COLLECTION).doc(docId);
    let existed = false;
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (snap.exists) {
        existed = true;
        return;
      }
      tx.set(ref, {
        firstName,
        lastName,
        email,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        source,
        status: "active",
        consent: true,
        userAgent: userAgent || null
      }, { merge: false });
    });
    return { ok: true, existing: existed, docId };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("joinWaitlist failed", err);
    throw new HttpsError("internal", err?.message || "Could not join waitlist");
  }
});

exports.adminUpdateVenue = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      rateLimit: { maxPerMin: 80, maxPerDay: 2500, burstAllowance: 20 },
    });
    requireAdminClaim(context);
    const venueId = resolveStaffVenueId(String(data?.venueId || "").trim().toLowerCase());
    if (!venueId) throw new HttpsError("invalid-argument", "venueId required");
    const patch = data?.patch;
    if (!patch || typeof patch !== "object" || Array.isArray(patch)) {
      throw new HttpsError("invalid-argument", "patch object required");
    }
    const updates = {};
    Object.keys(patch).forEach((key) => {
      if (!ADMIN_VENUE_MUTABLE_KEYS.has(key)) {
        throw new HttpsError("invalid-argument", `Field '${key}' is not allowed`);
      }
      if (key === "status") {
        updates.status = normalizeVenueStatusValue(patch.status);
        return;
      }
      if (key === "showInHomeFeed") {
        updates.showInHomeFeed = patch.showInHomeFeed !== false;
        return;
      }
      if (key === "showLivePill") {
        updates.showLivePill = patch.showLivePill !== false;
        return;
      }
      if (key === "priority") {
        const nextPriority = Number(patch.priority);
        if (!Number.isFinite(nextPriority)) {
          throw new HttpsError("invalid-argument", "priority must be numeric");
        }
        updates.priority = Math.max(0, Math.min(999, Math.round(nextPriority)));
        return;
      }
      if (key === "adminNote") {
        updates.adminNote = String(patch.adminNote || "").trim().slice(0, 400);
        return;
      }
      if (key === "reason") {
        updates.reason = String(patch.reason || "").trim().slice(0, 180);
      }
    });
    const actor = getAdminActorRef(context);
    updates.updatedAt = admin.firestore.FieldValue.serverTimestamp();
    updates.updatedBy = actor.uid;
    if (actor.email) updates.updatedByEmail = actor.email;
    await db.collection("venues").doc(venueId).set(updates, { merge: true });
    return { ok: true, venueId, updates };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("adminUpdateVenue failed", err);
    throw new HttpsError("internal", err?.message || "Could not update venue");
  }
});

exports.adminCreateVenue = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      rateLimit: { maxPerMin: 30, maxPerDay: 400, burstAllowance: 8 },
    });
    requireAdminClaim(context);
    const venue = data?.venue;
    if (!venue || typeof venue !== "object" || Array.isArray(venue)) {
      throw new HttpsError("invalid-argument", "venue object required");
    }
    const name = String(venue.name || "").trim().slice(0, 120);
    const address = String(venue.address || "").trim().slice(0, 220);
    const hours = String(venue.hours || "").trim().slice(0, 180);
    if (!name || !address) {
      throw new HttpsError("invalid-argument", "venue.name and venue.address are required");
    }
    const sourceId = String(venue.id || venue.venueId || "").trim().toLowerCase();
    const generatedId = sourceId
      || name
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "");
    const venueId = resolveStaffVenueId(generatedId).replace(/[^a-z0-9_]/g, "").slice(0, 60);
    if (!venueId) throw new HttpsError("invalid-argument", "Could not derive venueId");
    const ref = db.collection("venues").doc(venueId);
    const existing = await ref.get();
    if (existing.exists) throw new HttpsError("already-exists", "Venue already exists");
    const actor = getAdminActorRef(context);
    const payload = {
      venueId,
      name,
      address,
      hours: hours || null,
      status: VENUE_SOFT_CONTROL_DEFAULTS.status,
      showInHomeFeed: VENUE_SOFT_CONTROL_DEFAULTS.showInHomeFeed,
      showLivePill: VENUE_SOFT_CONTROL_DEFAULTS.showLivePill,
      priority: VENUE_SOFT_CONTROL_DEFAULTS.priority,
      adminNote: String(venue.adminNote || "").trim().slice(0, 400) || null,
      reason: String(venue.reason || "").trim().slice(0, 180) || null,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdBy: actor.uid,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedBy: actor.uid,
    };
    if (actor.email) {
      payload.createdByEmail = actor.email;
      payload.updatedByEmail = actor.email;
    }
    await ref.set(payload, { merge: false });
    return { ok: true, venueId, venue: payload };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("adminCreateVenue failed", err);
    throw new HttpsError("internal", err?.message || "Could not create venue");
  }
});

exports.adminRestoreVenue = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      rateLimit: { maxPerMin: 60, maxPerDay: 1200, burstAllowance: 12 },
    });
    requireAdminClaim(context);
    const venueId = resolveStaffVenueId(String(data?.venueId || "").trim().toLowerCase());
    if (!venueId) throw new HttpsError("invalid-argument", "venueId required");
    const actor = getAdminActorRef(context);
    const updates = {
      status: "active",
      showInHomeFeed: true,
      showLivePill: true,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedBy: actor.uid,
    };
    if (actor.email) updates.updatedByEmail = actor.email;
    if (data?.reason !== undefined) {
      updates.reason = String(data.reason || "").trim().slice(0, 180) || null;
    }
    if (data?.adminNote !== undefined) {
      updates.adminNote = String(data.adminNote || "").trim().slice(0, 400) || null;
    }
    await db.collection("venues").doc(venueId).set(updates, { merge: true });
    return { ok: true, venueId, status: "active" };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("adminRestoreVenue failed", err);
    throw new HttpsError("internal", err?.message || "Could not restore venue");
  }
});

exports.adminArchiveVenue = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      rateLimit: { maxPerMin: 60, maxPerDay: 1200, burstAllowance: 12 },
    });
    requireAdminClaim(context);
    const venueId = resolveStaffVenueId(String(data?.venueId || "").trim().toLowerCase());
    if (!venueId) throw new HttpsError("invalid-argument", "venueId required");
    const actor = getAdminActorRef(context);
    const updates = {
      status: "archived",
      showInHomeFeed: false,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedBy: actor.uid,
    };
    if (actor.email) updates.updatedByEmail = actor.email;
    if (data?.reason !== undefined) {
      updates.reason = String(data.reason || "").trim().slice(0, 180) || null;
    }
    if (data?.adminNote !== undefined) {
      updates.adminNote = String(data.adminNote || "").trim().slice(0, 400) || null;
    }
    await db.collection("venues").doc(venueId).set(updates, { merge: true });
    return { ok: true, venueId, status: "archived" };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("adminArchiveVenue failed", err);
    throw new HttpsError("internal", err?.message || "Could not archive venue");
  }
});

exports.adminListVenueManagers = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      rateLimit: { maxPerMin: 120, maxPerDay: 5000, burstAllowance: 40 },
    });
    requireAdminClaim(context);
    const venueId = resolveStaffVenueId(String(data?.venueId || "").trim().toLowerCase());
    if (!venueId) throw new HttpsError("invalid-argument", "venueId required");
    const managers = await listActiveVenueManagers(venueId);
    return {
      ok: true,
      venueId,
      managers: managers.map((item) => ({
        staffId: item.staffId,
        fullName: item.fullName,
        managerPin: item.managerPin || null
      }))
    };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("adminListVenueManagers failed", err);
    throw new HttpsError("internal", err?.message || "Could not load venue managers");
  }
});

exports.sendWaitlistBlast = functions.https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: true,
      rateLimit: { maxPerMin: 30, maxPerDay: 5000, burstAllowance: 10 }
    });
    requireAdminClaim(context);
    const subject = String(data?.subject || "").trim().slice(0, 160);
    const html = String(data?.html || "").trim().slice(0, 40000);
    const previewText = String(data?.previewText || "").trim().slice(0, 300);
    const dryRun = data?.dryRun === true;
    if (!subject || !html) {
      throw new HttpsError("invalid-argument", "subject and html are required");
    }
    const ref = db.collection(WAITLIST_MAIL_QUEUE_COLLECTION).doc();
    await ref.set({
      type: "waitlist_blast",
      subject,
      html,
      previewText: previewText || null,
      dryRun,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      createdByUid: context.auth.uid,
      createdByEmail: normalizeEmailInput(context.auth.token?.email || ""),
      status: "queued",
      stats: { attempted: 0, sent: 0, failed: 0 },
      error: null
    }, { merge: false });
    return { ok: true, jobId: ref.id };
  } catch (err) {
    if (err instanceof HttpsError) throw err;
    console.warn("sendWaitlistBlast failed", err);
    throw new HttpsError("internal", err?.message || "Could not queue waitlist blast");
  }
});

exports.waitlistOnCreateCounterIncrement = functions.firestore.document("waitlist/{entryId}").onCreate(async (snap) => {
  const data = snap.data() || {};
  const status = String(data.status || "active").toLowerCase();
  if (!WAITLIST_STATUSES.has(status)) {
    return null;
  }
  const counterRef = db.doc(WAITLIST_COUNTER_DOC_PATH);
  await counterRef.set({
    total: admin.firestore.FieldValue.increment(1),
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return null;
});

exports.processWaitlistBlastJob = functions.runWith(reportEmailSecrets).firestore.document("mailQueue/{jobId}").onCreate(async (snap, context) => {
  const jobId = context.params.jobId;
  const data = snap.data() || {};
  if (String(data.type || "") !== "waitlist_blast") return null;
  const jobRef = snap.ref;
  const subject = String(data.subject || "").trim().slice(0, 160);
  const html = String(data.html || "").trim().slice(0, 40000);
  if (!subject || !html) {
    await jobRef.set({
      status: "error",
      error: "Missing subject/html",
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return null;
  }
  try {
    await jobRef.set({
      status: "running",
      error: null,
      startedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    const dryRun = data.dryRun === true;
    const previewText = String(data.previewText || "").trim().slice(0, 300);
    const actorEmail = normalizeEmailInput(data.createdByEmail || "");
    let recipients = [];
    if (dryRun) {
      recipients = [WAITLIST_DRY_RUN_EMAIL || actorEmail || CEO_EMAIL].filter(Boolean);
    } else {
      const waitSnap = await db.collection(WAITLIST_COLLECTION).where("status", "==", "active").get();
      recipients = waitSnap.docs
        .map((docSnap) => {
          const row = docSnap.data() || {};
          return {
            docId: docSnap.id,
            email: normalizeEmailInput(row.email || "")
          };
        })
        .filter((item) => !!item.email);
    }
    let attempted = 0;
    let sent = 0;
    let failed = 0;
    const failedRows = [];
    const emailedDocIds = [];
    const waitMs = 80; // ~12.5/sec burst cap
    const textBody = previewText || htmlToText(html) || "FoCo After Dark update";
    for (const recipient of recipients) {
      attempted += 1;
      const targetEmail = typeof recipient === "string" ? recipient : recipient.email;
      const res = await queueOrSendEmail({
        to: targetEmail,
        subject,
        html,
        text: textBody
      });
      if (res?.sent) {
        sent += 1;
        if (!dryRun && recipient?.docId) emailedDocIds.push(recipient.docId);
      } else {
        failed += 1;
        failedRows.push({ email: targetEmail, error: res?.error || "send failed" });
      }
      if (attempted % 20 === 0) {
        await jobRef.set({
          stats: { attempted, sent, failed },
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
      }
      await sleep(waitMs);
    }
    if (!dryRun && emailedDocIds.length) {
      while (emailedDocIds.length) {
        const chunk = emailedDocIds.splice(0, 400);
        const batch = db.batch();
        chunk.forEach((docId) => {
          batch.set(db.collection(WAITLIST_COLLECTION).doc(docId), {
            status: "emailed",
            emailedAt: admin.firestore.FieldValue.serverTimestamp()
          }, { merge: true });
        });
        await batch.commit();
      }
    }
    await jobRef.set({
      status: "done",
      stats: { attempted, sent, failed },
      error: failedRows.length ? JSON.stringify(failedRows.slice(0, 20)) : null,
      finishedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    console.info("processWaitlistBlastJob complete", { jobId, attempted, sent, failed, dryRun });
    return null;
  } catch (err) {
    console.warn("processWaitlistBlastJob failed", { jobId, err: err?.message || err });
    await jobRef.set({
      status: "error",
      error: err?.message || "waitlist blast failed",
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return null;
  }
});

// App lock toggle (CEO only) with passcode stored in Secret Manager
exports.setAppLock = functions.runWith(appLockSecrets).https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      rateLimit: { maxPerMin: 8, maxPerDay: 80 }
    });
    if (!isCeoContext(context)) throw new HttpsError('permission-denied', 'CEO only');
    const secret = (process.env.APP_LOCK_CODE || '').trim();
    if (!secret) throw new HttpsError('failed-precondition', 'App lock code not configured');
    const code = (data?.code || '').toString().trim();
    if (!code || code !== secret) throw new HttpsError('permission-denied', 'Invalid lock code');
    const appLocked = data?.locked === true;
    const appRef = db.collection('settings').doc('app');
    const beforeSnap = await appRef.get();
    const wasLocked = beforeSnap.exists ? beforeSnap.data()?.appLocked === true : false;

    await appRef.set({
      appLocked,
      appLockedAt: admin.firestore.FieldValue.serverTimestamp(),
      appLockedBy: context.auth.uid
    }, { merge: true });

    let push = null;
    if (wasLocked !== appLocked) {
      const enteringMaintenance = appLocked === true;
      try {
        push = await sendPushToAll({
          title: enteringMaintenance ? "FoCo After Dark Maintenance" : "FoCo After Dark",
          body: enteringMaintenance
            ? "FoCo After Dark is currently undergoing maintenance."
            : "Maintenance complete. FoCo After Dark is back online.",
          link: "/",
          source: enteringMaintenance ? "maintenance-on" : "maintenance-off",
          dedupeKey: `maintenance:${enteringMaintenance ? "on" : "off"}`,
          dedupeWindowMs: 60000
        });
      } catch (pushErr) {
        console.warn("setAppLock push failed", pushErr?.message || pushErr);
      }
    }

    return { ok: true, appLocked, push };
  } catch (err) {
    console.warn("setAppLock failed", err);
    throw err instanceof HttpsError ? err : new HttpsError('internal', err?.message || 'Failed to update app lock');
  }
});

// CEO access gate while app is locked (password stored in Secret Manager)
exports.verifyCeoAccess = functions.runWith(ceoAccessSecrets).https.onCall(async (data, context) => {
  try {
    await enforceCallableSecurity(context, {
      requireAuth: false,
      rateLimit: true,
      publicScope: "verifyCeoAccess",
      publicRateLimit: { limit: 20, windowMs: 10 * 60 * 1000 }
    });
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
  const baseQuery = db.collection('members')
    .where('stripeSubscriptionId', '!=', null)
    .orderBy('stripeSubscriptionId');
  const pageSize = 200;
  let cursor = null;
  let pageSnap = await baseQuery.limit(pageSize).get();
  while (!pageSnap.empty) {
    for (const docSnap of pageSnap.docs) {
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
    if (pageSnap.size < pageSize) break;
    cursor = pageSnap.docs[pageSnap.docs.length - 1];
    pageSnap = await baseQuery.startAfter(cursor).limit(pageSize).get();
  }
  return null;
});

exports.purgeExpiredCanceledAccounts = functions.pubsub
  .schedule("every 60 minutes")
  .timeZone("America/Denver")
  .onRun(async () => {
    const nowTs = admin.firestore.Timestamp.now();
    let processed = 0;
    let deletedAuth = 0;
    let deletedFirestoreOnly = 0;
    let skipped = 0;
    while (true) {
      const snap = await db.collection("members")
        .where("accountDeleteAt", "<=", nowTs)
        .limit(200)
        .get();
      if (snap.empty) break;
      for (const docSnap of snap.docs) {
        processed += 1;
        const uid = docSnap.id;
        const data = docSnap.data() || {};
        if (isCeoMemberDoc(data, uid)) {
          skipped += 1;
          continue;
        }
        try {
          await admin.auth().deleteUser(uid);
          deletedAuth += 1;
        } catch (err) {
          const code = String(err?.code || "");
          if (code === "auth/user-not-found") {
            await purgeMemberFirestoreRecords(uid, data);
            deletedFirestoreOnly += 1;
          } else {
            console.warn("purgeExpiredCanceledAccounts deleteUser failed", uid, err?.message || err);
          }
        }
      }
      if (snap.size < 200) break;
    }
    console.log("purgeExpiredCanceledAccounts", { processed, deletedAuth, deletedFirestoreOnly, skipped });
    return null;
  });

const CEO_ASSISTANT_CONTEXT_DOC = "settings/ceoAssistantContext";
const CEO_ASSISTANT_CACHE_COLLECTION = "ceoAssistantCache";
function clampAssistantText(value, max = 3200) {
  const normalized = String(value || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  return normalized.slice(0, max);
}
function sanitizeAssistantContextPayload(raw = {}) {
  const obj = raw && typeof raw === "object" ? raw : {};
  const htmlSnippetsRaw = (obj.htmlSnippets && typeof obj.htmlSnippets === "object") ? obj.htmlSnippets : {};
  const scriptSnippetsRaw = (obj.scriptSnippets && typeof obj.scriptSnippets === "object") ? obj.scriptSnippets : {};
  const htmlSnippets = {};
  const scriptSnippets = {};
  Object.entries(htmlSnippetsRaw).slice(0, 12).forEach(([key, value]) => {
    htmlSnippets[String(key || "").slice(0, 48)] = clampAssistantText(value, 900);
  });
  Object.entries(scriptSnippetsRaw).slice(0, 16).forEach(([key, value]) => {
    scriptSnippets[String(key || "").slice(0, 48)] = clampAssistantText(value, 1000);
  });
  const coreCollections = Array.isArray(obj.coreCollections)
    ? obj.coreCollections.map((v) => String(v || "").trim()).filter(Boolean).slice(0, 40)
    : [];
  return {
    buildVersion: clampAssistantText(obj.buildVersion, 60) || "unknown",
    capturedAt: clampAssistantText(obj.capturedAt, 80),
    contextNotes: clampAssistantText(obj.contextNotes, 1200),
    firestoreRulesHint: clampAssistantText(obj.firestoreRulesHint, 700),
    coreCollections,
    htmlSnippets,
    scriptSnippets
  };
}
function parsePromptKeywords(prompt = "") {
  const stopWords = new Set([
    "the", "and", "for", "with", "from", "that", "this", "have", "what", "when", "where", "which", "your", "into", "about", "need", "please", "make", "just", "then"
  ]);
  const tokens = String(prompt || "")
    .toLowerCase()
    .split(/[^a-z0-9_]+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 4 && !stopWords.has(t));
  return Array.from(new Set(tokens)).slice(0, 10);
}
function rankSnippetEntries(entries = [], keywords = []) {
  if (!keywords.length) return entries;
  return entries
    .map(([key, value]) => {
      const hay = `${String(key || "").toLowerCase()} ${String(value || "").toLowerCase()}`;
      let score = 0;
      keywords.forEach((kw) => {
        if (hay.includes(kw)) score += 1;
      });
      return { key, value, score };
    })
    .sort((a, b) => b.score - a.score)
    .map((entry) => [entry.key, entry.value]);
}
function formatAssistantContextForPrompt(contextData = {}, prompt = "") {
  const keywords = parsePromptKeywords(prompt);
  const parts = [];
  if (contextData.buildVersion) parts.push(`Build: ${contextData.buildVersion}`);
  if (Array.isArray(contextData.coreCollections) && contextData.coreCollections.length) {
    parts.push(`Collections: ${contextData.coreCollections.join(", ")}`);
  }
  if (contextData.contextNotes) parts.push(`Notes: ${contextData.contextNotes}`);
  if (contextData.firestoreRulesHint) parts.push(`Rules: ${contextData.firestoreRulesHint}`);
  const htmlSnippets = contextData.htmlSnippets || {};
  rankSnippetEntries(Object.entries(htmlSnippets), keywords).slice(0, 3).forEach(([key, value]) => {
    if (!value) return;
    parts.push(`HTML[${key}]: ${clampAssistantText(value, 360)}`);
  });
  const scriptSnippets = contextData.scriptSnippets || {};
  rankSnippetEntries(Object.entries(scriptSnippets), keywords).slice(0, 4).forEach(([key, value]) => {
    if (!value) return;
    parts.push(`JS[${key}]: ${clampAssistantText(value, 420)}`);
  });
  return clampAssistantText(parts.join("\n"), 3600);
}
async function getAssistantLiveSnapshot() {
  const snapshot = {
    members: null,
    activeAlerts: null,
    activeDeals: null,
    pendingMessages: null,
    pendingRedemptions: null,
    maintenance: null,
    launchMode: null
  };
  const nowMs = Date.now();
  try {
    const membersSnap = await db.collection("members")
      .select("passCode", "paused", "revoked", "paymentStatus", "membershipStatus")
      .limit(5000)
      .get();
    let members = 0;
    membersSnap.forEach((docSnap) => {
      const data = docSnap.data() || {};
      const passCode = String(data.passCode || "").trim();
      if (!passCode) return;
      if (data.paused === true || data.revoked === true) return;
      const status = String(data.paymentStatus || data.membershipStatus || "active").toLowerCase();
      if (["canceled", "cancelled", "inactive", "deleted"].includes(status)) return;
      members += 1;
    });
    snapshot.members = members;
  } catch (_) {}
  try {
    const alertSnap = await db.collection("alerts").limit(1500).get();
    let activeAlerts = 0;
    alertSnap.forEach((docSnap) => {
      const data = docSnap.data() || {};
      if (data.deleted === true || data.archived === true || data.active === false) return;
      const expiresMs = toMillisSafe(data.expiresAt);
      if (expiresMs && expiresMs <= nowMs) return;
      activeAlerts += 1;
    });
    snapshot.activeAlerts = activeAlerts;
  } catch (_) {}
  try {
    const dealsSnap = await db.collection("deals").limit(1500).get();
    let activeDeals = 0;
    dealsSnap.forEach((docSnap) => {
      const data = docSnap.data() || {};
      const expiresMs = toMillisSafe(data.expiresAt);
      if (expiresMs && expiresMs <= nowMs) return;
      const standardQty = Number(data.standardQty || 0);
      const vipQty = Number(data.vipQty || 0);
      const totalQty = Number(data.quantity || 0);
      const hasInventory = standardQty > 0 || vipQty > 0 || totalQty > 0;
      const hasContent = !!String(data.title || data.detail || data.meta || "").trim();
      if (!hasInventory || !hasContent) return;
      activeDeals += 1;
    });
    snapshot.activeDeals = activeDeals;
  } catch (_) {}
  try {
    const msgSnap = await db.collection("staffMessages")
      .where("readByCeo", "==", false)
      .limit(1500)
      .get();
    let unread = 0;
    msgSnap.forEach((docSnap) => {
      const data = docSnap.data() || {};
      if (data.deletedByCeo === true || data.deletedByStaff === true) return;
      if (data.fromCeo === true) return;
      const hasContent = !!String(data.message || data.reply || "").trim();
      if (!hasContent) return;
      unread += 1;
    });
    snapshot.pendingMessages = unread;
  } catch (_) {}
  try {
    const redSnap = await db.collection("redemptions")
      .where("status", "==", "pending")
      .limit(1500)
      .get();
    let pending = 0;
    redSnap.forEach((docSnap) => {
      const data = docSnap.data() || {};
      const expiresMs = toMillisSafe(data.expiresAt);
      if (expiresMs && expiresMs <= nowMs) return;
      pending += 1;
    });
    snapshot.pendingRedemptions = pending;
  } catch (_) {}
  try {
    const appSnap = await db.doc("settings/app").get();
    const app = appSnap.exists ? (appSnap.data() || {}) : {};
    snapshot.maintenance = app.maintenanceMode === true;
    snapshot.launchMode = app.launched === true ? "live" : "beta";
  } catch (_) {}
  return snapshot;
}
function formatAssistantLiveSnapshot(snapshot = {}) {
  return [
    `Live members: ${snapshot.members ?? "n/a"}`,
    `Active alerts: ${snapshot.activeAlerts ?? "n/a"}`,
    `Active deals: ${snapshot.activeDeals ?? "n/a"}`,
    `Unread venue messages for CEO: ${snapshot.pendingMessages ?? "n/a"}`,
    `Pending redemptions: ${snapshot.pendingRedemptions ?? "n/a"}`,
    `Launch mode: ${snapshot.launchMode || "n/a"}`,
    `Maintenance mode: ${snapshot.maintenance === null ? "n/a" : (snapshot.maintenance ? "on" : "off")}`
  ].join("\n");
}
function buildAssistantFallbackReply(prompt = "", liveSnapshot = {}) {
  const lower = String(prompt || "").toLowerCase();
  if (lower.includes("message")) {
    return `Message center is live. Unread venue messages: ${liveSnapshot.pendingMessages ?? "n/a"}. Use staffMessages for reads/replies and deletedBy* flags for cleanup.`;
  }
  if (lower.includes("redeem") || lower.includes("verify")) {
    return `Redemptions are live. Pending redemptions: ${liveSnapshot.pendingRedemptions ?? "n/a"}. Validate createRedemption -> verifyRedemption path and staff venue matching first.`;
  }
  if (lower.includes("maintenance")) {
    return `Maintenance mode is currently ${liveSnapshot.maintenance ? "ON" : "OFF"}. Launch mode is ${liveSnapshot.launchMode || "unknown"}.`;
  }
  return `HQ AI is rate-limited right now. Live snapshot: members ${liveSnapshot.members ?? "n/a"}, alerts ${liveSnapshot.activeAlerts ?? "n/a"}, deals ${liveSnapshot.activeDeals ?? "n/a"}, unread messages ${liveSnapshot.pendingMessages ?? "n/a"}.`;
}
function parseOpenAiErrorBody(rawBody = "") {
  const text = String(rawBody || "");
  if (!text) return { code: "", message: "" };
  try {
    const parsed = JSON.parse(text);
    const err = parsed?.error || {};
    return {
      code: String(err.code || err.type || "").trim(),
      message: clampAssistantText(err.message || "", 360)
    };
  } catch (_) {
    return {
      code: "",
      message: clampAssistantText(text, 360)
    };
  }
}
function buildAssistantOpenAiFailureReply(status = 0, code = "", message = "", liveSnapshot = {}) {
  const normalizedCode = String(code || "").toLowerCase();
  const normalizedMessage = String(message || "").toLowerCase();
  const quotaLike = status === 429
    || normalizedCode.includes("quota")
    || normalizedCode.includes("rate")
    || normalizedMessage.includes("quota")
    || normalizedMessage.includes("billing")
    || normalizedMessage.includes("rate limit");
  if (quotaLike) {
    return `HQ AI is temporarily unavailable because the OpenAI API key is currently quota/rate limited. Live snapshot: members ${liveSnapshot.members ?? "n/a"}, alerts ${liveSnapshot.activeAlerts ?? "n/a"}, deals ${liveSnapshot.activeDeals ?? "n/a"}, unread messages ${liveSnapshot.pendingMessages ?? "n/a"}.`;
  }
  if (status === 401 || normalizedCode.includes("invalid_api_key")) {
    return "HQ AI could not authenticate with OpenAI. Update OPENAI_API_KEY in Firebase Functions secrets.";
  }
  return buildAssistantFallbackReply("", liveSnapshot);
}
async function requestOpenAiWithRetry(apiKey, payload) {
  const retries = [0, 800, 1700];
  let lastStatus = 0;
  let lastBody = "";
  for (let i = 0; i < retries.length; i += 1) {
    if (retries[i] > 0) {
      await new Promise((resolve) => setTimeout(resolve, retries[i]));
    }
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });
    const txt = await resp.text();
    if (resp.ok) {
      let json = {};
      try { json = JSON.parse(txt); } catch (_) {}
      const reply = json?.choices?.[0]?.message?.content || "";
      if (reply) return { ok: true, status: resp.status, reply };
      return { ok: true, status: resp.status, reply: "No reply." };
    }
    lastStatus = resp.status;
    lastBody = txt;
    if (![429, 500, 502, 503, 504].includes(resp.status)) {
      break;
    }
  }
  return { ok: false, status: lastStatus, body: lastBody };
}
async function requestOpenAiWithModelFallback(apiKey, basePayload, models = []) {
  const candidates = Array.isArray(models) && models.length
    ? models
    : ["gpt-4.1-mini", "gpt-4o-mini", "gpt-4.1-nano"];
  let last = { ok: false, status: 0, body: "" };
  for (const model of candidates) {
    const result = await requestOpenAiWithRetry(apiKey, { ...basePayload, model });
    if (result.ok) return { ...result, model };
    last = { ...result, model };
    const status = Number(result.status || 0);
    const body = String(result.body || "").toLowerCase();
    const modelError = status === 404 || (status === 400 && body.includes("model"));
    const transient = [429, 500, 502, 503, 504].includes(status);
    if (!modelError && !transient) break;
  }
  return last;
}

exports.syncCeoAssistantContext = functions.https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 8, maxPerDay: 200 }
  });
  if (!context?.auth) throw new HttpsError("unauthenticated", "Auth required");
  const uid = context.auth.uid;
  let isCeo = isCeoContext(context);
  if (!isCeo && uid) {
    try {
      const memberSnap = await db.collection("members").doc(uid).get();
      isCeo = isCeoMemberDoc(memberSnap.exists ? (memberSnap.data() || {}) : {}, uid);
    } catch (_) {}
  }
  if (!isCeo) throw new HttpsError("permission-denied", "CEO only");
  const contextPayload = sanitizeAssistantContextPayload(data?.context || {});
  await db.doc(CEO_ASSISTANT_CONTEXT_DOC).set({
    ...contextPayload,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedBy: uid,
    updatedByEmail: (context.auth.token?.email || "").toLowerCase()
  }, { merge: true });
  return { ok: true, buildVersion: contextPayload.buildVersion || "unknown" };
});

exports.ceoChat = functions.runWith({ secrets: ["OPENAI_API_KEY"] }).https.onCall(async (data, context) => {
  await enforceCallableSecurity(context, {
    rateLimit: { maxPerMin: 30, maxPerDay: 1000 }
  });
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const uid = context.auth.uid;
  let isCeo = isCeoContext(context);
  if (!isCeo && uid) {
    try {
      const snap = await db.collection('members').doc(uid).get();
      const memberData = snap.exists ? snap.data() : {};
      if (isCeoMemberDoc(memberData, uid)) {
        isCeo = true;
      }
    } catch (_) {}
  }
  if (!isCeo) throw new HttpsError('permission-denied', 'CEO only');
  const prompt = (data?.prompt || '').toString().trim();
  if (!prompt) throw new HttpsError('invalid-argument', 'Prompt required');
  const safePrompt = clampAssistantText(prompt, 1400);
  const normalizedPrompt = safePrompt.toLowerCase().slice(0, 1200);
  let contextData = {};
  try {
    const contextSnap = await db.doc(CEO_ASSISTANT_CONTEXT_DOC).get();
    contextData = contextSnap.exists ? (contextSnap.data() || {}) : {};
  } catch (_) {}
  const liveSnapshot = await getAssistantLiveSnapshot();
  const staticContext = [
    "FoCo After Dark app context:",
    "- Core roles: member, venue staff, CEO.",
    "- Core flows: createRedemption -> verifyRedemption -> closeOutReports.",
    "- Messaging flow: staffMessages stores venue-to-CEO and CEO-to-venue replies.",
    "- Maintenance and launch mode are controlled in settings/app.",
    "- Prefer concrete steps: where to click, what collection/doc to inspect, and exact rollback plan."
  ].join("\n");
  const dynamicContext = formatAssistantContextForPrompt(contextData, safePrompt);
  const liveContext = formatAssistantLiveSnapshot(liveSnapshot);
  const contextHash = crypto
    .createHash("sha256")
    .update(`${contextData?.buildVersion || "none"}|${contextData?.updatedAt?.seconds || 0}|${liveContext}`)
    .digest("hex")
    .slice(0, 16);
  const promptHash = crypto
    .createHash("sha256")
    .update(`${normalizedPrompt}|${contextHash}`)
    .digest("hex")
    .slice(0, 48);
  const cacheRef = db.collection(CEO_ASSISTANT_CACHE_COLLECTION).doc(promptHash);
  let cached = null;
  try {
    const cacheSnap = await cacheRef.get();
    if (cacheSnap.exists) {
      const payload = cacheSnap.data() || {};
      const cachedAt = payload.cachedAt?.toMillis ? payload.cachedAt.toMillis() : 0;
      if (cachedAt && (Date.now() - cachedAt) < (6 * 60 * 60 * 1000) && payload.reply) {
        cached = payload;
      }
    }
  } catch (_) {}
  if (cached) {
    return {
      reply: cached.reply,
      status: 200,
      cached: true,
      degraded: false
    };
  }
  const liveKey = process.env.OPENAI_API_KEY;
  if (!liveKey) {
    return {
      reply: buildAssistantFallbackReply(prompt, liveSnapshot),
      status: 0,
      degraded: true,
      fallback: true
    };
  }
  try {
    const result = await requestOpenAiWithModelFallback(liveKey, {
      messages: [
        {
          role: "system",
          content: "You are HQ AI for FoCo After Dark. Behave like a general-purpose ChatGPT assistant: answer any topic clearly and helpfully. For app-specific requests, prioritize the provided app context and live snapshot."
        },
        { role: "system", content: staticContext },
        { role: "system", content: `Live snapshot:\n${liveContext}` },
        { role: "system", content: `App code context:\n${dynamicContext || "No context synced yet."}` },
        { role: "user", content: safePrompt }
      ],
      max_tokens: 650,
      temperature: 0.4
    }, ["gpt-4o-mini", "gpt-4.1-mini", "gpt-4.1-nano"]);
    if (!result.ok) {
      const openAiErr = parseOpenAiErrorBody(result.body || "");
      console.warn("OpenAI error", result.status, openAiErr.code || "", openAiErr.message || "", result.model || "");
      return {
        reply: buildAssistantOpenAiFailureReply(result.status, openAiErr.code, openAiErr.message, liveSnapshot),
        status: result.status || 429,
        openAiCode: openAiErr.code || "",
        degraded: true,
        fallback: true
      };
    }
    const reply = clampAssistantText(result.reply || "No reply.", 6000);
    await cacheRef.set({
      prompt: clampAssistantText(prompt, 1600),
      reply,
      model: result.model || "unknown",
      status: result.status || 200,
      contextHash,
      cachedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return {
      reply,
      status: result.status || 200,
      cached: false,
      degraded: false
    };
  } catch (err) {
    console.warn('ceoChat failed', err);
    return {
      reply: buildAssistantFallbackReply(prompt, liveSnapshot),
      status: 500,
      degraded: true,
      fallback: true
    };
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
