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
  bondi_beach: { name: "Bondi Beach Bar & Grill", login: "bondi" },
  rec_room: { name: "Rec Room Fort Collins", login: "rec" },
  road_34: { name: "Road 34 Bike Bar", login: "road34" },
  pour_brothers: { name: "Pour Brothers Community Tavern", login: "pour" },
  lucky_joes: { name: "Lucky Joe's Sidewalk Saloon", login: "joes" },
  whiskey: { name: "The Whiskey", login: "whiskey" },
  yeti: { name: "Yeti Bar & Grill", login: "yeti" },
  drunken_monkey: { name: "The Drunken Monkey", login: "monkey" },
  boot_grill: { name: "The Boot Grill", login: "boot" },
  trail_head: { name: "Trail Head Tavern", login: "trail" },
  steak_out: { name: "Steak-Out Saloon", login: "steak" },
  mo_jeaux: { name: "Mo Jeaux's Bar & Grill", login: "jeaux" },
  old_town_putt: { name: "Old Town Putt", login: "putt" },
  social: { name: "Social", login: "social" },
  ace_gilletts: { name: "Ace Gillett's Lounge", login: "ace" },
  elliotts: { name: "Elliott's Martini Bar", login: "elliotts" },
  tap_handle: { name: "Tap and Handle", login: "tap" },
  bar_louie: { name: "Bar Louie", login: "louie" },
  cb_potts: { name: "C.B. & Potts", login: "cb" },
  intersect: { name: "Intersect Brewing", login: "intersect" },
  maxline: { name: "Maxline Brewing", login: "maxline" },
  odell: { name: "Odell Brewing Taproom", login: "odell" },
  new_belgium: { name: "New Belgium Brewing", login: "belgium" },
  equinox: { name: "Equinox Brewing", login: "equinox" },
};
const STAFF_VENUE_ALIASES = {
  district: "bar_district",
  "the bar district": "bar_district",
  "bar district": "bar_district",
  bondi: "bondi_beach",
  "bondi beach": "bondi_beach",
  "bondi beach bar": "bondi_beach",
  rec: "rec_room",
  recroom: "rec_room",
  "rec room": "rec_room",
  "rec room fort collins": "rec_room",
  road34: "road_34",
  road: "road_34",
  "road 34": "road_34",
  "road 34 bike bar": "road_34",
  pour: "pour_brothers",
  "pour brothers": "pour_brothers",
  "pour brothers community tavern": "pour_brothers",
  joes: "lucky_joes",
  "lucky joes": "lucky_joes",
  "lucky joe's sidewalk saloon": "lucky_joes",
  whiskey: "whiskey",
  "the whiskey": "whiskey",
  yeti: "yeti",
  "yeti bar": "yeti",
  monkey: "drunken_monkey",
  "drunken monkey": "drunken_monkey",
  "the drunken monkey": "drunken_monkey",
  boot: "boot_grill",
  "boot grill": "boot_grill",
  "the boot grill": "boot_grill",
  trail: "trail_head",
  "trail head": "trail_head",
  "trail head tavern": "trail_head",
  steak: "steak_out",
  "steak out": "steak_out",
  "steak-out saloon": "steak_out",
  jeaux: "mo_jeaux",
  "mo jeaux": "mo_jeaux",
  "mo jeaux's": "mo_jeaux",
  putt: "old_town_putt",
  "old town putt": "old_town_putt",
  social: "social",
  ace: "ace_gilletts",
  "ace gilletts": "ace_gilletts",
  "ace gillett's": "ace_gilletts",
  elliott: "elliotts",
  elliotts: "elliotts",
  "elliott's": "elliotts",
  tap: "tap_handle",
  "tap and handle": "tap_handle",
  louie: "bar_louie",
  "bar louie": "bar_louie",
  cb: "cb_potts",
  "cb potts": "cb_potts",
  "c.b. & potts": "cb_potts",
  intersect: "intersect",
  maxline: "maxline",
  odell: "odell",
  belgium: "new_belgium",
  "new belgium": "new_belgium",
  equinox: "equinox"
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

async function sendReportEmail(subject, text) {
  const transporter = getReportTransporter();
  if (!transporter) {
    return { sent: false, error: "SMTP not configured" };
  }
  const from = process.env.REPORTS_SMTP_FROM || process.env.REPORTS_SMTP_USER || "reports@focoafterdark.com";
  try {
    await transporter.sendMail({
      from: `FoCo After Dark <${from}>`,
      to: REPORTS_TO_EMAIL,
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
  const uname = (profile.username || '').trim().toLowerCase();
  if (uname) {
    await db.collection('usernames').doc(uname).set({
      uid,
      email,
      passCode,
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

// Nightly 3am MT close-out summary with analytics + guaranteed email queue
exports.nightlyCloseOut = functions.runWith(reportEmailSecrets).pubsub.schedule('0 3 * * *').timeZone('America/Denver').onRun(async () => {
  const now = admin.firestore.Timestamp.now();
  const dayStart = admin.firestore.Timestamp.fromDate(new Date(Date.now() - 24 * 60 * 60 * 1000));

  const VENUE_CATALOG = {
    bar_district: "The Bar District",
    bondi_beach: "Bondi Beach Bar & Grill",
    rec_room: "Rec Room Fort Collins",
    road_34: "Road 34 Bike Bar",
    pour_brothers: "Pour Brothers Community Tavern",
    lucky_joes: "Lucky Joe’s Sidewalk Saloon",
    whiskey: "The Whisk(e)y",
    yeti: "Yeti Bar & Grill",
    drunken_monkey: "The Drunken Monkey",
    boot_grill: "The Boot Grill",
    trail_head: "Trail Head Tavern",
    steak_out: "Steak-Out Saloon",
    mo_jeaux: "Mo Jeaux’s Bar & Grill",
    old_town_putt: "Old Town Putt (bar + mini golf)",
    social: "Social",
    ace_gilletts: "Ace Gillett’s Lounge",
    elliotts: "Elliott’s Martini Bar",
    tap_handle: "Tap and Handle",
    bar_louie: "Bar Louie",
    cb_potts: "C.B. & Potts (Collindale)",
    intersect: "Intersect Brewing",
    maxline: "Maxline Brewing",
    odell: "Odell Brewing Taproom",
    new_belgium: "New Belgium Brewing",
    equinox: "Equinox Brewing",
    beta: "FoCo Beta Venue"
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
    const redSnap = await db.collection('redemptions')
      .where('timestamp', '>=', dayStart)
      .get();
    summary.scansToday = redSnap.size;
    const guestSet = new Set();
    const venueSet = new Set();
    const perkCounts = {};
    redSnap.forEach(doc => {
      const d = doc.data() || {};
      if (d.passId) guestSet.add(String(d.passId).toUpperCase());
      if (d.venue) venueSet.add(String(d.venue).toLowerCase());
      const perkKey = (d.perkKey || d.perk || 'perk').toLowerCase();
      perkCounts[perkKey] = (perkCounts[perkKey] || 0) + 1;

      const venueId = (d.venue || 'unknown').toLowerCase();
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
    const dealSnap = await db.collection('vipDeals')
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
    meta: summary
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
  const smtpResult = await sendReportEmail(payload.subject, humanText);
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
  if (!token) throw new HttpsError('invalid-argument', 'Token required');
  const uid = context.auth.uid;
  const ref = db.collection('pushTokens').doc(uid).collection('tokens').doc(token);
  await ref.set({
    token,
    uid,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return { success: true };
});

// Push notifications: send to a user by uid
exports.sendPushToUser = functions.https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const targetUid = data?.uid;
  const title = (data?.title || 'FoCo Alert').toString();
  const body = (data?.body || '').toString().slice(0, 200);
  if (!targetUid || !body) throw new HttpsError('invalid-argument', 'uid and body required');
  const tokenSnap = await db.collection('pushTokens').doc(targetUid).collection('tokens').get();
  const tokens = tokenSnap.docs.map(d => d.id).filter(Boolean);
  if (!tokens.length) return { success: false, reason: 'no_tokens' };
  const message = {
    notification: { title, body },
    tokens
  };
  const res = await messaging.sendEachForMulticast(message);
  return { success: true, sent: res.successCount, failed: res.failureCount };
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

const TOKEN_PACKS = {
  tokens_1: { perk: "tokens", amount: 1, priceCents: 300, priceLabel: "$3.00", label: "Redemption token" },
  tokens_2: { perk: "tokens", amount: 2, priceCents: 600, priceLabel: "$6.00", label: "Redemption tokens" },
  tokens_3: { perk: "tokens", amount: 3, priceCents: 900, priceLabel: "$9.00", label: "Redemption tokens" },
  tokens_4: { perk: "tokens", amount: 4, priceCents: 1200, priceLabel: "$12.00", label: "Redemption tokens" },
  tokens_5: { perk: "tokens", amount: 5, priceCents: 1500, priceLabel: "$15.00", label: "Redemption tokens" },
};

const VOUCHER_PACKS = {
  standard: {
    drink: { perk: "drink", amount: 2, priceCents: 600, priceLabel: "$6.00", label: "$3 drink voucher" },
    shot: { perk: "shot", amount: 4, priceCents: 500, priceLabel: "$5.00", label: "$1 shot voucher" },
    cover: { perk: "cover", amount: 3, priceCents: 2000, priceLabel: "$20.00", label: "Skip Line + No Cover Charge" },
    ...TOKEN_PACKS,
  },
  vip: {
    drink: { perk: "drink", amount: 4, priceCents: 1000, priceLabel: "$10.00", label: "$3 drink voucher" },
    shot: { perk: "shot", amount: 4, priceCents: 500, priceLabel: "$5.00", label: "$1 shot voucher" },
    cover: { perk: "cover", amount: 3, priceCents: 1500, priceLabel: "$15.00", label: "Skip Line + No Cover Charge" },
    ...TOKEN_PACKS,
  },
};

function getTokenCycleKey(date = new Date()) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

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
    const cycleKey = getTokenCycleKey();
    let nextCount = pack.amount;
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const data = snap.exists ? snap.data() : {};
      const currentCycle = data.extraRedemptionTokensCycle || "";
      const current = currentCycle === cycleKey ? Number(data.extraRedemptionTokens || 0) : 0;
      nextCount = current + pack.amount;
      tx.set(ref, {
        extraRedemptionTokens: nextCount,
        extraRedemptionTokensCycle: cycleKey,
        lastVoucherPurchase: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    });
    return { extraRedemptionTokens: nextCount, extraRedemptionTokensCycle: cycleKey };
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

async function ensureStripeCustomer(uid, email) {
  const ref = db.collection('members').doc(uid);
  const snap = await ref.get();
  const data = snap.exists ? snap.data() : {};
  if (data.stripeCustomerId) return data.stripeCustomerId;
  const stripe = getStripeClient();
  const customer = await stripe.customers.create({
    email: email || undefined,
    metadata: { uid }
  });
  await ref.set({ stripeCustomerId: customer.id }, { merge: true });
  return customer.id;
}

const stripeSecrets = { secrets: ["STRIPE_SECRET", "STRIPE_PUBLISHABLE"] };
const stripeWebhookSecrets = { secrets: ["STRIPE_SECRET", "STRIPE_WEBHOOK_SECRET"] };

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

exports.createMembershipPaymentIntent = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const tier = (data?.tier || 'standard').toString();
  // CEO should not be charged
  if ((email === 'ceo@gmail.com') || context.auth.token.ceo) {
    return { clientSecret: null, publishableKey: publishable, free: true };
  }
  const amount = priceForTier(tier);
  const customerId = await ensureStripeCustomer(uid, email);
  const intent = await stripe.paymentIntents.create({
    amount,
    currency: 'usd',
    customer: customerId,
    payment_method_types: ['card'],
    setup_future_usage: 'off_session',
    metadata: { uid, tier }
  });
  return { clientSecret: intent.client_secret, publishableKey: publishable };
});

exports.confirmMembershipActivation = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const stripe = getStripeClient();
  const uid = context.auth.uid;
  const paymentIntentId = (data?.paymentIntentId || '').toString();
  if (!paymentIntentId) throw new HttpsError('invalid-argument', 'paymentIntentId required');
  const intent = await stripe.paymentIntents.retrieve(paymentIntentId);
  if (intent.status !== 'succeeded') throw new HttpsError('failed-precondition', 'Payment not successful');
  const tier = intent.metadata?.tier || 'standard';
  const customerId = intent.customer;
  const defaultPm = intent.payment_method;
  const nextRenewal = new Date();
  nextRenewal.setMonth(nextRenewal.getMonth() + 1);
  await db.collection('members').doc(uid).set({
    tier,
    stripeCustomerId: customerId,
    defaultPaymentMethodId: defaultPm || null,
    paymentStatus: 'active',
    nextRenewal: nextRenewal.toISOString(),
    lastCharge: new Date().toISOString()
  }, { merge: true });
  return { ok: true, tier };
});

function nextMonthlyRenewalISO(fromDate = new Date()) {
  const next = new Date(fromDate);
  next.setMonth(next.getMonth() + 1);
  return next.toISOString();
}

async function applyStripePaymentIntentUpdate(intent, status) {
  const uid = (intent?.metadata?.uid || "").toString();
  if (!uid) return;
  const updates = {
    lastStripeEvent: status,
    lastStripeEventAt: admin.firestore.FieldValue.serverTimestamp()
  };
  if (status === "succeeded") {
    const tier = (intent?.metadata?.tier || "").toString();
    if (tier) updates.tier = tier;
    updates.paymentStatus = "active";
    updates.lastCharge = new Date().toISOString();
    updates.nextRenewal = nextMonthlyRenewalISO();
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
    }
  } catch (err) {
    console.warn("Stripe webhook handler failed", err?.message || err);
    res.status(500).send("Webhook handler failed");
    return;
  }
  res.json({ received: true });
});

exports.createSetupIntent = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const customerId = await ensureStripeCustomer(uid, email);
  const setupIntent = await stripe.setupIntents.create({
    customer: customerId,
    payment_method_types: ['card'],
    metadata: { uid }
  });
  return { clientSecret: setupIntent.client_secret, publishableKey: publishable };
});

exports.createBillingPortalSession = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const stripe = getStripeClient();
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const customerId = await ensureStripeCustomer(uid, email);
  const returnUrl = (data?.returnUrl || 'https://foco-after-dark.web.app').toString();
  const session = await stripe.billingPortal.sessions.create({
    customer: customerId,
    return_url: returnUrl
  });
  return { url: session.url };
});

exports.chargeMembershipOnFile = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const tier = (data?.tier || 'standard').toString();

  const memberRef = db.collection('members').doc(uid);
  const memberSnap = await memberRef.get();
  const profile = memberSnap.exists ? memberSnap.data() : {};

  if (isCeoContext(context, profile) || profile.freeMembership) {
    return { free: true, tier };
  }

  const amount = priceForTier(tier);
  const customerId = await ensureStripeCustomer(uid, email);
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

  const intent = await stripe.paymentIntents.create({
    amount,
    currency: 'usd',
    customer: customerId,
    payment_method: defaultPm,
    confirm: true,
    off_session: false,
    payment_method_types: ['card'],
    setup_future_usage: 'off_session',
    metadata: { uid, tier, source: 'membership_change' }
  });

  if (intent.status === 'requires_action' && intent.client_secret) {
    return {
      requiresAction: true,
      clientSecret: intent.client_secret,
      publishableKey: publishable,
      paymentIntentId: intent.id,
      tier
    };
  }
  if (intent.status !== 'succeeded') {
    throw new HttpsError('failed-precondition', 'Payment did not complete');
  }

  const nextRenewal = new Date();
  nextRenewal.setMonth(nextRenewal.getMonth() + 1);
  await memberRef.set({
    tier,
    stripeCustomerId: customerId,
    defaultPaymentMethodId: defaultPm || intent.payment_method || null,
    paymentStatus: 'active',
    nextRenewal: nextRenewal.toISOString(),
    lastCharge: new Date().toISOString()
  }, { merge: true });
  return { ok: true, tier };
});

exports.createVoucherPaymentIntent = functions.runWith(stripeSecrets).https.onCall(async (data, context) => {
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const packId = (data?.packId || '').toString().toLowerCase();
  const memberRef = db.collection('members').doc(uid);
  const memberSnap = await memberRef.get();
  const profile = memberSnap.exists ? memberSnap.data() : {};
  if (isCeoContext(context, profile) || profile.freeMembership) {
    throw new HttpsError('failed-precondition', 'Free/CEO accounts do not need voucher purchases');
  }
  const tier = (profile.tier || 'standard').toString();
  const pack = resolveVoucherPack(tier, packId);
  if (!pack) throw new HttpsError('invalid-argument', 'Unknown voucher pack');
  const customerId = await ensureStripeCustomer(uid, email);
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
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const stripe = getStripeClient();
  const { publishable } = getStripeConfig();
  if (!publishable) throw new HttpsError('failed-precondition', 'Stripe not configured');
  const uid = context.auth.uid;
  const email = (context.auth.token.email || '').toLowerCase();
  const packId = (data?.packId || '').toString().toLowerCase();
  const memberRef = db.collection('members').doc(uid);
  const memberSnap = await memberRef.get();
  const profile = memberSnap.exists ? memberSnap.data() : {};
  if (isCeoContext(context, profile) || profile.freeMembership) {
    throw new HttpsError('failed-precondition', 'Free/CEO accounts do not need voucher purchases');
  }
  const tier = (profile.tier || 'standard').toString();
  const pack = resolveVoucherPack(tier, packId);
  if (!pack) throw new HttpsError('invalid-argument', 'Unknown voucher pack');
  const customerId = await ensureStripeCustomer(uid, email);
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
  if (!context.auth) throw new HttpsError('unauthenticated', 'Auth required');
  const stripe = getStripeClient();
  const uid = context.auth.uid;
  const paymentIntentId = (data?.paymentIntentId || '').toString();
  if (!paymentIntentId) throw new HttpsError('invalid-argument', 'paymentIntentId required');
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
  if (isBeta) {
    const appSnap = await db.collection('settings').doc('app').get();
    const launched = appSnap.exists ? !!appSnap.data().launched : false;
    if (launched) throw new HttpsError('permission-denied', 'Beta access is disabled in live mode');
    if (passNorm !== expectedPass) {
      throw new HttpsError('permission-denied', 'Invalid passphrase');
    }
  } else {
    if (!STAFF_VENUES[venueId]) throw new HttpsError('not-found', 'Unknown venue');
    if (passNorm !== expectedPass) throw new HttpsError('permission-denied', 'Invalid passphrase');
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
  const pageToken = data?.pageToken || undefined;
  const dryRun = data?.dryRun === true;
  const result = await admin.auth().listUsers(limit, pageToken);

  let matchedUsers = 0;
  let deletedUsers = 0;
  let deletedMemberDocs = 0;
  let deletedUsernames = 0;
  const errors = [];

  for (const user of result.users) {
    const isAnon = (user.providerData || []).length === 0 && !user.email && !user.phoneNumber;
    if (!isAnon) continue;
    matchedUsers += 1;
    if (dryRun) continue;
    const uid = user.uid;
    try {
      const memberRef = db.collection('members').doc(uid);
      const memberSnap = await memberRef.get();
      const memberData = memberSnap.exists ? memberSnap.data() : {};
      const isCeo = memberData.ceo === true ||
        (memberData.passCode || "").toUpperCase() === CEO_PASS_ID ||
        (memberData.email || "").toLowerCase() === "ceo@gmail.com";
      if (isCeo) continue;
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
    } catch (err) {
      errors.push({ uid: user.uid, error: err?.message || String(err) });
    }
  }

  return {
    ok: true,
    matchedUsers,
    deletedUsers,
    deletedMemberDocs,
    deletedUsernames,
    processed: result.users.length,
    nextPageToken: result.pageToken || null,
    dryRun,
    errors: errors.slice(0, 5)
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
  const updates = {
    tier: null,
    paymentStatus: 'canceled',
    nextRenewal: null,
    lastCharge: null,
    defaultPaymentMethodId: null,
    canceledAt: new Date().toISOString()
  };

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
  return { ok: true, canceled: true, wiped: wipe };
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
  const now = new Date();
  const isoNow = now.toISOString();
  const snap = await db.collection('members')
    .where('nextRenewal', '<=', isoNow)
    .limit(200)
    .get();
  for (const docSnap of snap.docs) {
    const data = docSnap.data() || {};
    const uid = docSnap.id;
    if (data.paymentStatus === 'canceled' || data.paused === true || !data.tier) {
      continue;
    }
    const tier = data.tier || 'standard';
    const customerId = data.stripeCustomerId;
    const defaultPm = data.defaultPaymentMethodId;
    const isCeo = data.ceo === true || (data.email || '').toLowerCase() === 'ceo@gmail.com' || (data.passCode || '').toUpperCase() === 'DREE4695';
    if (isCeo) continue;
    if (!customerId || !defaultPm) {
      await docSnap.ref.set({ paymentStatus: 'past_due' }, { merge: true });
      continue;
    }
    try {
      const intent = await stripe.paymentIntents.create({
        amount: priceForTier(tier),
        currency: 'usd',
        customer: customerId,
        payment_method: defaultPm,
        off_session: true,
        confirm: true,
        metadata: { uid, tier, renewal: 'true' }
      });
      if (intent.status === 'succeeded') {
        const next = new Date(now);
        next.setMonth(next.getMonth() + 1);
        await docSnap.ref.set({
          paymentStatus: 'active',
          lastCharge: isoNow,
          nextRenewal: next.toISOString()
        }, { merge: true });
      } else {
        await docSnap.ref.set({ paymentStatus: 'past_due' }, { merge: true });
      }
    } catch (err) {
      console.warn('Renewal charge failed for', uid, err?.message);
      await docSnap.ref.set({ paymentStatus: 'past_due' }, { merge: true });
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
