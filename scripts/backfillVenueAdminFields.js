#!/usr/bin/env node
/*
 * Usage:
 *   node scripts/backfillVenueAdminFields.js [--dry-run]
 *
 * Adds default soft-control fields to venues documents when missing:
 *   status="active", showInHomeFeed=true, showLivePill=true, priority=50
 */

function loadAdminSdk() {
  try {
    return require("firebase-admin");
  } catch (_) {
    return require("../functions/node_modules/firebase-admin");
  }
}

function parseServiceAccount() {
  const inline = process.env.FIREBASE_SERVICE_ACCOUNT_JSON || "";
  if (inline.trim()) {
    return JSON.parse(inline);
  }
  const path = process.env.GOOGLE_APPLICATION_CREDENTIALS || "";
  if (!path) return null;
  const fs = require("fs");
  return JSON.parse(fs.readFileSync(path, "utf8"));
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const admin = loadAdminSdk();
  const serviceAccount = parseServiceAccount();
  if (admin.apps.length === 0) {
    if (serviceAccount) {
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: serviceAccount.project_id,
      });
    } else {
      admin.initializeApp();
    }
  }

  const db = admin.firestore();
  const snap = await db.collection("venues").get();
  let scanned = 0;
  let updated = 0;
  let unchanged = 0;

  const batch = db.batch();
  snap.forEach((docSnap) => {
    scanned += 1;
    const data = docSnap.data() || {};
    const patch = {};
    if (typeof data.status !== "string" || !data.status.trim()) patch.status = "active";
    if (typeof data.showInHomeFeed !== "boolean") patch.showInHomeFeed = true;
    if (typeof data.showLivePill !== "boolean") patch.showLivePill = true;
    if (!Number.isFinite(Number(data.priority))) patch.priority = 50;
    if (!Object.keys(patch).length) {
      unchanged += 1;
      return;
    }
    patch.updatedAt = admin.firestore.FieldValue.serverTimestamp();
    patch.updatedByScript = "backfillVenueAdminFields";
    updated += 1;
    if (!dryRun) {
      batch.set(docSnap.ref, patch, { merge: true });
    }
  });

  if (!dryRun && updated > 0) {
    await batch.commit();
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        dryRun,
        scanned,
        updated,
        unchanged,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("backfillVenueAdminFields failed:", err?.message || err);
  process.exit(1);
});
