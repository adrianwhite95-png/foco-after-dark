#!/usr/bin/env node
/*
 * Usage:
 *   node scripts/setAdminClaim.js <uid> [--admin=true|false]
 *
 * Requires one of:
 *   - GOOGLE_APPLICATION_CREDENTIALS pointing to a service account JSON file
 *   - FIREBASE_SERVICE_ACCOUNT_JSON containing service account JSON
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
  if (!path) {
    return null;
  }
  const fs = require("fs");
  return JSON.parse(fs.readFileSync(path, "utf8"));
}

async function main() {
  const uid = String(process.argv[2] || "").trim();
  if (!uid) {
    throw new Error("Usage: node scripts/setAdminClaim.js <uid> [--admin=true|false]");
  }
  const adminArg = process.argv.find((arg) => arg.startsWith("--admin="));
  const adminEnabled = adminArg ? adminArg.split("=")[1] !== "false" : true;

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

  const user = await admin.auth().getUser(uid);
  const existingClaims = user.customClaims || {};
  const nextClaims = { ...existingClaims, admin: adminEnabled };
  if (!adminEnabled) {
    delete nextClaims.admin;
  }
  await admin.auth().setCustomUserClaims(uid, nextClaims);

  // Force token refresh marker in Firestore for app-side visibility if needed.
  try {
    const db = admin.firestore();
    await db.collection("members").doc(uid).set(
      {
        claimsUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
        claimsUpdatedByScript: true,
      },
      { merge: true }
    );
  } catch (_) {}

  console.log(JSON.stringify({ ok: true, uid, admin: adminEnabled, claims: nextClaims }, null, 2));
}

main().catch((err) => {
  console.error("setAdminClaim failed:", err?.message || err);
  process.exit(1);
});
