import admin from "firebase-admin";

const projectId = process.env.GCLOUD_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || "foco-after-dark";
const days = Math.max(1, Math.min(365, Number(process.env.BACKFILL_DAYS || 90)));
const maxDocs = Math.max(100, Math.min(50000, Number(process.env.BACKFILL_MAX_DOCS || 20000)));

if (!admin.apps.length) {
  admin.initializeApp({ projectId });
}

const db = admin.firestore();

function inferQueueScope(path = "") {
  const value = String(path || "");
  if (value.includes("/venues/") && value.includes("/redemptions/")) return "venue";
  if (value.includes("/members/") && value.includes("/redemptions/")) return "member";
  return "legacy";
}

async function run() {
  const cutoffMs = Date.now() - days * 24 * 60 * 60 * 1000;
  const cutoff = admin.firestore.Timestamp.fromMillis(cutoffMs);
  const snap = await db
    .collectionGroup("redemptions")
    .where("createdAt", ">=", cutoff)
    .limit(maxDocs)
    .get();

  let scanned = 0;
  let updated = 0;
  let unchanged = 0;
  const byScope = { venue: 0, member: 0, legacy: 0 };

  let batch = db.batch();
  let ops = 0;
  const flush = async () => {
    if (!ops) return;
    await batch.commit();
    batch = db.batch();
    ops = 0;
  };

  for (const docSnap of snap.docs) {
    scanned += 1;
    const expected = inferQueueScope(docSnap.ref.path);
    byScope[expected] = (byScope[expected] || 0) + 1;
    const current = String((docSnap.data() || {}).queueScope || "").toLowerCase();
    if (current === expected) {
      unchanged += 1;
      continue;
    }
    batch.set(
      docSnap.ref,
      {
        queueScope: expected,
        queueScopeBackfilledAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true },
    );
    updated += 1;
    ops += 1;
    if (ops >= 400) await flush();
  }
  await flush();

  console.log(
    JSON.stringify(
      {
        ok: true,
        projectId,
        days,
        maxDocs,
        scanned,
        updated,
        unchanged,
        byScope,
        cutoffMs,
      },
      null,
      2,
    ),
  );
}

run().catch((err) => {
  console.error("Backfill failed:", err?.message || err);
  process.exitCode = 1;
});

