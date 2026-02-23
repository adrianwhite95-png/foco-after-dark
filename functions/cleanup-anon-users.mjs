import fs from "node:fs";
import admin from "firebase-admin";

const keyPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
if (!keyPath) {
  console.error("Set GOOGLE_APPLICATION_CREDENTIALS to your service-account JSON path.");
  process.exit(1);
}
const svc = JSON.parse(fs.readFileSync(keyPath, "utf8"));
admin.initializeApp({ credential: admin.credential.cert(svc) });

function isAnon(u) {
  return (!u.email || u.email === "") && (!u.providerData || u.providerData.length === 0);
}

let pageToken = undefined;
const anonUids = [];
do {
  const res = await admin.auth().listUsers(1000, pageToken);
  for (const u of res.users) if (isAnon(u)) anonUids.push(u.uid);
  pageToken = res.pageToken;
} while (pageToken);

console.log("Anonymous users found:", anonUids.length);

let deleted = 0;
for (let i = 0; i < anonUids.length; i += 1000) {
  const batch = anonUids.slice(i, i + 1000);
  const out = await admin.auth().deleteUsers(batch);
  deleted += out.successCount;
  if (out.failureCount) {
    console.log("Batch failures:", out.failureCount);
  }
}
console.log("Deleted:", deleted);
