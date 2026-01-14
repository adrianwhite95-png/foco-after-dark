// Simple helper to set a custom claim for a user (Admin SDK)
// Usage: node setClaim.js <UID> [claimKey] [claimValue]
// Example: node setClaim.js uid123 admin true

const admin = require('firebase-admin');

const projectId = process.env.FIREBASE_PROJECT || 'foco-after-dark';

admin.initializeApp({ projectId });

async function main() {
  const uid = process.argv[2];
  const key = process.argv[3] || 'admin';
  const rawVal = process.argv[4] || 'true';
  if (!uid) {
    console.error('Usage: node setClaim.js <UID> [claimKey] [claimValue]');
    process.exit(1);
  }
  const value = rawVal === 'true' ? true : (rawVal === 'false' ? false : rawVal);
  const claims = {};
  claims[key] = value;
  try {
    await admin.auth().setCustomUserClaims(uid, claims);
    console.log(`Set claim on ${uid}:`, claims);
    process.exit(0);
  } catch (err) {
    console.error('Failed to set claim', err);
    process.exit(2);
  }
}

main();
