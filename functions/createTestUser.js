const admin = require('firebase-admin');

const projectId = process.env.FIREBASE_PROJECT || 'foco-after-dark';
admin.initializeApp({ projectId });

async function main() {
  const email = process.argv[2] || `ceo_tester_${Date.now() % 10000}@example.com`;
  const password = process.argv[3] || 'Password123!';
  try {
    let userRecord;
    try {
      userRecord = await admin.auth().getUserByEmail(email);
      console.log('User already exists:', userRecord.uid);
    } catch (err) {
      userRecord = await admin.auth().createUser({ email, password });
      console.log('Created user:', userRecord.uid);
    }
    await admin.auth().setCustomUserClaims(userRecord.uid, { admin: true });
    console.log('Set admin claim for', userRecord.uid);
    console.log('Email:', email);
    console.log('Password:', password);
    console.log('UID:', userRecord.uid);
    process.exit(0);
  } catch (err) {
    console.error('Failed to create or claim user', err);
    process.exit(2);
  }
}

main();
