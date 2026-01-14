const admin = require('firebase-admin');
const projectId = process.env.FIREBASE_PROJECT || 'foco-after-dark';
admin.initializeApp({ projectId });

async function main() {
  const db = admin.firestore();
  try {
    const snap = await db.collection('ceoVouchers').get();
    if (snap.empty) {
      console.log('No ceoVouchers found');
      return process.exit(0);
    }
    snap.forEach(doc => {
      console.log(doc.id, JSON.stringify(doc.data()));
    });
    const audits = await db.collection('auditLogs').orderBy('timestamp', 'desc').limit(10).get();
    console.log('Recent auditLogs:');
    audits.forEach(a => console.log(a.id, JSON.stringify(a.data())));
    process.exit(0);
  } catch (err) {
    console.error('Failed to read Firestore', err);
    process.exit(2);
  }
}

main();
