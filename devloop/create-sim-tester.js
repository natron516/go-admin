// One-off: create the simulator test account (real Firebase user for sim testing).
// Run: railway run node devloop/create-sim-tester.js
const admin = require('firebase-admin');
const sa = JSON.parse(process.env.FIREBASE_SA);
admin.initializeApp({ credential: admin.credential.cert(sa) });

const EMAIL = 'simtester@gomedia.test';
const PASS = 'SimTest-2026!';

(async () => {
  let user;
  try {
    user = await admin.auth().getUserByEmail(EMAIL);
    console.log('exists', user.uid);
    await admin.auth().updateUser(user.uid, { password: PASS });
  } catch {
    user = await admin.auth().createUser({ email: EMAIL, password: PASS, displayName: 'Sim Tester' });
    console.log('created', user.uid);
  }
  await admin.firestore().collection('users').doc(user.uid).set(
    { pendingApproval: false, privateAccess: false, blocked: false },
    { merge: true }
  );
  // prevent signup notification spam
  await admin.firestore().collection('newSignups').doc(user.uid).set(
    { uid: user.uid, email: EMAIL, notified: true, platform: 'sim' },
    { merge: true }
  );
  console.log('done uid=' + user.uid);
  process.exit(0);
})().catch(e => { console.error(e.message); process.exit(1); });
