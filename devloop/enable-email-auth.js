// One-off: enable email/password sign-in provider (needed for sim test account).
// Run: FIREBASE_SA=... node devloop/enable-email-auth.js
const { GoogleAuth } = require('google-auth-library');
const sa = JSON.parse(process.env.FIREBASE_SA);

(async () => {
  const auth = new GoogleAuth({
    credentials: sa,
    scopes: ['https://www.googleapis.com/auth/cloud-platform'],
  });
  const client = await auth.getClient();
  const url = `https://identitytoolkit.googleapis.com/admin/v2/projects/${sa.project_id}/config?updateMask=signIn.email`;
  const res = await client.request({
    url,
    method: 'PATCH',
    data: { signIn: { email: { enabled: true, passwordRequired: true } } },
  });
  console.log('email sign-in enabled:', JSON.stringify(res.data.signIn?.email));
})().catch(e => { console.error(e.response?.data || e.message); process.exit(1); });
