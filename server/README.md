FoCo After Dark — External Voucher API

This small Express server provides two endpoints to securely create and consume CEO vouchers if you cannot deploy Firebase Cloud Functions (Blaze required).

Endpoints
- POST /generateCeoVoucher — requires Authorization: Bearer <Firebase ID token>; returns { code, perk }
- POST /useCeoVoucher — requires Authorization: Bearer <Firebase ID token>; body { code }

Environment variables
- `SERVICE_ACCOUNT_KEY` — required. JSON string of a Firebase service account (do NOT commit). Example: `export SERVICE_ACCOUNT_KEY="$(cat serviceAccount.json)"`
- `FIREBASE_PROJECT_ID` — optional, defaults to service account project id
- `CEO_MAX_PER_MIN` — optional, default 5
- `CEO_MAX_PER_DAY` — optional, default 200
- `PORT` — optional, default 8787

Local testing
1. Create a service account JSON in GCP Console with Firestore & Auth permissions and download it as `serviceAccount.json`.
2. From the `server/` folder:
```bash
npm install
export SERVICE_ACCOUNT_KEY="$(cat ../serviceAccount.json)"
export FIREBASE_PROJECT_ID="foco-after-dark"
node index.js
```
3. The server will run on `http://localhost:8787` by default.

Deploying to Render (recommended free option)
1. Create a new Web Service on Render and connect your repo.
2. Set the Start Command to `node index.js`.
3. Add environment variables in the Render dashboard:
   - `SERVICE_ACCOUNT_KEY` (paste entire JSON)
   - `FIREBASE_PROJECT_ID`
   - `CEO_MAX_PER_MIN` and `CEO_MAX_PER_DAY` if desired
4. Deploy. Use the provided service URL as `SERVER_API_ORIGIN` in `index.html` (see below).

Deploying to Vercel
1. Convert this server to Vercel Serverless functions (optional) or deploy as a server on another host.
2. Provide the same env vars in Vercel dashboard.

Client integration
1. Set a global variable `window.SERVER_API_ORIGIN` pointing to your deployed server (e.g. `https://api.focoafterdark.com`). You can add a small script snippet in `index.html` before the main app script:
```html
<script>window.SERVER_API_ORIGIN = 'https://your-server.example.com'</script>
```
2. The app will try Firebase Functions first, then this external endpoint, then fall back to local generation.

Security notes
- Keep `SERVICE_ACCOUNT_KEY` secret; use platform-provided secret storage.
- Ensure HTTPS and restrict access (CORS) appropriately.
- Rotate service account keys if leaked.
