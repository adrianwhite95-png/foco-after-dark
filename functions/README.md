# FoCo After Dark — Cloud Functions

Folder contains Firebase Cloud Functions used to securely generate and consume CEO vouchers.

Quick start (local emulator):

1. Install deps

```bash
cd functions
npm install
```

2. Start emulator

```bash
npm run start
```

3. Set custom claims for testing (use Firebase Admin SDK or Emulator UI). The callable `generateCeoVoucher` requires `admin` or `ceo` custom claim.

Deployment:

```bash
cd functions
npm run deploy
```

Notes:
- Do not deploy without verifying Firestore rules and testing in the Emulator.
- Audit logs are written to the `auditLogs` collection.

Additional notes:

- Rate limiting: `generateCeoVoucher` enforces a simple per-issuer limit (default 5/min, 200/day). You can configure limits via functions config:

	```bash
	firebase functions:config:set ceo.max_per_min=5 ceo.max_per_day=200
	```

- CI/CD: A sample GitHub Actions workflow is included at `.github/workflows/ci-deploy.yml`. To enable automatic deploys you must add repository secrets:
	- `FIREBASE_TOKEN` — obtain via `firebase login:ci`
	- `FIREBASE_PROJECT_ID` — your project id (e.g., `foco-after-dark`)

- Backups: A helper script is available at `scripts/backup_firestore.sh` to export Firestore to a GCS bucket (requires `gcloud` and an existing bucket).

