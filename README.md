# Foco After Dark

[![Deploy Production](https://github.com/adrianwhite95-png/foco-after-dark/actions/workflows/deploy.yml/badge.svg?branch=main)](https://github.com/adrianwhite95-png/foco-after-dark/actions/workflows/deploy.yml)

Production site: https://foco-after-dark.web.app

## CEO Control Center Ops

### Set admin claim for CEO user (one-time)

```bash
node scripts/setAdminClaim.js <CEO_UID>
```

Optional revoke:

```bash
node scripts/setAdminClaim.js <CEO_UID> --admin=false
```

Script uses `GOOGLE_APPLICATION_CREDENTIALS` (service account JSON) or `FIREBASE_SERVICE_ACCOUNT_JSON`.

### Venue soft-control backfill (safe)

Dry-run:

```bash
node scripts/backfillVenueAdminFields.js --dry-run
```

Apply:

```bash
node scripts/backfillVenueAdminFields.js
```

Backfill only fills missing fields on `venues/{venueId}`:
- `status = "active"`
- `showInHomeFeed = true`
- `showLivePill = true`
- `priority = 50`
