Realtime Test Protocol

1) Device A (member): redeem a venue perk.
2) Device B (same member): confirm balance + redemptions update within seconds.
3) Staff portal (venue): pending queue shows new redemption instantly.
4) Staff confirms: both member devices update within seconds.
5) CEO dashboard: redemptions + venue counts update within seconds.
6) Toggle Wi‑Fi off on Device A: pill shows Offline, UI stays readable.
7) Toggle Wi‑Fi on: pill returns to Syncing briefly then Live without refresh.
8) Lock app (`settings/app.appLocked = true`): member UI still loads read‑only; pill shows Live (or No access if user lacks rules access).
9) While locked: verify redeem/verify actions are blocked (no writes).

Notes
- These steps assume Firestore listeners are connected and functions deploy is live.
- No local tests were executed in this workspace.
