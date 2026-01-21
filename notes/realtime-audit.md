# Realtime Audit (Phase 1 hardening)

This table documents user-visible shared state that must be consistent across devices and the Firestore realtime source of truth used by the client.

| Feature | Old source (non-realtime) | New Firestore path (source of truth) | Listener label (client) |
|---|---|---|---|
| App lock / launch mode | Local flags / page reload | `settings/app` | `settingsApp` |
| Landing “Current users” | Local placeholder | `settings/appStats` | `appStats` |
| Member profile (tier, passCode, status, billing flags) | localStorage `membership_*`, `passcode_*`, in-memory `memberProfile` | `members/{uid}` | `member` |
| Member redemption history (pending/verified) | localStorage `redemptionLog` | `members/{uid}/redemptions` | `memberRedemptions` |
| Staff pending queue + venue redemption log | localStorage `staffRedemptions_*`, in-memory arrays | `venues/{venueId}/redemptions` | `staffRedemptions` |
| Venue monthly perks list | `venuePerksCache` / legacy `venuePerks` | `venues/{venueId}/perks` (and global via collectionGroup) | `staffPerks`, `redeemPerks`, `globalPerks` |
| Deals feed | localStorage `vipDealsCache` | `deals` | `globalDeals` |
| Alerts feed + ticker | localStorage `tonightAlertsCache` | `alerts` | `globalAlerts` |
| Pending redemption modal watcher | in-memory polling | `members/{uid}/redemptions/{redemptionId}` | `redeemWatch` |
| CEO redemptions analytics | local cache | `collectionGroup(redemptions)` | `ceoRedemptions` |
| Staff → CEO messages | local cache | `staffMessages` (filtered) | `staffMessages`, `ceoMessages` |

## Notes

- localStorage is still used for cosmetic UX and offline demo/beta fallback (accent theme, background theme, onboarding, “remember email”, etc).
- For signed-in members/staff, redemption creation/verification is server-trusted via Cloud Functions; the UI is driven by the snapshot listeners above.

