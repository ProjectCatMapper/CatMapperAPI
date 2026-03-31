# ArchaMap Built Feature USES Label Updated To NATURAL 2026 03 28

Manual database change applied in ArchaMap.

Scope:
- Category CMIDs `AM37714` through the provided list ending in `AM37765`
- Updated the `label` property on matching `USES` relationships to `NATURAL`

Logging:
- Used the standard relation logging process via `CM.log.createLog(..., type="relation", user="user1")`
- Sample verified log:
  - `updated USES label from FEATURE to NATURAL for CATEGORY AM37714 in DATASET AD37660`

Results:
- `97` `USES` relationships were logged
- `97` `USES` relationships were updated

Verification:
- `AM37714` in dataset `AD37660` now has `r.label = 'NATURAL'`
