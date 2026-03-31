# ArchaMap NATURAL Metadata Deleted And Uses Reprocessed 2026 03 28

Follow-up cleanup applied in ArchaMap after `[[ArchaMap_NATURAL_Subdomain_Not_Primary_Domain_2026_03_28]]`.

Scope:
- Deleted the obsolete top-level `NATURAL` metadata node `CL99`
- Re-ran `processUSES` for all `CATEGORY` nodes that previously had `NATURAL` as their only non-`CATEGORY` label
- Verified the obsidian category CMIDs moved to `NATURAL` no longer retained `FEATURE` on either their `USES` ties or node labels

Results:
- `CL99` remaining: `0`
- NATURAL-only categories before processing: `377`
- NATURAL-only categories after processing: `0`
- `processUSES` batches completed successfully: `4`
- `processUSES` batch failures: `0`
- Obsidian `USES` ties still labeled `FEATURE`: `0`
- Obsidian category nodes still carrying `:FEATURE` before cleanup: `48`
- Obsidian category nodes still carrying `:FEATURE` after cleanup: `0`

Logging:
- Used standard node logging via `CM.log.createLog(..., type="node", user="user1")` while removing stale `:FEATURE` labels from the obsidian category nodes

Verification:
- `MATCH (n {CMID:"CL99"}) RETURN n` returns no rows
- No ArchaMap categories remain with labels exactly `[:CATEGORY, :NATURAL]`
- No obsidian category CMIDs in the updated set remain labeled `:FEATURE`
