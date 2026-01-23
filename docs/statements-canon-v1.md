
# Statements Canon v1 (Frozen)

## Status
**FROZEN — DO NOT MODIFY**

Frozen after successful genesis:
- NAV run created & audited
- NAV snapshot resolved
- Allocations computed via `nav_snapshot_id`
- Capital accounts finalized
- Statement Run v1 created and SEALED
- Dual-control & audit enforcement active

## Invariants (Non-Negotiable)
1. **NAV → Snapshot → Allocations → Capital → Statements**
2. Allocations require `nav_snapshot_id` (no dates, no lookups).
3. SEALED records are immutable.
4. Any post-seal change requires **dual-control** + **audit**.
5. No silent backfill. Backfill must be explicit and logged.
6. DDL on canon schemas is blocked at the DB layer.

## Governance
- Audit required before state mutation.
- Dual-control required for:
  - Statement regeneration / reseal
  - NAV reopen
  - Capital corrections
  - Event backdating
- Approvals are time-boxed and require two distinct approvers.

## Schemas in Scope
- `nav`
- `capital`
- `statements`
- `governance`
- `canon`

## Migration Policy
- Canon is locked via `canon.migration_lock_v1`.
- Any future change requires:
  - New Canon (v2)
  - Explicit unfreeze procedure
  - New checksum + registry entry

## Unfreeze Procedure (v2)
1. Remove DDL block trigger
2. Unlock migration lock
3. Apply migrations
4. Re-freeze with new checksum
5. Document deltas

## Notes
This canon is designed to be auditable, reproducible, and hostile-environment safe.


