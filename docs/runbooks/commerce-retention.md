# Commerce replay and correction retention contract

This contract applies to the existing single-writer full-snapshot commerce path.
The default is to retain every accepted batch's source and validated silver
observations for the lifetime of that warehouse. There is no automatic commerce
row TTL and no implemented compacted baseline that permits discarding earlier
customer observations. No finite replay or late-correction window is established
by the Iceberg maintenance defaults.

Replay is supported only while the original hash-bound manifest and payloads,
checkpoint and unfinished-run intent remain available and consistent with the
published warehouse. Customer late correction additionally requires complete
validated silver customer history from the earliest published customer version.
These are availability preconditions, not an unlimited recovery guarantee.

## What must survive

| State | Required purpose | Retention decision |
|---|---|---|
| Original landing manifest and all four payloads | Integrity-checked selected-batch replay and source reprocessing | Keep all accepted batches; preserve original bytes and hashes. A checkpoint is not a replacement. |
| Current silver customer table's historical observation rows | Reconstruct customer attribute periods after a late full snapshot | Keep all observations, including unchanged intermediate batches. Do not filter this table down to the latest batch or current customer state. |
| Other bronze/silver batch rows and batch gold | Existing per-batch verification and audit | Keep accepted batches. This contract does not implement their reconstruction after deletion. |
| Current customer SCD2 and current gold | Serve and verify the published state | Preserve the warehouse and catalog together. Current rows alone cannot reconstruct the source observations. |
| Checkpoint, adjacent active intent and runner journal | Distinguish completed work from unfinished publication and choose a safe retry | Preserve together with the source and warehouse; never reset completion to manufacture replay. The journal is diagnostic evidence, not authoritative business history. |
| Historical Iceberg snapshots and referenced files | Time travel and rollback within the actually retained snapshot set | Separate maintenance policy; expiring them ends those historical reads. It does not delete rows visible in the current table snapshot. |
| Noncurrent S3 versions and delete markers | Object-store accounting and potential operator recovery input | Listing versions proves neither a usable catalog backup nor a successful restore. Do not treat them as a tested backup. |

The [rebuild entry point](../../jobs/spark/commerce_customer_scd2.py) reads
`spark.table("lakehouse.silver.commerce_customers")` without a time-travel selector.
Rebuild derives periods from that current table's observations, including older
`source_batch_at` values. Expiring old Iceberg snapshots can therefore preserve the
rebuild input, whereas deleting older silver rows can destroy it even when many
Iceberg snapshots remain. Equal adjacent attributes collapse into one period;
removing an intermediate changed observation can silently join two periods.

The existing coverage guards reject missing customers and a source starting after
the earliest published version. They cannot detect every missing intermediate
observation. A successful rebuild, unchanged row count, or earliest timestamp check
is not proof that an operator's shortened source is complete. See the
[history rebuild](commerce-customer-history-rebuild.md) and
[late correction](commerce-late-customer-correction.md) runbooks for the implemented
semantics and literal SQL acceptance.

## Before maintenance

1. Establish exclusive writer ownership for the intended warehouse, catalog and
   checkpoint. Resolve any unfinished active intent using the normal runner before
   planning maintenance. Record the current source manifest inventory and accepted
   batch IDs with their original checksums; investigate missing or changed sources.
2. Preserve the checkpoint, intent when present, journal, and a recoverable copy of
   the warehouse/catalog outside the objects targeted for deletion. Record where
   that backup is and the result of a restore rehearsal. A listing report alone
   does not satisfy this requirement.
3. Record current table snapshot IDs, row and customer-period SQL oracles, and the
   latest successful replay/late-correction acceptance evidence. Check the silver
   observations against the retained accepted source batches. The code does not
   issue a complete-history certificate, so unresolved gaps block a destructive
   retention change.
4. Use the existing guarded maintenance plan and its dry-run on that same table
   state. Review the exact expired snapshot IDs or approved orphan paths. Preserve
   the plan and reports outside the targeted prefix. Never derive a delete list
   from the difference between an S3 listing and selected table references.
5. After an approved operation, verify the current snapshot/rows, the promised
   retained historical reads, and the relevant replay/correction oracle. If they
   differ, stop further maintenance and use the tested recovery procedure; do not
   change the checkpoint to hide the mismatch.

The existing [snapshot expiration](iceberg-snapshot-expiration.md) defaults are
168 hours, at least three retained snapshots, and at most 50 expirations per plan.
Those numbers bound that action; they do not promise seven days of source replay
or customer correction. Actual retained IDs and references determine time travel.
[Orphan removal](iceberg-orphan-inventory.md) requires its separate inspection,
exact candidate approvals and revalidation. Its age cutoff is not permission to
remove business-history rows, landing objects, or arbitrary noncurrent S3 versions.
[Data-file rewrite](iceberg-data-file-rewrite.md) preserves logical rows and can
leave earlier files reachable until separately approved snapshot expiration.

## Introducing a finite business window

A shorter window is a future data-contract change, not a maintenance flag. Before
enabling it, define the earliest accepted correction time and replay batch, the
handling of older arrivals, and a versioned baseline containing sufficient prior
customer state. Demonstrate reconstruction across the boundary, a changed
intermediate observation, return to an earlier state, unchanged-state replay, and
the next ordinary batch with literal SQL oracles. Test restoring the baseline,
sources, catalog, checkpoint and intent together before deleting any required input.

Until that baseline and boundary behavior exist and are verified, preserve all
accepted source/silver history. This document does not implement row deletion,
automatic backup, retention-gap detection, concurrent-writer coordination or
distributed exactly-once processing.
