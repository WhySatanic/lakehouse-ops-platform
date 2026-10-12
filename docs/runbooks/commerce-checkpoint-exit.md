# Exit after checkpoint persistence

An owner can exit after the processing checkpoint is atomically written but before
the diagnostic journal records checkpoint completion or a ready outcome. An unfinished
journal alone does not mean that the batch needs to be replayed.

The existing private read-only inspector can now correlate recent attempts with a
processing checkpoint:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_journal_inspection --journal artifacts/owner/journal.sqlite3 --checkpoint artifacts/owner/checkpoint.json
```

Use actual retained paths. Without `--checkpoint`, the existing result is unchanged.
With it, each attempt retains its original outcome and gains `checkpoint_observation`:

- `matching_manifest`: the checkpoint contains the same batch ID and manifest digest.
- `different_manifest`: the batch ID exists but its digest differs; investigate identity.
- `not_recorded`: the selected checkpoint has no entry, including a missing file.
- `not_batch_bound`: the attempt has no usable batch identity.

The inspector never creates a missing checkpoint, modifies journal events, repairs
state, executes stages or instructs an automatic replay. Matching identity does not
prove current gold correctness or recover a failed transaction. Journal and checkpoint
can match because a later retry wrote the checkpoint; this does not attribute the
commit to a particular run or change an interrupted attempt's outcome. Both files
are read separately, not as a distributed or atomic snapshot; inspect with no active
writer and verify warehouse correctness independently. The checkpoint remains the
planner's processing authority, not a successful-looking diagnostic event.

## Executable boundary acceptance

```powershell
uv run --frozen pytest --no-cov -q tests/test_commerce_checkpoint_exit.py
uv run --frozen python -m lakehouse_ops.local_quality
```

The acceptance launches a real Python owner holding the local runner lock. It writes
`checkpoint/started` to the real SQLite journal, uses the real planner's atomic
checkpoint persistence, then terminates through `os._exit(23)` before journal completion.
A new process observes unfinished evidence with matching checkpoint identity. The
released OS lock can be acquired, incremental planning selects zero batches, and a
duplicate commit reports `created=False`, leaving journal/checkpoint bytes unchanged.

The discovered batch is a controlled in-memory fixture. This test exercises process
exit, local persistence and lock release, **not** S3 discovery, Spark execution,
Iceberg transactions, power loss or proof of filesystem durability across host failure.
Separate real-stack owner-exit and linked scenarios remain mandatory integration paths.
This is a private diagnostic extension, not a stable public contract or release change.
