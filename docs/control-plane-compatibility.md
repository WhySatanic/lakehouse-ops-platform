# Control-plane compatibility

The public control plane is the `lakeops` command surface and its versioned JSON
reports. The baseline is machine-readable in
[`config/control-plane/contract.json`](../config/control-plane/contract.json) and is
checked on every pull request.

Run the same gate locally:

```bash
uv run lakeops verify-control-plane-contract \
  --contract config/control-plane/contract.json
```

The command exits non-zero if a baseline command or long option no longer exists, a
frozen option changes its required flag, type, default, or choices, an output references
an unknown producer, an output leaves schema major `1`, or its declared `schema_path` is
missing, does not resolve to a file, contains invalid JSON, or fails Draft 2020-12
metaschema validation. Absolute schema paths and references that resolve outside the
contract directory are also rejected. Each schema must require `schema_version` and bind
it with a `const` equal to the output version declared by the contract. Schemas must also
declare the Draft 2020-12 dialect and a unique non-empty `$id`. New commands and options
are compatible additions and do not require consumers to upgrade. `$ref` and
`$dynamicRef` values must be local fragments so verification never depends on a remote
schema registry or network availability. Every fragment must resolve to a bundled JSON
Pointer or anchor; dangling references fail the compatibility gate before runtime.

## CLI policy

Command names and documented long options in contract `1.0.0` are stable. Removing or
renaming one, changing its meaning, or making an optional argument required is a
breaking change. A replacement must first ship additively, remain available for at
least one minor release with a migration note, and only be removed in a new product
major release.

The machine-readable `option_semantics` baseline covers at least one critical option
for every public command. Each entry freezes whether the option is required, its parsed
type, its default, and its allowed choices. Environment-backed defaults are deliberately
excluded because deployment configuration is expected to vary; their option names and
documented meaning remain stable under the same policy.

## JSON policy

Every public report carries a `schema_version`. Within major `1`, producers may add
fields and enum values. Consumers must ignore fields they do not understand. Producers
must not remove or rename fields, change their types or meaning, or make previously
valid values invalid. Such changes require a new schema major and a product major
release. Existing producer tests remain the executable field and type specification;
the compatibility gate prevents an unnoticed major-version escape. Producers can also
declare a Draft 2020-12 JSON Schema through `schema_path`. The control-plane, image-lock,
release-readiness, release-candidate, Iceberg metadata, maintenance-plan, Trino baseline,
Trino compaction-comparison, partition-pruning, sort-order, and Ranger policy-sync
producers validate their emitted reports this way. Every declared public output now has
a standalone schema and runtime producer validation.

The contract digest in the verifier output identifies the exact baseline used by CI.
Release evidence should record this digest so a report can be tied to the supported
surface.

## Upgrade notes

Version `1.14.7` resolves every local `$ref` and `$dynamicRef` in each public output schema
and rejects missing JSON Pointers or anchors. It preserves contract `1.0.0`, report shapes,
and producer behavior. Custom contract extensions must bundle definitions locally and keep
all fragments resolvable.
