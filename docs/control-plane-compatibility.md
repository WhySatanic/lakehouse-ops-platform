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

The command exits non-zero if a baseline command or long option no longer exists, an
output references an unknown producer, or an output leaves schema major `1`. New
commands and options are compatible additions and do not require consumers to upgrade.

## CLI policy

Command names and documented long options in contract `1.0.0` are stable. Removing or
renaming one, changing its meaning, or making an optional argument required is a
breaking change. A replacement must first ship additively, remain available for at
least one minor release with a migration note, and only be removed in a new product
major release.

## JSON policy

Every public report carries a `schema_version`. Within major `1`, producers may add
fields and enum values. Consumers must ignore fields they do not understand. Producers
must not remove or rename fields, change their types or meaning, or make previously
valid values invalid. Such changes require a new schema major and a product major
release. Existing producer tests remain the executable field and type specification;
the compatibility gate prevents an unnoticed major-version escape.

The contract digest in the verifier output identifies the exact baseline used by CI.
Release evidence should record this digest so a report can be tied to the supported
surface.

## Upgrade notes

Version `0.47.0` adds the verifier and freezes contract `1.0.0`. It does not remove or
rename any existing command, option, or JSON field. Automation should run the verifier
before deployment and retain its JSON output with other release evidence.
