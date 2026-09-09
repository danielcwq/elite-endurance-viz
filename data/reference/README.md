# Reference data

Reference files persist decisions that must remain stable across canonical rebuilds.

- `athlete_registry_2024.csv` assigns opaque internal UUIDs to canonical athletes. Names are attributes and matching keys, not identifiers.
- `athlete_external_accounts_2024.csv` maps provider accounts to internal UUIDs with matching evidence.
- `identity_resolution_report_2024.json` records conflicts, exclusions, and resolution counts.

The bootstrap command refuses to overwrite these files. Changes require an explicit reviewed migration, not regeneration in place.
