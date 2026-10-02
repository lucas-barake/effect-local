---
"@lucas-barake/effect-local": minor
"@lucas-barake/effect-local-sql": minor
---

Add `Migrations.renderServer`, which reads a server database without writing and returns the exact SQL script that
brings it to the schema the code expects, and a verify-only `migration: { mode: "verify" }` for `ServerStore` that
refuses to serve with `StorageMigrationPending` while the database is behind.
