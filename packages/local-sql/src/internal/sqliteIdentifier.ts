import type * as SqlClient from "effect/sql/SqlClient"

export const random = (sql: SqlClient.SqlClient, prefix: "cli" | "inc") =>
  sql.literal(
    `('${prefix}_' || lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
      substr(lower(hex(randomblob(2))), 2) || '-' ||
      substr('89ab', abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' ||
      lower(hex(randomblob(6))))`
  )
