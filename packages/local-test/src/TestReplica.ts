import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import type * as Definition from "@lucas-barake/effect-local/Definition"

export const layer = <D extends Definition.Any,>(options: SqlReplica.Options<D>) => SqlReplica.layer(options)
