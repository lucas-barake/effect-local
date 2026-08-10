import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"

export const layer = <D extends Definition.Any,>(options: {
  readonly definition: D
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
}) => SqlReplica.layer(options)
