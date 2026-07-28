import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"

/**
 * The replica's identity row is gone.
 *
 * Every module that reads `effect_local_metadata WHERE singleton = 1` can observe this, and they must
 * all report it the same way: it is replica wide, unlike `StorageCorrupt`, which means one document's
 * stored bytes are unusable and which consumers act on per document. Before this was shared, whichever
 * module happened to read the row first decided how the replica described the condition.
 *
 * A function rather than a shared instance, so each failure captures its own stack at the site that
 * observed it. `operation` names that site, because the reason carries no cause of its own: nothing
 * failed, a row that must exist is simply absent.
 */
export const metadataMissing = (operation: string): ReplicaError.ReplicaError =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ReplicaMetadataMissing({ operation })
  })
