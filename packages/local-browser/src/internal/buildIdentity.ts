import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Runners from "effect/cluster/Runners"
import type * as Rpc from "effect/rpc/Rpc"
import * as RpcSchema from "effect/rpc/RpcSchema"
import * as Schema from "effect/Schema"
import * as replicaWire from "./replicaWire.js"
import * as TabTransport from "./tabTransport.js"

export interface BuildIdentity {
  readonly version: number
  readonly fingerprint: string
}

export interface Options {
  readonly definition: Definition.Any
  readonly ephemerals: ReadonlyArray<Ephemeral.Any>
  readonly profiles: ReadonlyMap<string, Ephemeral.AnyMember>
}

const wireProtocolRevision = 1

const document = (schema: Schema.Constraint) => Schema.toJsonSchemaDocument(schema)

const rpcDocument = (rpc: Rpc.AnyWithProps) => {
  const success = rpc.successSchema
  if (RpcSchema.isStreamSchema(success)) {
    return {
      tag: rpc._tag,
      payload: document(rpc.payloadSchema),
      success: document(success.success),
      error: document(rpc.errorSchema),
      streamError: document(success.error)
    }
  }
  return {
    tag: rpc._tag,
    payload: document(rpc.payloadSchema),
    success: document(success),
    error: document(rpc.errorSchema),
    streamError: null
  }
}

const rpcDocuments = (requests: ReadonlyMap<string, Rpc.AnyWithProps>) => Array.from(requests.values(), rpcDocument)

const wireFingerprint = Canonical.hash({
  revision: wireProtocolRevision,
  replica: rpcDocuments(replicaWire.ReplicaRpcs.requests),
  runners: rpcDocuments(Runners.Rpcs.requests),
  frame: document(TabTransport.Frame)
})

const byName = <A extends { readonly name: string },>(left: A, right: A): number => {
  if (left.name < right.name) return -1
  if (left.name > right.name) return 1
  return 0
}

const ephemeralDocument = (ephemeral: Ephemeral.Any) => {
  if (ephemeral.kind === "event") {
    return { name: ephemeral.name, kind: ephemeral.kind, payload: document(ephemeral.payloadSchema), key: null }
  }
  return {
    name: ephemeral.name,
    kind: ephemeral.kind,
    payload: document(ephemeral.payloadSchema),
    key: document(ephemeral.keySchema)
  }
}

export const make = (options: Options): BuildIdentity => ({
  version: options.definition.version,
  fingerprint: Canonical.hash({
    wire: wireFingerprint,
    definition: options.definition.hash,
    ephemerals: options.ephemerals.map(ephemeralDocument).sort(byName),
    profiles: Array.from(options.profiles, ([name, profile]) => ({ name, payload: document(profile.payloadSchema) }))
      .sort(byName)
  })
})
