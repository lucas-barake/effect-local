import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as SchemaDescriptor from "@lucas-barake/effect-local/SchemaDescriptor"
import * as Arr from "effect/Array"
import * as Order from "effect/Order"
import * as Rpc from "effect/rpc/Rpc"
import * as RpcGroup from "effect/rpc/RpcGroup"
import * as RpcSchema from "effect/rpc/RpcSchema"
import * as Schema from "effect/Schema"
import * as SyncRpc from "../src/SyncRpc.js"

const wireFingerprints: { readonly [protocolVersion: number]: string } = {
  1: "b888f3e127cfc6fd"
}

const describeSchema = (schema: Schema.Top) => SchemaDescriptor.make(schema, { includeConstructorDefaults: false })

const describeRpc = (rpc: Rpc.AnyWithProps) => {
  const success = rpc.successSchema
  const contract = {
    tag: rpc._tag,
    payload: describeSchema(rpc.payloadSchema),
    error: describeSchema(rpc.errorSchema),
    defect: describeSchema(rpc.defectSchema),
    middlewareErrors: Array.from(rpc.middlewares, (middleware) => describeSchema(middleware.error))
  }
  if (RpcSchema.isStreamSchema(success)) {
    return {
      ...contract,
      stream: true,
      success: describeSchema(success.success),
      streamError: describeSchema(success.error)
    }
  }
  return { ...contract, stream: false, success: describeSchema(success), streamError: null }
}

const fingerprint = (requests: ReadonlyMap<string, Rpc.AnyWithProps>): string => {
  const contracts = Array.from(requests.values(), describeRpc)
  return Canonical.hash(Arr.sortWith(contracts, (contract) => contract.tag, Order.String))
}

class Failed extends Schema.TaggedError<Failed>("test/Failed")("Failed", {
  resource: Schema.Literals(["a", "b"]),
  detail: Schema.optionalKey(Schema.String)
}) {}

class FailedWithWiderLiteral extends Schema.TaggedError<FailedWithWiderLiteral>("test/Failed")("Failed", {
  resource: Schema.Literals(["a", "b", "c"]),
  detail: Schema.optionalKey(Schema.String)
}) {}

class FailedWithRequiredKey extends Schema.TaggedError<FailedWithRequiredKey>("test/Failed")("Failed", {
  resource: Schema.Literals(["a", "b"]),
  detail: Schema.String
}) {}

class FailedWithOtherTag extends Schema.TaggedError<FailedWithOtherTag>("test/Failed")("Failure", {
  resource: Schema.Literals(["a", "b"]),
  detail: Schema.optionalKey(Schema.String)
}) {}

class Other extends Schema.TaggedError<Other>("test/Other")("Other", {}) {}

const payload = { id: Schema.String }
const base = { payload, success: Schema.Number, error: Failed }

const fingerprintOf = (rpc: Rpc.AnyWithProps): string => fingerprint(RpcGroup.make(rpc).requests)

describe("sync wire contract", () => {
  it("fingerprints every part of an rpc that a peer has to decode", () => {
    const variants = [
      Rpc.make("Call", base),
      Rpc.make("Renamed", base),
      Rpc.make("Call", { ...base, payload: { id: Schema.Number } }),
      Rpc.make("Call", { ...base, payload: { ...payload, extra: Schema.String } }),
      Rpc.make("Call", { ...base, payload: { ...payload, extra: Schema.optionalKey(Schema.String) } }),
      Rpc.make("Call", { ...base, success: Schema.NullOr(Schema.Number) }),
      Rpc.make("Call", { ...base, stream: true }),
      Rpc.make("Call", { ...base, stream: true, success: Schema.String }),
      Rpc.make("Call", { ...base, stream: true, error: FailedWithRequiredKey }),
      Rpc.make("Call", { ...base, defect: Schema.Defect({ includeStack: true }) }),
      Rpc.make("Call", { ...base, error: FailedWithWiderLiteral }),
      Rpc.make("Call", { ...base, error: FailedWithRequiredKey }),
      Rpc.make("Call", { ...base, error: FailedWithOtherTag }),
      Rpc.make("Call", { ...base, error: Schema.Union([Failed, Other]) })
    ]
    const fingerprints = variants.map(fingerprintOf)
    assert.strictEqual(new Set(fingerprints).size, variants.length)
    const rebuilt = Rpc.make("Call", base)
    assert.strictEqual(fingerprintOf(rebuilt), fingerprints[0])
  })

  it("changes the wire schemas only together with the protocol version", () => {
    const version = Protocol.currentProtocolVersion
    const actual = fingerprint(SyncRpc.Rpcs.requests)
    const pinned = wireFingerprints[version]
    if (pinned === undefined) {
      assert.fail(
        `No wire fingerprint is pinned for protocol version ${version}. ` +
          `Add { ${version}: "${actual}" } to wireFingerprints in packages/local-rpc/test/WireContract.test.ts.`
      )
    }
    assert.strictEqual(
      actual,
      pinned,
      `A schema that crosses the sync wire changed while Protocol.currentProtocolVersion is still ${version}. ` +
        `Peers that negotiated version ${version} cannot decode the new shape, and RpcClient reports that as a defect. ` +
        "If the change is unintended, revert it. If it is intended, bump currentProtocolVersion in " +
        `packages/local/src/Protocol.ts and pin { [the new version]: "${actual}" } in wireFingerprints in ` +
        "packages/local-rpc/test/WireContract.test.ts."
    )
  })
})
