import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

export const RestoreNonce = Schema.String.check(
  Schema.isPattern(/^rst_[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
).pipe(Schema.brand("@lucas-barake/effect-local-browser/RestoreNonce"))
export type RestoreNonce = typeof RestoreNonce.Type

export const RestoreSequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
  Schema.brand("@lucas-barake/effect-local-browser/RestoreSequence")
)
export type RestoreSequence = typeof RestoreSequence.Type

export const BoundedErrorDescription = Schema.Struct({
  name: Schema.String,
  message: Schema.String
})
export type BoundedErrorDescription = typeof BoundedErrorDescription.Type

const DocumentNotFound = Schema.TaggedStruct("DocumentNotFound", {
  documentId: Identity.DocumentId
})
const DocumentDecodeError = Schema.TaggedStruct("DocumentDecodeError", {
  documentId: Identity.DocumentId,
  cause: BoundedErrorDescription
})
const DocumentEncodeError = Schema.TaggedStruct("DocumentEncodeError", {
  documentId: Identity.DocumentId,
  cause: BoundedErrorDescription
})
const UnsupportedDocumentVersion = Schema.TaggedStruct("UnsupportedDocumentVersion", {
  documentId: Identity.DocumentId,
  observedVersion: Schema.Int,
  supportedVersion: Schema.Int
})
const ProjectionBlocked = Schema.TaggedStruct("ProjectionBlocked", {
  projection: Schema.String,
  cause: BoundedErrorDescription
})
const CommandIdConflict = Schema.TaggedStruct("CommandIdConflict", {
  commandId: Identity.CommandId
})
const ReceiptOperationMismatch = Schema.TaggedStruct("ReceiptOperationMismatch", {
  commandId: Identity.CommandId,
  expected: Schema.String,
  observed: Schema.String
})
const StorageUnavailable = Schema.TaggedStruct("StorageUnavailable", {
  cause: BoundedErrorDescription
})
const CanonicalEncodeError = Schema.TaggedStruct("CanonicalEncodeError", {
  cause: BoundedErrorDescription
})
const StorageCorrupt = Schema.TaggedStruct("StorageCorrupt", {
  cause: BoundedErrorDescription
})
const QuotaExceeded = Schema.TaggedStruct("QuotaExceeded", {
  resource: Schema.String,
  limit: Schema.Int
})
const MigrationFailed = Schema.TaggedStruct("MigrationFailed", {
  migration: Schema.String,
  cause: BoundedErrorDescription
})
const BackupInvalid = Schema.TaggedStruct("BackupInvalid", {
  cause: BoundedErrorDescription
})
const BackupTooLarge = Schema.TaggedStruct("BackupTooLarge", {
  limit: Schema.Int,
  observed: Schema.Int
})
const RestoreBusy = Schema.TaggedStruct("RestoreBusy", {
  replica: Schema.String
})
const RestoreFailed = Schema.TaggedStruct("RestoreFailed", {
  cause: BoundedErrorDescription
})
const ProtocolMismatch = Schema.TaggedStruct("ProtocolMismatch", {
  expected: Schema.String,
  observed: Schema.String
})
const ReplicaFenced = Schema.TaggedStruct("ReplicaFenced", {
  expectedGeneration: Identity.WriterGeneration,
  observedGeneration: Identity.WriterGeneration
})
const OperationTimeout = Schema.TaggedStruct("OperationTimeout", {
  operation: Schema.String,
  timeoutMillis: Schema.Int
})

export const RestoreWireError = Schema.Union([
  DocumentNotFound,
  DocumentDecodeError,
  DocumentEncodeError,
  UnsupportedDocumentVersion,
  ProjectionBlocked,
  CommandIdConflict,
  ReceiptOperationMismatch,
  StorageUnavailable,
  CanonicalEncodeError,
  StorageCorrupt,
  QuotaExceeded,
  MigrationFailed,
  BackupInvalid,
  BackupTooLarge,
  RestoreBusy,
  RestoreFailed,
  ProtocolMismatch,
  ReplicaFenced,
  OperationTimeout
])
export type RestoreWireError = typeof RestoreWireError.Type

const fields = {
  nonce: RestoreNonce,
  sequence: RestoreSequence
}

export const Start = Schema.TaggedStruct("Start", {
  nonce: RestoreNonce,
  sequence: Schema.Literal(0)
})
export type Start = typeof Start.Type

export const Pull = Schema.TaggedStruct("Pull", fields)
export type Pull = typeof Pull.Type

export const Chunk = Schema.TaggedStruct("Chunk", {
  ...fields,
  bytes: Schema.Uint8Array
})
export type Chunk = typeof Chunk.Type

export const End = Schema.TaggedStruct("End", fields)
export type End = typeof End.Type

export const SourceFailure = Schema.TaggedStruct("SourceFailure", {
  ...fields,
  error: RestoreWireError
})
export type SourceFailure = typeof SourceFailure.Type

export const TerminalSuccess = Schema.TaggedStruct("TerminalSuccess", fields)
export type TerminalSuccess = typeof TerminalSuccess.Type

export const TerminalSessionFailure = Schema.TaggedStruct("TerminalSessionFailure", {
  ...fields,
  error: RestoreWireError
})
export type TerminalSessionFailure = typeof TerminalSessionFailure.Type

export const TerminalRestoreFailure = Schema.TaggedStruct("TerminalRestoreFailure", {
  ...fields,
  error: RestoreWireError
})
export type TerminalRestoreFailure = typeof TerminalRestoreFailure.Type

export const TerminalDefect = Schema.TaggedStruct("TerminalDefect", {
  ...fields,
  defect: BoundedErrorDescription
})
export type TerminalDefect = typeof TerminalDefect.Type

export const TerminalAck = Schema.TaggedStruct("TerminalAck", fields)
export type TerminalAck = typeof TerminalAck.Type

export const Released = Schema.TaggedStruct("Released", fields)
export type Released = typeof Released.Type

export const ReleasedAck = Schema.TaggedStruct("ReleasedAck", fields)
export type ReleasedAck = typeof ReleasedAck.Type

export const PageToOwnerFrame = Schema.Union([
  Start,
  Chunk,
  End,
  SourceFailure,
  TerminalAck,
  ReleasedAck
])
export type PageToOwnerFrame = typeof PageToOwnerFrame.Type

export const OwnerToPageFrame = Schema.Union([
  Pull,
  TerminalSuccess,
  TerminalSessionFailure,
  TerminalRestoreFailure,
  TerminalDefect,
  Released
])
export type OwnerToPageFrame = typeof OwnerToPageFrame.Type

export const RestoreFrame = Schema.Union([PageToOwnerFrame, OwnerToPageFrame])
export type RestoreFrame = typeof RestoreFrame.Type

const encoder = new TextEncoder()
const utf8Bytes = (value: string): number => encoder.encode(value).byteLength

const truncateUtf8 = (value: string, maxBytes: number): string => {
  if (maxBytes <= 0) return ""
  if (encoder.encode(value).byteLength <= maxBytes) return value
  let result = ""
  for (const character of value) {
    if (encoder.encode(result + character).byteLength > maxBytes) break
    result += character
  }
  return result
}

const safeString = (value: unknown, fallback: string): string => {
  try {
    return typeof value === "string" ? value : String(value)
  } catch {
    return fallback
  }
}

const emptyErrorDescription: BoundedErrorDescription = { name: "", message: "" }

const payloadStringBytes = (value: unknown): number => {
  if (typeof value === "string") return utf8Bytes(value)
  if (typeof value !== "object" || value === null) return 0
  let total = 0
  for (const [key, field] of Object.entries(value)) {
    total += utf8Bytes(key) + payloadStringBytes(field)
  }
  return total
}

const encodeDefectText = (defect: unknown, maxBytes: number): BoundedErrorDescription => {
  let name = "Error"
  let message = "Unknown failure"
  try {
    if (typeof defect === "object" && defect !== null) {
      name = safeString(Reflect.get(defect, "name"), "Error")
      message = safeString(Reflect.get(defect, "message"), "Unknown failure")
    } else {
      message = safeString(defect, "Unknown failure")
    }
  } catch {
    name = "Error"
    message = "Unknown failure"
  }
  const nameBudget = Math.min(128, Math.max(0, Math.floor(maxBytes / 4)))
  const boundedName = truncateUtf8(name, nameBudget)
  const remaining = Math.max(0, maxBytes - utf8Bytes(boundedName))
  return {
    name: boundedName,
    message: truncateUtf8(message, remaining)
  }
}

export const encodeDefect = (defect: unknown, maxBytes: number): BoundedErrorDescription =>
  encodeDefectText(
    defect,
    Math.max(0, maxBytes - payloadStringBytes(emptyErrorDescription))
  )

export const decodeDefect = (description: BoundedErrorDescription): Error => {
  const error = new Error(description.message)
  error.name = description.name
  return error
}

const bounded = (value: string, maxBytes: number) => truncateUtf8(value, maxBytes)

interface ErrorBudget {
  readonly text: (value: string) => string
  readonly defect: (value: unknown) => BoundedErrorDescription
}

const encodeWithinBudget = (
  maxBytes: number,
  empty: RestoreWireError,
  encode: (budget: ErrorBudget) => RestoreWireError
): RestoreWireError => {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
    throw new RangeError("maxBytes must be a positive safe integer")
  }
  const fixedBytes = payloadStringBytes(empty)
  if (fixedBytes > maxBytes) {
    throw new RangeError("maxBytes cannot preserve the restore error shape")
  }
  let remaining = maxBytes - fixedBytes
  const text = (value: string): string => {
    const result = bounded(value, remaining)
    remaining -= utf8Bytes(result)
    return result
  }
  const defect = (value: unknown): BoundedErrorDescription => {
    const result = encodeDefectText(value, remaining)
    remaining -= utf8Bytes(result.name) + utf8Bytes(result.message)
    return result
  }
  const encoded = encode({ text, defect })
  if (!preflight(encoded, maxBytes)) {
    throw new RangeError("encoded restore error exceeds maxBytes")
  }
  return encoded
}

export const encodeReplicaError = (
  error: ReplicaError.ReplicaError,
  maxBytes: number
): RestoreWireError => {
  const reason = error.reason
  switch (reason._tag) {
    case "DocumentNotFound":
      return encodeWithinBudget(
        maxBytes,
        { _tag: reason._tag, documentId: reason.documentId },
        () => ({ _tag: reason._tag, documentId: reason.documentId })
      )
    case "DocumentDecodeError":
    case "DocumentEncodeError":
      return encodeWithinBudget(
        maxBytes,
        {
          _tag: reason._tag,
          documentId: reason.documentId,
          cause: emptyErrorDescription
        },
        ({ defect }) => ({
          _tag: reason._tag,
          documentId: reason.documentId,
          cause: defect(reason.cause)
        })
      )
    case "UnsupportedDocumentVersion":
      return encodeWithinBudget(
        maxBytes,
        {
          _tag: reason._tag,
          documentId: reason.documentId,
          observedVersion: reason.observedVersion,
          supportedVersion: reason.supportedVersion
        },
        () => ({
          _tag: reason._tag,
          documentId: reason.documentId,
          observedVersion: reason.observedVersion,
          supportedVersion: reason.supportedVersion
        })
      )
    case "ProjectionBlocked":
      return encodeWithinBudget(
        maxBytes,
        {
          _tag: reason._tag,
          projection: "",
          cause: emptyErrorDescription
        },
        ({ defect, text }) => ({
          _tag: reason._tag,
          projection: text(reason.projection),
          cause: defect(reason.cause)
        })
      )
    case "CommandIdConflict":
      return encodeWithinBudget(
        maxBytes,
        { _tag: reason._tag, commandId: reason.commandId },
        () => ({ _tag: reason._tag, commandId: reason.commandId })
      )
    case "ReceiptOperationMismatch":
      return encodeWithinBudget(
        maxBytes,
        {
          _tag: reason._tag,
          commandId: reason.commandId,
          expected: "",
          observed: ""
        },
        ({ text }) => ({
          _tag: reason._tag,
          commandId: reason.commandId,
          expected: text(reason.expected),
          observed: text(reason.observed)
        })
      )
    case "StorageUnavailable":
    case "CanonicalEncodeError":
    case "StorageCorrupt":
    case "BackupInvalid":
    case "RestoreFailed":
      return encodeWithinBudget(
        maxBytes,
        { _tag: reason._tag, cause: emptyErrorDescription },
        ({ defect }) => ({ _tag: reason._tag, cause: defect(reason.cause) })
      )
    case "QuotaExceeded":
      return encodeWithinBudget(
        maxBytes,
        { _tag: reason._tag, resource: "", limit: reason.limit },
        ({ text }) => ({ _tag: reason._tag, resource: text(reason.resource), limit: reason.limit })
      )
    case "MigrationFailed":
      return encodeWithinBudget(
        maxBytes,
        {
          _tag: reason._tag,
          migration: "",
          cause: emptyErrorDescription
        },
        ({ defect, text }) => ({
          _tag: reason._tag,
          migration: text(reason.migration),
          cause: defect(reason.cause)
        })
      )
    case "BackupTooLarge":
      return encodeWithinBudget(
        maxBytes,
        { _tag: reason._tag, limit: reason.limit, observed: reason.observed },
        () => ({ _tag: reason._tag, limit: reason.limit, observed: reason.observed })
      )
    case "RestoreBusy":
      return encodeWithinBudget(
        maxBytes,
        { _tag: reason._tag, replica: "" },
        ({ text }) => ({ _tag: reason._tag, replica: text(reason.replica) })
      )
    case "ProtocolMismatch":
      return encodeWithinBudget(
        maxBytes,
        { _tag: reason._tag, expected: "", observed: "" },
        ({ text }) => ({
          _tag: reason._tag,
          expected: text(reason.expected),
          observed: text(reason.observed)
        })
      )
    case "ReplicaFenced":
      return encodeWithinBudget(
        maxBytes,
        {
          _tag: reason._tag,
          expectedGeneration: reason.expectedGeneration,
          observedGeneration: reason.observedGeneration
        },
        () => ({
          _tag: reason._tag,
          expectedGeneration: reason.expectedGeneration,
          observedGeneration: reason.observedGeneration
        })
      )
    case "OperationTimeout":
      return encodeWithinBudget(
        maxBytes,
        { _tag: reason._tag, operation: "", timeoutMillis: reason.timeoutMillis },
        ({ text }) => ({
          _tag: reason._tag,
          operation: text(reason.operation),
          timeoutMillis: reason.timeoutMillis
        })
      )
  }
}

export const replicaErrorFromWire = (wire: RestoreWireError): ReplicaError.ReplicaError => {
  let reason: ReplicaError.Reason
  switch (wire._tag) {
    case "DocumentNotFound":
      reason = new ReplicaError.DocumentNotFound({ documentId: wire.documentId })
      break
    case "DocumentDecodeError":
      reason = new ReplicaError.DocumentDecodeError({
        documentId: wire.documentId,
        cause: decodeDefect(wire.cause)
      })
      break
    case "DocumentEncodeError":
      reason = new ReplicaError.DocumentEncodeError({
        documentId: wire.documentId,
        cause: decodeDefect(wire.cause)
      })
      break
    case "UnsupportedDocumentVersion":
      reason = new ReplicaError.UnsupportedDocumentVersion({
        documentId: wire.documentId,
        observedVersion: wire.observedVersion,
        supportedVersion: wire.supportedVersion
      })
      break
    case "ProjectionBlocked":
      reason = new ReplicaError.ProjectionBlocked({
        projection: wire.projection,
        cause: decodeDefect(wire.cause)
      })
      break
    case "CommandIdConflict":
      reason = new ReplicaError.CommandIdConflict({ commandId: wire.commandId })
      break
    case "ReceiptOperationMismatch":
      reason = new ReplicaError.ReceiptOperationMismatch({
        commandId: wire.commandId,
        expected: wire.expected,
        observed: wire.observed
      })
      break
    case "StorageUnavailable":
      reason = new ReplicaError.StorageUnavailable({ cause: decodeDefect(wire.cause) })
      break
    case "CanonicalEncodeError":
      reason = new ReplicaError.CanonicalEncodeError({ cause: decodeDefect(wire.cause) })
      break
    case "StorageCorrupt":
      reason = new ReplicaError.StorageCorrupt({ cause: decodeDefect(wire.cause) })
      break
    case "QuotaExceeded":
      reason = new ReplicaError.QuotaExceeded({ resource: wire.resource, limit: wire.limit })
      break
    case "MigrationFailed":
      reason = new ReplicaError.MigrationFailed({
        migration: wire.migration,
        cause: decodeDefect(wire.cause)
      })
      break
    case "BackupInvalid":
      reason = new ReplicaError.BackupInvalid({ cause: decodeDefect(wire.cause) })
      break
    case "BackupTooLarge":
      reason = new ReplicaError.BackupTooLarge({ limit: wire.limit, observed: wire.observed })
      break
    case "RestoreBusy":
      reason = new ReplicaError.RestoreBusy({ replica: wire.replica })
      break
    case "RestoreFailed":
      reason = new ReplicaError.RestoreFailed({ cause: decodeDefect(wire.cause) })
      break
    case "ProtocolMismatch":
      reason = new ReplicaError.ProtocolMismatch({ expected: wire.expected, observed: wire.observed })
      break
    case "ReplicaFenced":
      reason = new ReplicaError.ReplicaFenced({
        expectedGeneration: wire.expectedGeneration,
        observedGeneration: wire.observedGeneration
      })
      break
    case "OperationTimeout":
      reason = new ReplicaError.OperationTimeout({
        operation: wire.operation,
        timeoutMillis: wire.timeoutMillis
      })
      break
  }
  return new ReplicaError.ReplicaError({ reason })
}

export const decodeReplicaError = (
  wire: RestoreWireError
): Effect.Effect<never, ReplicaError.ReplicaError> => Effect.fail(replicaErrorFromWire(wire))

export const preflight = (
  input: unknown,
  maxStringBytes: number
): boolean => {
  const pending: Array<readonly [unknown, number]> = [[input, 0]]
  const seen = new Set<object>()
  const encoder = new TextEncoder()
  let nodes = 0
  let stringBytes = 0
  try {
    while (pending.length > 0) {
      const [value, depth] = pending.pop()!
      nodes += 1
      if (nodes > 64 || depth > 8) return false
      if (typeof value === "string") {
        const remaining = maxStringBytes - stringBytes
        if (value.length > remaining) return false
        stringBytes += encoder.encode(value).byteLength
        if (stringBytes > maxStringBytes) return false
      } else if (
        value === null ||
        value === undefined ||
        typeof value === "boolean" ||
        typeof value === "number" ||
        typeof value === "bigint"
      ) {
        continue
      } else if (
        value instanceof ArrayBuffer ||
        (typeof SharedArrayBuffer !== "undefined" && value instanceof SharedArrayBuffer) ||
        ArrayBuffer.isView(value)
      ) {
        continue
      } else if (typeof value !== "object" || seen.has(value)) {
        return false
      } else {
        seen.add(value)
        if (Array.isArray(value)) {
          if (value.length > 32) return false
          for (let index = 0; index < value.length; index++) {
            pending.push([Reflect.get(value, index), depth + 1])
          }
        } else {
          const keys = Reflect.ownKeys(value)
          if (keys.length > 16 || keys.some((key) => typeof key !== "string")) return false
          for (const key of keys) {
            const remaining = maxStringBytes - stringBytes
            if ((key as string).length > remaining) return false
            stringBytes += encoder.encode(key as string).byteLength
            if (stringBytes > maxStringBytes) return false
            pending.push([Reflect.get(value, key), depth + 1])
          }
        }
      }
    }
    return true
  } catch {
    return false
  }
}
