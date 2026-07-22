import type * as Automerge from "@automerge/automerge"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import type * as Document from "./Document.js"
import * as SchemaInput from "./internal/schemaInput.js"
import type * as TaggedError from "./internal/taggedError.js"

export type DraftValue<A,> = A extends Automerge.ScalarValue ? A
  : A extends ReadonlyArray<infer Item> ? Array<DraftValue<Item>>
  : A extends object ? { -readonly [Key in keyof A]: DraftValue<A[Key]> }
  : A

export type Draft<D extends Document.Any,> = DraftValue<D["schema"]["Encoded"]>

export type SuccessResult<A,> = [A] extends [void] ? undefined : A

export type HandlerResult<A, E,> = [E] extends [never] ? SuccessResult<A> : Result.Result<A, E>

export interface HandlerOptions<D extends Document.Any, P,> {
  readonly draft: Draft<D>
  readonly payload: P
  readonly current: D["schema"]["Type"]
}

export type Handler<D extends Document.Any, P, A, E,> = (
  options: HandlerOptions<D, P>
) => HandlerResult<A, E>

let handlerId = 0

export interface HandlerService<
  Name extends string,
  D extends Document.Any,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends TaggedError.Schema,
> {
  readonly mutation: Mutation<Name, D, P, A, E>
}

export interface Mutation<
  Name extends string,
  D extends Document.Any,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends TaggedError.Schema,
> {
  readonly name: Name
  readonly version: number
  readonly document: D
  readonly payloadSchema: P
  readonly successSchema: A
  readonly errorSchema: E
  readonly handler: Context.Service<
    HandlerService<Name, D, P, A, E>,
    Handler<D, P["Type"], A["Type"], E["Type"]>
  >
  readonly of: (implementation: Handler<D, P["Type"], A["Type"], E["Type"]>) => Handler<
    D,
    P["Type"],
    A["Type"],
    E["Type"]
  >
  readonly toLayer: <EX = never, RX = never,>(
    build:
      | Handler<D, P["Type"], A["Type"], E["Type"]>
      | Effect.Effect<Handler<D, P["Type"], A["Type"], E["Type"]>, EX, RX>
  ) => Layer.Layer<HandlerService<Name, D, P, A, E>, EX, Exclude<RX, Scope.Scope>>
}

export interface Any {
  readonly name: string
  readonly version: number
  readonly document: Document.Any
  readonly payloadSchema: Document.WireSchema
  readonly successSchema: Document.WireSchema
  readonly errorSchema: TaggedError.Schema
  readonly handler: Context.Service.Any
}

export const make = <
  const Name extends string,
  D extends Document.Any,
  P extends SchemaInput.Input = typeof Schema.Void,
  A extends Document.WireSchema = typeof Schema.Void,
  E extends TaggedError.Schema = typeof Schema.Never,
>(
  name: Name,
  options: {
    readonly document: D
    readonly version?: number
    readonly payload?: SchemaInput.Valid<P>
    readonly success?: A
    readonly error?: E
  }
): Mutation<Name, D, SchemaInput.Wire<P>, A, E> => {
  if (name.length === 0) throw new TypeError("Mutation name must be nonempty")
  const version = options.version ?? 1
  if (!Number.isSafeInteger(version) || version < 1) throw new TypeError("Mutation version must be a positive integer")
  const handler = Context.Service<
    HandlerService<Name, D, SchemaInput.Wire<P>, A, E>,
    Handler<D, SchemaInput.Wire<P>["Type"], A["Type"], E["Type"]>
  >(`@lucas-barake/effect-local/Mutation/${options.document.name}/${name}/${handlerId++}`)
  return {
    name,
    version,
    document: options.document,
    payloadSchema: options.payload === undefined
      ? Schema.Void as SchemaInput.Wire<P>
      : SchemaInput.normalize(options.payload),
    successSchema: (options.success ?? Schema.Void) as unknown as A,
    errorSchema: (options.error ?? Schema.Never) as unknown as E,
    handler,
    of: handler.of,
    toLayer: (build) => Layer.effect(handler, Effect.isEffect(build) ? build : Effect.succeed(build))
  }
}
