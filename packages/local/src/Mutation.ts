import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Identity from "./Identity.js"
import * as Defect from "./internal/defect.js"
import * as SchemaInput from "./internal/schemaInput.js"
import type * as ReplicaError from "./ReplicaError.js"
import type * as Transaction from "./Transaction.js"

export type Handler<P, A, E, R,> = (options: {
  readonly transaction: Transaction.Transaction
  readonly payload: P
}) => Effect.Effect<A, E | ReplicaError.StorageError, R>

export interface HandlerService<
  Name extends string,
  P extends Schema.Top,
  A extends Schema.Top,
  E extends Schema.Top,
> {
  readonly mutation: Mutation<Name, P, A, E>
  readonly execute: Handler<P["Type"], A["Type"], E["Type"], never>
}

export interface Mutation<Name extends string, P extends Schema.Top, A extends Schema.Top, E extends Schema.Top,> {
  readonly name: Name
  readonly version: Identity.SchemaVersion
  readonly payloadSchema: P
  readonly successSchema: A
  readonly rejectionSchema: E
  readonly handler: Context.Service<HandlerService<Name, P, A, E>, HandlerService<Name, P, A, E>>
  readonly of: <R,>(
    implementation: Handler<P["Type"], A["Type"], E["Type"], R>
  ) => Handler<P["Type"], A["Type"], E["Type"], R>
  readonly toLayer: <R, EX = never, RX = never,>(
    build:
      | Handler<P["Type"], A["Type"], E["Type"], R>
      | Effect.Effect<Handler<P["Type"], A["Type"], E["Type"], R>, EX, RX>
  ) => Layer.Layer<HandlerService<Name, P, A, E>, EX, Exclude<R | RX, Scope.Scope>>
}

export interface Any {
  readonly name: string
  readonly version: Identity.SchemaVersion
  readonly payloadSchema: SchemaInput.WireSchema
  readonly successSchema: SchemaInput.WireSchema
  readonly rejectionSchema: SchemaInput.WireSchema
  readonly handler: Context.Service.Any
}

let handlerId = 0

export function make<
  const Name extends string,
  P extends SchemaInput.Input = typeof SchemaInput.Void,
  A extends SchemaInput.WireSchema = typeof SchemaInput.Void,
  E extends SchemaInput.WireSchema = typeof Schema.Never,
>(name: Name, options: {
  readonly version: number
  readonly payload?: SchemaInput.Valid<P>
  readonly success?: A
  readonly rejection?: E
}): Mutation<Name, SchemaInput.Wire<P>, A, E>
export function make(name: string, options: {
  readonly version: number
  readonly payload?: SchemaInput.Input
  readonly success?: SchemaInput.WireSchema
  readonly rejection?: SchemaInput.WireSchema
}): Mutation<string, SchemaInput.WireSchema, SchemaInput.WireSchema, SchemaInput.WireSchema> {
  if (name.length === 0) return Defect.invalid("Mutation name must be nonempty")
  if (name.startsWith("$")) return Defect.invalid(`Mutation name must not start with $: ${name}`)
  let payloadSchema: SchemaInput.WireSchema = SchemaInput.Void
  if (options.payload !== undefined) payloadSchema = SchemaInput.normalize(options.payload)
  const successSchema = options.success ?? SchemaInput.Void
  const rejectionSchema = options.rejection ?? Schema.Never
  const handler = Context.Service<
    HandlerService<string, SchemaInput.WireSchema, SchemaInput.WireSchema, SchemaInput.WireSchema>,
    HandlerService<string, SchemaInput.WireSchema, SchemaInput.WireSchema, SchemaInput.WireSchema>
  >(
    `@lucas-barake/effect-local/Mutation/${name}/${handlerId++}`
  )
  const toLayer = <R, EX = never, RX = never,>(
    build:
      | Handler<SchemaInput.WireSchema["Type"], SchemaInput.WireSchema["Type"], SchemaInput.WireSchema["Type"], R>
      | Effect.Effect<
        Handler<SchemaInput.WireSchema["Type"], SchemaInput.WireSchema["Type"], SchemaInput.WireSchema["Type"], R>,
        EX,
        RX
      >
  ): Layer.Layer<
    HandlerService<string, SchemaInput.WireSchema, SchemaInput.WireSchema, SchemaInput.WireSchema>,
    EX,
    Exclude<R | RX, Scope.Scope>
  > =>
    Layer.effect(
      handler,
      Effect.gen(function*() {
        const context = (yield* Effect.context<R | Scope.Scope>()).pipe(Context.omit(Scope.Scope))
        let implementation: Handler<
          SchemaInput.WireSchema["Type"],
          SchemaInput.WireSchema["Type"],
          SchemaInput.WireSchema["Type"],
          R
        >
        if (Effect.isEffect(build)) implementation = yield* build
        else implementation = build
        return {
          mutation,
          execute: (input: Parameters<typeof implementation>[0]) =>
            implementation(input).pipe(Effect.scoped, Effect.provide(context))
        }
      })
    )
  const mutation: Mutation<string, SchemaInput.WireSchema, SchemaInput.WireSchema, SchemaInput.WireSchema> = {
    name,
    version: Identity.SchemaVersion.make(options.version),
    payloadSchema,
    successSchema,
    rejectionSchema,
    handler,
    of: (implementation) => implementation,
    toLayer
  }
  return mutation
}

export type Payload<M extends Any,> = M["payloadSchema"]["Type"]
export type Success<M extends Any,> = M["successSchema"]["Type"]
export type Rejection<M extends Any,> = M["rejectionSchema"]["Type"]
