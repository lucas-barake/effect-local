import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as SchemaInput from "./internal/schemaInput.js"
import type * as Model from "./Model.js"
import type * as ReplicaError from "./ReplicaError.js"
import type * as Transaction from "./Transaction.js"

export type Handler<P, A, E, R,> = (options: {
  readonly query: Transaction.Query
  readonly payload: P
}) => Effect.Effect<A, E | ReplicaError.StorageError, R>

export interface HandlerService<
  Name extends string,
  P extends Schema.Top,
  A extends Schema.Top,
  E extends Schema.Top,
> {
  readonly query: Query<Name, P, A, E>
  readonly execute: Handler<P["Type"], A["Type"], E["Type"], never>
}

export interface Query<Name extends string, P extends Schema.Top, A extends Schema.Top, E extends Schema.Top,> {
  readonly name: Name
  readonly payloadSchema: P
  readonly successSchema: A
  readonly errorSchema: E
  readonly dependsOn: ReadonlyArray<Model.Any>
  readonly handler: Context.Service<HandlerService<Name, P, A, E>, HandlerService<Name, P, A, E>>
  readonly toLayer: <R, EX = never, RX = never,>(
    build:
      | Handler<P["Type"], A["Type"], E["Type"], R>
      | Effect.Effect<Handler<P["Type"], A["Type"], E["Type"], R>, EX, RX>
  ) => Layer.Layer<HandlerService<Name, P, A, E>, EX, Exclude<R | RX, Scope.Scope>>
}

export interface Any {
  readonly name: string
  readonly payloadSchema: Schema.Top
  readonly successSchema: Schema.Top
  readonly errorSchema: Schema.Top
  readonly dependsOn: ReadonlyArray<Model.Any>
  readonly handler: Context.Service.Any
}

let handlerId = 0

export const make = <
  const Name extends string,
  P extends SchemaInput.Input = typeof SchemaInput.Void,
  A extends SchemaInput.WireSchema = typeof SchemaInput.Void,
  E extends SchemaInput.WireSchema = typeof Schema.Never,
>(
  name: Name,
  options: {
    readonly payload?: SchemaInput.Valid<P>
    readonly success?: A
    readonly error?: E
    readonly dependsOn: ReadonlyArray<Model.Any>
  }
): Query<Name, SchemaInput.Wire<P>, A, E> => {
  if (name.length === 0) throw new TypeError("Query name must be nonempty")
  if (name.startsWith("$")) throw new TypeError(`Query name must not start with $: ${name}`)
  const payloadSchema = options.payload === undefined ?
    SchemaInput.Void as unknown as SchemaInput.Wire<P> :
    (Schema.isSchema(options.payload) ? options.payload : Schema.Struct(options.payload)) as SchemaInput.Wire<P>
  const successSchema = (options.success ?? SchemaInput.Void) as A
  const errorSchema = (options.error ?? Schema.Never) as E
  const handler = Context.Service<
    HandlerService<Name, SchemaInput.Wire<P>, A, E>,
    HandlerService<Name, SchemaInput.Wire<P>, A, E>
  >(
    `@lucas-barake/effect-local/Query/${name}/${handlerId++}`
  )
  const toLayer = <R, EX = never, RX = never,>(
    build:
      | Handler<SchemaInput.Wire<P>["Type"], A["Type"], E["Type"], R>
      | Effect.Effect<Handler<SchemaInput.Wire<P>["Type"], A["Type"], E["Type"], R>, EX, RX>
  ): Layer.Layer<HandlerService<Name, SchemaInput.Wire<P>, A, E>, EX, Exclude<R | RX, Scope.Scope>> =>
    Layer.effect(
      handler,
      Effect.gen(function*() {
        const context = (yield* Effect.context<R | Scope.Scope>()).pipe(Context.omit(Scope.Scope)) as Context.Context<R>
        const implementation = Effect.isEffect(build) ? yield* build : build
        return {
          query,
          execute: (input: Parameters<typeof implementation>[0]) =>
            implementation(input).pipe(Effect.provide(context), Effect.scoped)
        }
      })
    )
  const query: Query<Name, SchemaInput.Wire<P>, A, E> = {
    name,
    payloadSchema,
    successSchema,
    errorSchema,
    dependsOn: Object.freeze([...options.dependsOn]),
    handler,
    toLayer
  }
  return query
}
