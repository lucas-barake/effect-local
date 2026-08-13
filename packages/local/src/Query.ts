import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Defect from "./internal/defect.js"
import * as SchemaInput from "./internal/schemaInput.js"
import type * as ReplicaError from "./ReplicaError.js"
import type * as Transaction from "./Transaction.js"

export interface TaggedError {
  readonly _tag: string
}

export type ErrorSchema = Schema.Top & Schema.ConstraintCodec<TaggedError, typeof Schema.Json.Type> & {
  readonly Rebuild: SchemaInput.WireSchema
}

export type Handler<P, A, E extends TaggedError, R,> = (options: {
  readonly query: Transaction.Query
  readonly payload: P
}) => Effect.Effect<A, E | ReplicaError.StorageError, R>

export interface HandlerService<
  Name extends string,
  P extends Schema.Top,
  A extends Schema.Top,
  E extends ErrorSchema,
> {
  readonly query: Query<Name, P, A, E>
  readonly execute: Handler<P["Type"], A["Type"], E["Type"], never>
}

export interface Query<Name extends string, P extends Schema.Top, A extends Schema.Top, E extends ErrorSchema,> {
  readonly name: Name
  readonly payloadSchema: P
  readonly successSchema: A
  readonly errorSchema: E
  readonly handler: Context.Service<HandlerService<Name, P, A, E>, HandlerService<Name, P, A, E>>
  readonly toLayer: <R, EX extends TaggedError = never, RX = never,>(
    build:
      | Handler<P["Type"], A["Type"], E["Type"], R>
      | Effect.Effect<Handler<P["Type"], A["Type"], E["Type"], R>, EX, RX>
  ) => Layer.Layer<HandlerService<Name, P, A, E>, EX, Exclude<R | RX, Scope.Scope>>
}

export interface Any {
  readonly name: string
  readonly payloadSchema: Schema.Top
  readonly successSchema: Schema.Top
  readonly errorSchema: ErrorSchema
  readonly handler: Context.Service.Any
}

let handlerId = 0

export function make<
  const Name extends string,
  P extends SchemaInput.Input = typeof SchemaInput.Void,
  A extends SchemaInput.WireSchema = typeof SchemaInput.Void,
  E extends ErrorSchema = typeof Schema.Never,
>(
  name: Name,
  options: {
    readonly payload?: SchemaInput.Valid<P>
    readonly success?: A
    readonly error?: E
  }
): Query<Name, SchemaInput.Wire<P>, A, E>
export function make(name: string, options: {
  readonly payload?: SchemaInput.Input
  readonly success?: SchemaInput.WireSchema
  readonly error?: ErrorSchema
}): Query<string, SchemaInput.WireSchema, SchemaInput.WireSchema, ErrorSchema> {
  if (name.length === 0) return Defect.invalid("Query name must be nonempty")
  if (name.startsWith("$")) return Defect.invalid(`Query name must not start with $: ${name}`)
  let payloadSchema: SchemaInput.WireSchema = SchemaInput.Void
  if (options.payload !== undefined) payloadSchema = SchemaInput.normalize(options.payload)
  const successSchema = options.success ?? SchemaInput.Void
  const errorSchema: ErrorSchema = options.error ?? Schema.Never
  const handler = Context.Service<
    HandlerService<string, SchemaInput.WireSchema, SchemaInput.WireSchema, ErrorSchema>,
    HandlerService<string, SchemaInput.WireSchema, SchemaInput.WireSchema, ErrorSchema>
  >(
    `@lucas-barake/effect-local/Query/${name}/${handlerId++}`
  )
  const toLayer = <R, EX extends TaggedError = never, RX = never,>(
    build:
      | Handler<SchemaInput.WireSchema["Type"], SchemaInput.WireSchema["Type"], TaggedError, R>
      | Effect.Effect<
        Handler<SchemaInput.WireSchema["Type"], SchemaInput.WireSchema["Type"], TaggedError, R>,
        EX,
        RX
      >
  ): Layer.Layer<
    HandlerService<string, SchemaInput.WireSchema, SchemaInput.WireSchema, ErrorSchema>,
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
          TaggedError,
          R
        >
        if (Effect.isEffect(build)) implementation = yield* build
        else implementation = build
        return {
          query,
          execute: (input: Parameters<typeof implementation>[0]) =>
            implementation(input).pipe(Effect.scoped, Effect.provide(context))
        }
      })
    )
  const query: Query<string, SchemaInput.WireSchema, SchemaInput.WireSchema, ErrorSchema> = {
    name,
    payloadSchema,
    successSchema,
    errorSchema,
    handler,
    toLayer
  }
  return query
}
