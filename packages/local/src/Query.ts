import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import type * as Document from "./Document.js"
import type * as TaggedError from "./internal/taggedError.js"
import type * as Projection from "./Projection.js"

export type Handler<P, A, E, R,> = (payload: P) => Effect.Effect<A, E, R>

let handlerId = 0

export interface HandlerService<
  Name extends string,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends TaggedError.Schema,
  Dependencies extends ReadonlyArray<Projection.Any>,
> {
  readonly query: Query<Name, P, A, E, Dependencies>
}

export interface Query<
  Name extends string,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends TaggedError.Schema,
  Dependencies extends ReadonlyArray<Projection.Any>,
> {
  readonly name: Name
  readonly version: number
  readonly payloadSchema: P
  readonly successSchema: A
  readonly errorSchema: E
  readonly dependsOn: Dependencies
  readonly handler: Context.Service<
    HandlerService<Name, P, A, E, Dependencies>,
    Handler<P["Type"], A["Type"], E["Type"], never>
  >
  readonly of: <R,>(implementation: Handler<P["Type"], A["Type"], E["Type"], R>) => Handler<
    P["Type"],
    A["Type"],
    E["Type"],
    R
  >
  readonly toLayer: <R, EX = never, RX = never,>(
    build:
      | Handler<P["Type"], A["Type"], E["Type"], R>
      | Effect.Effect<Handler<P["Type"], A["Type"], E["Type"], R>, EX, RX>
  ) => Layer.Layer<HandlerService<Name, P, A, E, Dependencies>, EX, R | Exclude<RX, Scope.Scope>>
}

export interface Any {
  readonly name: string
  readonly version: number
  readonly payloadSchema: Document.WireSchema
  readonly successSchema: Document.WireSchema
  readonly errorSchema: TaggedError.Schema
  readonly dependsOn: ReadonlyArray<Projection.Any>
  readonly handler: Context.Service.Any
}

export const make = <
  const Name extends string,
  P extends Document.WireSchema = typeof Schema.Void,
  A extends Document.WireSchema = typeof Schema.Void,
  E extends TaggedError.Schema = typeof Schema.Never,
  const Dependencies extends ReadonlyArray<Projection.Any> = readonly [],
>(
  name: Name,
  options: {
    readonly payload?: P
    readonly version?: number
    readonly success?: A
    readonly error?: E
    readonly dependsOn: Dependencies
  }
): Query<Name, P, A, E, Dependencies> => {
  if (name.length === 0) throw new TypeError("Query name must be nonempty")
  const version = options.version ?? 1
  if (!Number.isSafeInteger(version) || version < 1) throw new TypeError("Query version must be a positive integer")
  const names = new Set<string>()
  for (const projection of options.dependsOn) {
    if (names.has(projection.name)) throw new TypeError(`Duplicate query dependency: ${projection.name}`)
    names.add(projection.name)
  }
  const handler = Context.Service<
    HandlerService<Name, P, A, E, Dependencies>,
    Handler<P["Type"], A["Type"], E["Type"], never>
  >(`@lucas-barake/effect-local/Query/${name}/${handlerId++}`)
  const toLayer = <R, EX = never, RX = never,>(
    build:
      | Handler<P["Type"], A["Type"], E["Type"], R>
      | Effect.Effect<Handler<P["Type"], A["Type"], E["Type"], R>, EX, RX>
  ): Layer.Layer<HandlerService<Name, P, A, E, Dependencies>, EX, R | Exclude<RX, Scope.Scope>> =>
    Layer.effect(
      handler,
      Effect.gen(function*() {
        const context = yield* Effect.context<R>()
        const implementation = Effect.isEffect(build) ? yield* build : build
        return (payload: P["Type"]) => implementation(payload).pipe(Effect.provide(context))
      })
    )
  return {
    name,
    version,
    payloadSchema: (options.payload ?? Schema.Void) as unknown as P,
    successSchema: (options.success ?? Schema.Void) as unknown as A,
    errorSchema: (options.error ?? Schema.Never) as unknown as E,
    dependsOn: options.dependsOn,
    handler,
    of: (implementation) => implementation,
    toLayer
  }
}
