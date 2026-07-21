import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import type * as Document from "./Document.js"
import type * as Projection from "./Projection.js"

let handlerId = 0
const nextHandlerId = () => ++handlerId

export type Handler<P, A, E, R,> = (payload: P) => Effect.Effect<A, E, R>

export interface HandlerService<
  Name extends string,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends Document.WireSchema,
  Dependencies extends ReadonlyArray<Projection.Any>,
> {
  readonly query: Query<Name, P, A, E, Dependencies>
}

export interface Query<
  Name extends string,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends Document.WireSchema,
  Dependencies extends ReadonlyArray<Projection.Any>,
> {
  readonly name: Name
  readonly version: number
  readonly payload: P
  readonly success: A
  readonly error: E
  readonly dependsOn: Dependencies
  readonly handler: Context.Service<
    HandlerService<Name, P, A, E, Dependencies>,
    Handler<P["Type"], A["Type"], E["Type"], never>
  >
}

export interface Any {
  readonly name: string
  readonly version: number
  readonly payload: Document.WireSchema
  readonly success: Document.WireSchema
  readonly error: Document.WireSchema
  readonly dependsOn: ReadonlyArray<Projection.Any>
  readonly handler: Context.Service<any, any>
}

export const make = <
  const Name extends string,
  P extends Document.WireSchema = typeof Schema.Void,
  A extends Document.WireSchema = typeof Schema.Void,
  E extends Document.WireSchema = typeof Schema.Never,
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
  return {
    name,
    version,
    payload: (options.payload ?? Schema.Void) as unknown as P,
    success: (options.success ?? Schema.Void) as unknown as A,
    error: (options.error ?? Schema.Never) as unknown as E,
    dependsOn: options.dependsOn,
    handler: Context.Service(`@lucas-barake/effect-local/Query/${name}/${nextHandlerId()}`)
  }
}

export const handler = <
  Name extends string,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends Document.WireSchema,
  Dependencies extends ReadonlyArray<Projection.Any>,
  R,
>(
  _definition: Query<Name, P, A, E, Dependencies>,
  implementation: Handler<P["Type"], A["Type"], E["Type"], R>
): Handler<P["Type"], A["Type"], E["Type"], R> => implementation

export const layer = <
  Name extends string,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends Document.WireSchema,
  Dependencies extends ReadonlyArray<Projection.Any>,
  R,
>(
  definition: Query<Name, P, A, E, Dependencies>,
  implementation: Handler<P["Type"], A["Type"], E["Type"], R>
): Layer.Layer<HandlerService<Name, P, A, E, Dependencies>, never, R> =>
  Layer.effect(
    definition.handler,
    Effect.context<R>().pipe(
      Effect.map((context) => (payload: P["Type"]) => implementation(payload).pipe(Effect.provide(context)))
    )
  )
