import type * as Automerge from "@automerge/automerge"
import * as Context from "effect/Context"
import * as Layer from "effect/Layer"
import type * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import type * as Document from "./Document.js"

let handlerId = 0
const nextHandlerId = () => ++handlerId

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

export interface HandlerService<
  Name extends string,
  D extends Document.Any,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends Document.WireSchema,
> {
  readonly mutation: Mutation<Name, D, P, A, E>
}

export interface Mutation<
  Name extends string,
  D extends Document.Any,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends Document.WireSchema,
> {
  readonly name: Name
  readonly version: number
  readonly document: D
  readonly payload: P
  readonly success: A
  readonly error: E
  readonly handler: Context.Service<
    HandlerService<Name, D, P, A, E>,
    Handler<D, P["Type"], A["Type"], E["Type"]>
  >
}

export interface Any {
  readonly name: string
  readonly version: number
  readonly document: Document.Any
  readonly payload: Document.WireSchema
  readonly success: Document.WireSchema
  readonly error: Document.WireSchema
  readonly handler: Context.Service<any, any>
}

export const make = <
  const Name extends string,
  D extends Document.Any,
  P extends Document.WireSchema = typeof Schema.Void,
  A extends Document.WireSchema = typeof Schema.Void,
  E extends Document.WireSchema = typeof Schema.Never,
>(
  name: Name,
  options: {
    readonly document: D
    readonly version?: number
    readonly payload?: P
    readonly success?: A
    readonly error?: E
  }
): Mutation<Name, D, P, A, E> => {
  if (name.length === 0) throw new TypeError("Mutation name must be nonempty")
  const version = options.version ?? 1
  if (!Number.isSafeInteger(version) || version < 1) throw new TypeError("Mutation version must be a positive integer")
  return {
    name,
    version,
    document: options.document,
    payload: (options.payload ?? Schema.Void) as unknown as P,
    success: (options.success ?? Schema.Void) as unknown as A,
    error: (options.error ?? Schema.Never) as unknown as E,
    handler: Context.Service(`@lucas-barake/effect-local/Mutation/${name}/${nextHandlerId()}`)
  }
}

export const handler = <
  Name extends string,
  D extends Document.Any,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends Document.WireSchema,
>(
  _definition: Mutation<Name, D, P, A, E>,
  implementation: Handler<D, P["Type"], A["Type"], E["Type"]>
): Handler<D, P["Type"], A["Type"], E["Type"]> => implementation

export const layer = <
  Name extends string,
  D extends Document.Any,
  P extends Document.WireSchema,
  A extends Document.WireSchema,
  E extends Document.WireSchema,
>(
  definition: Mutation<Name, D, P, A, E>,
  implementation: Handler<D, P["Type"], A["Type"], E["Type"]>
): Layer.Layer<HandlerService<Name, D, P, A, E>> => Layer.succeed(definition.handler, implementation)
