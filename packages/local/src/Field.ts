import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Canonical from "./Canonical.js"

export interface TaggedError {
  readonly _tag: string
}

export interface Semantics<Value extends Schema.Top, Operation extends Schema.Top, E extends TaggedError = never,> {
  readonly schema: Value
  readonly operation: Operation
  readonly apply: (current: Value["Type"], operation: Operation["Type"]) => Effect.Effect<Value["Type"], E>
}

export const make = <Value extends Schema.Top, Operation extends Schema.Top, E extends TaggedError = never,>(options: {
  readonly schema: Value
  readonly operation: Operation
  readonly apply: (current: Value["Type"], operation: Operation["Type"]) => Effect.Effect<Value["Type"], E>
}): Semantics<Value, Operation, E> => Object.freeze(options)

export const register = <Value extends Schema.Top,>(schema: Value) => {
  const operation = Schema.Struct({ _tag: Schema.Literal("Set"), value: schema })
  return make({
    schema,
    operation,
    apply: (_current, update) => {
      const decode = schema.pipe(Schema.toType, Schema.decodeUnknownEffect)
      const value = Reflect.get(update, "value")
      return decode(value).pipe(Effect.orDie)
    }
  })
}

export const CounterOperation = Schema.Struct({
  _tag: Schema.Literal("Increment"),
  delta: Schema.Number
})

export const counter = make({
  schema: Schema.Number,
  operation: CounterOperation,
  apply: (current, operation) => Effect.succeed(current + operation.delta)
})

export const growOnlySet = <Item extends Schema.Top,>(item: Item) => {
  const operation = Schema.Struct({ _tag: Schema.Literal("Add"), value: item })
  return make({
    schema: Schema.Array(item),
    operation,
    apply: (current, update) =>
      Effect.gen(function*() {
        const decode = item.pipe(Schema.toType, Schema.decodeUnknownEffect)
        const input = Reflect.get(update, "value")
        const value = yield* decode(input).pipe(Effect.orDie)
        const encoded = Canonical.stringify(value)
        if (current.some((currentValue) => Canonical.stringify(currentValue) === encoded)) return current
        return [...current, value]
      })
  })
}
