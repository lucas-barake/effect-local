import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Canonical from "./Canonical.js"

export interface Semantics<Value extends Schema.Top, Operation extends Schema.Top, E = never,> {
  readonly schema: Value
  readonly operation: Operation
  readonly apply: (current: Value["Type"], operation: Operation["Type"]) => Effect.Effect<Value["Type"], E>
}

export const make = <Value extends Schema.Top, Operation extends Schema.Top, E = never,>(options: {
  readonly schema: Value
  readonly operation: Operation
  readonly apply: (current: Value["Type"], operation: Operation["Type"]) => Effect.Effect<Value["Type"], E>
}): Semantics<Value, Operation, E> => Object.freeze(options)

export const register = <Value extends Schema.Top,>(schema: Value) => {
  const operation = Schema.Struct({ _tag: Schema.Literal("Set"), value: schema })
  return make({
    schema,
    operation,
    apply: (_current, update) => Effect.succeed((update as { readonly value: Value["Type"] }).value)
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
    apply: (current, update) => {
      const value = (update as { readonly value: Item["Type"] }).value
      const encoded = Canonical.stringify(value)
      return Effect.succeed(
        current.some((currentValue) => Canonical.stringify(currentValue) === encoded) ? current : [...current, value]
      )
    }
  })
}
