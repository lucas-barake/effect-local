import * as Schema from "effect/Schema"
import * as Defect from "./defect.js"

const maximumLength = 256

export const ComponentName = Schema.NonEmptyString.check(Schema.isMaxLength(maximumLength))

const isComponentName = Schema.is(ComponentName)

export const validate = (kind: string, name: string): void => {
  if (!isComponentName(name)) {
    return Defect.invalid(`${kind} name must be nonempty and at most ${maximumLength} characters`)
  }
  if (name.startsWith("$")) return Defect.invalid(`${kind} name must not start with $: ${name}`)
}
