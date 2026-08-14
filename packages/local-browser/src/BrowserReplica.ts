import type * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import type * as Duration from "effect/Duration"
import type * as Layer from "effect/Layer"
import type { Atom } from "effect/unstable/reactivity"
import type * as AtomRegistry from "effect/unstable/reactivity/AtomRegistry"
import type * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as ReplicaAtom from "./ReplicaAtom.js"

export const make = <E,>(
  layer: Layer.Layer<
    Replica.Replica | QueryReactivity.QueryReactivity,
    E,
    AtomRegistry.AtomRegistry | Reactivity.Reactivity
  >,
  options: {
    readonly maximumWholeAttachmentBytes: number
    readonly factory?: Atom.RuntimeFactory
    readonly idleTTL?: Duration.Input
  }
) => ReplicaAtom.make(layer, options)
