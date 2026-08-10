import type * as Replica from "@lucas-barake/effect-local/Replica"
import type * as Duration from "effect/Duration"
import type * as Layer from "effect/Layer"
import type { Atom } from "effect/unstable/reactivity"
import type * as AtomRegistry from "effect/unstable/reactivity/AtomRegistry"
import type * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as ReplicaAtom from "./ReplicaAtom.js"

export const make = <R, E,>(
  layer: Layer.Layer<Replica.Replica | R, E, AtomRegistry.AtomRegistry | Reactivity.Reactivity>,
  options?: {
    readonly factory?: Atom.RuntimeFactory
    readonly idleTTL?: Duration.Input
  }
) => ReplicaAtom.make(layer, options)
