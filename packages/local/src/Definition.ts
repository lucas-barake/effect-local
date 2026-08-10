import * as Canonical from "./Canonical.js"
import type * as Model from "./Model.js"
import type * as Mutation from "./Mutation.js"
import type * as Query from "./Query.js"
import * as SchemaDescriptor from "./SchemaDescriptor.js"

export interface Definition<
  Models extends ReadonlyArray<Model.Any>,
  Mutations extends ReadonlyArray<Mutation.Any>,
  Queries extends ReadonlyArray<Query.Any>,
> {
  readonly models: Models
  readonly mutations: Mutations
  readonly queries: Queries
  readonly modelByName: ReadonlyMap<string, Model.Any>
  readonly mutationByName: ReadonlyMap<string, Mutation.Any>
  readonly queryByName: ReadonlyMap<string, Query.Any>
  readonly hash: string
}

const indexed = <A extends { readonly name: string },>(kind: string, values: ReadonlyArray<A>) => {
  const result = new Map<string, A>()
  for (const value of values) {
    if (result.has(value.name)) throw new TypeError(`Duplicate ${kind} name: ${value.name}`)
    result.set(value.name, value)
  }
  return result
}

export const make = <
  const Models extends ReadonlyArray<Model.Any>,
  const Mutations extends ReadonlyArray<Mutation.Any>,
  const Queries extends ReadonlyArray<Query.Any> = readonly [],
>(options: {
  readonly models: Models
  readonly mutations: Mutations
  readonly queries?: Queries
}): Definition<Models, Mutations, Queries> => {
  const queries = (options.queries ?? []) as unknown as Queries
  const modelByName = indexed("model", options.models)
  const mutationByName = indexed("mutation", options.mutations)
  const queryByName = indexed("query", queries)
  for (const query of queries) {
    for (const dependency of query.dependsOn) {
      if (modelByName.get(dependency.name) !== dependency) {
        throw new TypeError(`Query ${query.name} depends on an unregistered model: ${dependency.name}`)
      }
    }
  }
  const hash = Canonical.hash({
    models: options.models.map((model) => ({
      name: model.name,
      key: SchemaDescriptor.make(model.key),
      schema: SchemaDescriptor.make(model.schema)
    })),
    mutations: options.mutations.map((mutation) => ({
      name: mutation.name,
      payload: SchemaDescriptor.make(mutation.payloadSchema),
      success: SchemaDescriptor.make(mutation.successSchema),
      rejection: SchemaDescriptor.make(mutation.rejectionSchema)
    })),
    queries: queries.map((query) => ({
      name: query.name,
      payload: SchemaDescriptor.make(query.payloadSchema),
      success: SchemaDescriptor.make(query.successSchema),
      error: SchemaDescriptor.make(query.errorSchema),
      dependsOn: query.dependsOn.map((model) => model.name)
    }))
  })
  return Object.freeze({
    models: Object.freeze([...options.models]) as unknown as Models,
    mutations: Object.freeze([...options.mutations]) as unknown as Mutations,
    queries: Object.freeze([...queries]) as unknown as Queries,
    modelByName,
    mutationByName,
    queryByName,
    hash
  })
}

export interface Any
  extends Definition<ReadonlyArray<Model.Any>, ReadonlyArray<Mutation.Any>, ReadonlyArray<Query.Any>>
{}
