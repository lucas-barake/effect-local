import * as Canonical from "./Canonical.js"
import * as Identity from "./Identity.js"
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
  readonly version: Identity.SchemaVersion
  readonly schemaIdentity: Identity.SchemaIdentity
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

const byName = <A extends { readonly name: string },>(left: A, right: A): number =>
  left.name < right.name ? -1 : left.name > right.name ? 1 : 0

export const make = <
  const Models extends ReadonlyArray<Model.Any>,
  const Mutations extends ReadonlyArray<Mutation.Any>,
  const Queries extends ReadonlyArray<Query.Any> = readonly [],
>(options: {
  readonly version: number
  readonly models: Models
  readonly mutations: Mutations
  readonly queries?: Queries
}): Definition<Models, Mutations, Queries> => {
  const queries = (options.queries ?? []) as unknown as Queries
  const modelByName = indexed("model", options.models)
  const mutationByName = indexed("mutation", options.mutations)
  const queryByName = indexed("query", queries)
  const version = Identity.SchemaVersion.make(options.version)
  for (const query of queries) {
    for (const dependency of query.dependsOn) {
      if (modelByName.get(dependency.name) !== dependency) {
        throw new TypeError(`Query ${query.name} depends on an unregistered model: ${dependency.name}`)
      }
    }
  }
  const models = options.models.map((model) => ({
      name: model.name,
      version: model.version,
      key: SchemaDescriptor.make(model.key),
      schema: SchemaDescriptor.make(model.schema)
    })).toSorted(byName)
  const mutations = options.mutations.map((mutation) => ({
      name: mutation.name,
      version: mutation.version,
      payload: SchemaDescriptor.make(mutation.payloadSchema),
      success: SchemaDescriptor.make(mutation.successSchema),
      rejection: SchemaDescriptor.make(mutation.rejectionSchema)
    })).toSorted(byName)
  const schemaIdentity = Identity.SchemaIdentity.make({
    version,
    hash: Identity.SchemaHash.make(Canonical.hash({ format: 1, models, mutations }))
  })
  const hash = Canonical.hash({
    format: 2,
    schemaIdentity,
    queries: queries.map((query) => ({
      name: query.name,
      payload: SchemaDescriptor.make(query.payloadSchema),
      success: SchemaDescriptor.make(query.successSchema),
      error: SchemaDescriptor.make(query.errorSchema),
      dependsOn: query.dependsOn.map((model) => model.name).toSorted()
    })).toSorted(byName)
  })
  return Object.freeze({
    models: Object.freeze([...options.models]) as unknown as Models,
    mutations: Object.freeze([...options.mutations]) as unknown as Mutations,
    queries: Object.freeze([...queries]) as unknown as Queries,
    modelByName,
    mutationByName,
    queryByName,
    version,
    schemaIdentity,
    hash
  })
}

export interface Any
  extends Definition<ReadonlyArray<Model.Any>, ReadonlyArray<Mutation.Any>, ReadonlyArray<Query.Any>>
{}
