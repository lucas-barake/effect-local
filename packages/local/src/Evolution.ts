import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Canonical from "./Canonical.js"
import type * as Definition from "./Definition.js"
import * as Identity from "./Identity.js"
import * as Defect from "./internal/defect.js"
import type * as SchemaInput from "./internal/schemaInput.js"
import type * as Model from "./Model.js"
import type * as Mutation from "./Mutation.js"
import * as ReplicaError from "./ReplicaError.js"
import * as SchemaDescriptor from "./SchemaDescriptor.js"

export interface ModelMigration<From extends Model.Any, To extends Model.Any,> {
  readonly id: string
  readonly from: From
  readonly to: To
  migrateKey?(key: Model.Key<From>): Model.Key<To>
  migrateValue?(input: {
    readonly key: Model.Key<From>
    readonly targetKey: Model.Key<To>
    readonly value: Model.Value<From>
  }): Model.Value<To>
}

export interface MutationMigration<From extends Mutation.Any, To extends Mutation.Any,> {
  readonly id: string
  readonly from: From
  readonly to: To
  migratePayload?(payload: Mutation.Payload<From>): Mutation.Payload<To>
  migrateSuccess?(success: Mutation.Success<From>): Mutation.Success<To>
  migrateRejection?(rejection: Mutation.Rejection<From>): Mutation.Rejection<To>
}

type AnyModelMigration = ModelMigration<Model.Any, Model.Any>
type AnyMutationMigration = MutationMigration<Mutation.Any, Mutation.Any>

export interface Step {
  readonly id: string
  readonly from: Definition.Any
  readonly to: Definition.Any
  readonly models: ReadonlyMap<string, AnyModelMigration>
  readonly mutations: ReadonlyMap<string, AnyMutationMigration>
  readonly hash: Identity.SchemaHash
}

export interface LegacyBaseline {
  readonly id: string
  readonly hash: Identity.SchemaHash
  readonly definition: Definition.Any
}

export interface Evolution {
  readonly current: Definition.Any
  readonly steps: ReadonlyArray<Step>
  readonly legacyBaselines: ReadonlyArray<LegacyBaseline>
  readonly migrationHash: Identity.SchemaHash
  readonly definitionByIdentity: ReadonlyMap<string, Definition.Any>
  readonly stepBySourceIdentity: ReadonlyMap<string, Step>
  readonly legacyBaselineByHash: ReadonlyMap<string, LegacyBaseline>
}

export interface ModelAlias {
  readonly schemaIdentity: Identity.SchemaIdentity
  readonly modelVersion: Identity.SchemaVersion
  readonly key: typeof Schema.Json.Type
}

export interface MigratedModel {
  readonly schemaIdentity: Identity.SchemaIdentity
  readonly modelVersion: Identity.SchemaVersion
  readonly key: typeof Schema.Json.Type
  readonly value?: typeof Schema.Json.Type | undefined
  readonly aliases: ReadonlyArray<ModelAlias>
}

export interface MigratedMutationValue {
  readonly schemaIdentity: Identity.SchemaIdentity
  readonly mutationVersion: Identity.SchemaVersion
  readonly value: typeof Schema.Json.Type
}

const identityKey = (identity: Identity.SchemaIdentity): string => `${identity.version}:${identity.hash}`

const byName = <A extends { readonly name: string },>(left: A, right: A): number => {
  if (left.name < right.name) return -1
  if (left.name > right.name) return 1
  return 0
}

const byId = <A extends { readonly id: string },>(left: A, right: A): number => {
  if (left.id < right.id) return -1
  if (left.id > right.id) return 1
  return 0
}

const assertStableId = (kind: string, id: string): void => {
  if (!/^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$/.test(id)) {
    return Defect.invalid(
      `${kind} id must be stable, nonempty, and contain only letters, numbers, dot, slash, underscore, or dash: ${id}`
    )
  }
}

const descriptor = (schema: Schema.Top): string => Canonical.stringify(SchemaDescriptor.make(schema))

const sameSchema = (left: Schema.Top, right: Schema.Top): boolean => descriptor(left) === descriptor(right)

const sameIdentity = (left: Identity.SchemaIdentity, right: Identity.SchemaIdentity): boolean =>
  left.version === right.version && left.hash === right.hash

const indexedMigrations = <A extends { readonly id: string; readonly from: { readonly name: string } },>(
  kind: string,
  values: ReadonlyArray<A>
): ReadonlyMap<string, A> => {
  const migrationByName = new Map<string, A>()
  const ids = new Set<string>()
  for (const value of values) {
    assertStableId(kind, value.id)
    if (ids.has(value.id)) return Defect.invalid(`Duplicate ${kind} migration id: ${value.id}`)
    if (migrationByName.has(value.from.name)) {
      return Defect.invalid(`Duplicate ${kind} migration for: ${value.from.name}`)
    }
    ids.add(value.id)
    migrationByName.set(value.from.name, value)
  }
  return migrationByName
}

export const model = <From extends Model.Any, To extends Model.Any,>(options: {
  readonly id: string
  readonly from: From
  readonly to: To
  readonly key?: ((key: Model.Key<From>) => Model.Key<To>) | undefined
  readonly value?:
    | ((input: {
      readonly key: Model.Key<From>
      readonly targetKey: Model.Key<To>
      readonly value: Model.Value<From>
    }) => Model.Value<To>)
    | undefined
}): ModelMigration<From, To> => {
  assertStableId("model", options.id)
  if (options.from.name !== options.to.name) {
    return Defect.invalid(`Model migration must preserve its name: ${options.from.name} -> ${options.to.name}`)
  }
  const keyChanged = !sameSchema(options.from.key, options.to.key)
  const valueChanged = !sameSchema(options.from.schema, options.to.schema)
  if (keyChanged && options.key === undefined) {
    return Defect.invalid(`Model migration ${options.id} requires a key transform`)
  }
  if (valueChanged && options.value === undefined) {
    return Defect.invalid(`Model migration ${options.id} requires a value transform`)
  }
  if (!keyChanged && !valueChanged && options.key === undefined && options.value === undefined) {
    return Defect.invalid(`Model migration ${options.id} must declare at least one semantic transform`)
  }
  const migration: {
    id: string
    from: From
    to: To
    migrateKey?: (key: Model.Key<From>) => Model.Key<To>
    migrateValue?: (input: {
      readonly key: Model.Key<From>
      readonly targetKey: Model.Key<To>
      readonly value: Model.Value<From>
    }) => Model.Value<To>
  } = {
    id: options.id,
    from: options.from,
    to: options.to
  }
  if (options.key !== undefined) migration.migrateKey = options.key
  if (options.value !== undefined) migration.migrateValue = options.value
  return Object.freeze(migration)
}

export const mutation = <From extends Mutation.Any, To extends Mutation.Any,>(options: {
  readonly id: string
  readonly from: From
  readonly to: To
  readonly payload?: ((payload: Mutation.Payload<From>) => Mutation.Payload<To>) | undefined
  readonly success?: ((success: Mutation.Success<From>) => Mutation.Success<To>) | undefined
  readonly rejection?: ((rejection: Mutation.Rejection<From>) => Mutation.Rejection<To>) | undefined
}): MutationMigration<From, To> => {
  assertStableId("mutation", options.id)
  if (options.from.name !== options.to.name) {
    return Defect.invalid(`Mutation migration must preserve its name: ${options.from.name} -> ${options.to.name}`)
  }
  const payloadChanged = !sameSchema(options.from.payloadSchema, options.to.payloadSchema)
  const successChanged = !sameSchema(options.from.successSchema, options.to.successSchema)
  const rejectionChanged = !sameSchema(options.from.rejectionSchema, options.to.rejectionSchema)
  if (payloadChanged && options.payload === undefined) {
    return Defect.invalid(`Mutation migration ${options.id} requires a payload transform`)
  }
  if (successChanged && options.success === undefined) {
    return Defect.invalid(`Mutation migration ${options.id} requires a success transform`)
  }
  if (rejectionChanged && options.rejection === undefined) {
    return Defect.invalid(`Mutation migration ${options.id} requires a rejection transform`)
  }
  if (
    !payloadChanged && !successChanged && !rejectionChanged &&
    options.payload === undefined && options.success === undefined && options.rejection === undefined
  ) {
    return Defect.invalid(`Mutation migration ${options.id} must declare at least one semantic transform`)
  }
  const migration: {
    id: string
    from: From
    to: To
    migratePayload?: (payload: Mutation.Payload<From>) => Mutation.Payload<To>
    migrateSuccess?: (success: Mutation.Success<From>) => Mutation.Success<To>
    migrateRejection?: (rejection: Mutation.Rejection<From>) => Mutation.Rejection<To>
  } = {
    id: options.id,
    from: options.from,
    to: options.to
  }
  if (options.payload !== undefined) migration.migratePayload = options.payload
  if (options.success !== undefined) migration.migrateSuccess = options.success
  if (options.rejection !== undefined) migration.migrateRejection = options.rejection
  return Object.freeze(migration)
}

const validateModelChanges = (
  from: Definition.Any,
  to: Definition.Any,
  migrations: ReadonlyMap<string, AnyModelMigration>
): void => {
  for (const source of from.models) {
    const target = to.modelByName.get(source.name)
    if (target === undefined) return Defect.invalid(`Schema evolution cannot remove model: ${source.name}`)
    const changed = source.version !== target.version ||
      !sameSchema(source.key, target.key) || !sameSchema(source.schema, target.schema)
    const migration = migrations.get(source.name)
    if (!changed) {
      if (migration !== undefined) return Defect.invalid(`Unchanged model must not declare a migration: ${source.name}`)
      continue
    }
    if (target.version !== source.version + 1) {
      return Defect.invalid(`Changed model ${source.name} must advance exactly one version`)
    }
    if (migration === undefined || migration.from !== source || migration.to !== target) {
      return Defect.invalid(`Changed model ${source.name} requires an exact source and target migration`)
    }
  }
  for (const target of to.models) {
    if (!from.modelByName.has(target.name) && target.version !== 1) {
      return Defect.invalid(`Added model ${target.name} must start at version 1`)
    }
  }
  for (const migration of migrations.values()) {
    if (
      from.modelByName.get(migration.from.name) !== migration.from ||
      to.modelByName.get(migration.to.name) !== migration.to
    ) {
      return Defect.invalid(`Model migration ${migration.id} does not belong to its step definitions`)
    }
  }
}

const validateMutationChanges = (
  from: Definition.Any,
  to: Definition.Any,
  migrations: ReadonlyMap<string, AnyMutationMigration>
): void => {
  for (const source of from.mutations) {
    const target = to.mutationByName.get(source.name)
    if (target === undefined) return Defect.invalid(`Schema evolution cannot remove mutation: ${source.name}`)
    const changed = source.version !== target.version ||
      !sameSchema(source.payloadSchema, target.payloadSchema) ||
      !sameSchema(source.successSchema, target.successSchema) ||
      !sameSchema(source.rejectionSchema, target.rejectionSchema)
    const migration = migrations.get(source.name)
    if (!changed) {
      if (migration !== undefined) {
        return Defect.invalid(`Unchanged mutation must not declare a migration: ${source.name}`)
      }
      continue
    }
    if (target.version !== source.version + 1) {
      return Defect.invalid(`Changed mutation ${source.name} must advance exactly one version`)
    }
    if (migration === undefined || migration.from !== source || migration.to !== target) {
      return Defect.invalid(`Changed mutation ${source.name} requires an exact source and target migration`)
    }
  }
  for (const target of to.mutations) {
    if (!from.mutationByName.has(target.name) && target.version !== 1) {
      return Defect.invalid(`Added mutation ${target.name} must start at version 1`)
    }
  }
  for (const migration of migrations.values()) {
    if (
      from.mutationByName.get(migration.from.name) !== migration.from ||
      to.mutationByName.get(migration.to.name) !== migration.to
    ) {
      return Defect.invalid(`Mutation migration ${migration.id} does not belong to its step definitions`)
    }
  }
}

export const step = (options: {
  readonly id: string
  readonly from: Definition.Any
  readonly to: Definition.Any
  readonly models?: ReadonlyArray<AnyModelMigration> | undefined
  readonly mutations?: ReadonlyArray<AnyMutationMigration> | undefined
}): Step => {
  assertStableId("schema evolution step", options.id)
  if (options.to.version !== options.from.version + 1) {
    return Defect.invalid(`Schema evolution step ${options.id} must advance exactly one definition version`)
  }
  if (sameIdentity(options.from.schemaIdentity, options.to.schemaIdentity)) {
    return Defect.invalid(`Schema evolution step ${options.id} must change the structural schema identity`)
  }
  const models = indexedMigrations("model", options.models ?? [])
  const mutations = indexedMigrations("mutation", options.mutations ?? [])
  validateModelChanges(options.from, options.to, models)
  validateMutationChanges(options.from, options.to, mutations)
  return Object.freeze({
    id: options.id,
    from: options.from,
    to: options.to,
    models,
    mutations,
    hash: Identity.SchemaHash.make(Canonical.hash({
      format: 1,
      id: options.id,
      from: options.from.schemaIdentity,
      to: options.to.schemaIdentity,
      models: Array.from(models.values()).map((entry) => ({
        id: entry.id,
        name: entry.from.name,
        from: entry.from.version,
        to: entry.to.version
      })).toSorted(byName),
      mutations: Array.from(mutations.values()).map((entry) => ({
        id: entry.id,
        name: entry.from.name,
        from: entry.from.version,
        to: entry.to.version
      })).toSorted(byName)
    }))
  })
}

export const legacyBaseline = (options: {
  readonly id: string
  readonly hash: string
  readonly definition: Definition.Any
}): LegacyBaseline => {
  assertStableId("legacy baseline", options.id)
  return Object.freeze({
    id: options.id,
    hash: Identity.SchemaHash.make(options.hash),
    definition: options.definition
  })
}

export const make = (options: {
  readonly current: Definition.Any
  readonly steps?: ReadonlyArray<Step> | undefined
  readonly legacyBaselines?: ReadonlyArray<LegacyBaseline> | undefined
}): Evolution => {
  const steps = [...(options.steps ?? [])].toSorted((left, right) => left.from.version - right.from.version)
  const stepIds = new Set<string>()
  const migrationIds = new Set<string>()
  const definitionByIdentity = new Map<string, Definition.Any>()
  const stepBySourceIdentity = new Map<string, Step>()
  const addDefinition = (definition: Definition.Any): void => {
    const key = identityKey(definition.schemaIdentity)
    const existing = definitionByIdentity.get(key)
    if (existing !== undefined && existing.version !== definition.version) {
      return Defect.invalid(`Conflicting definition for schema identity: ${key}`)
    }
    definitionByIdentity.set(key, definition)
  }
  addDefinition(options.current)
  for (const entry of steps) {
    if (stepIds.has(entry.id)) return Defect.invalid(`Duplicate schema evolution step id: ${entry.id}`)
    stepIds.add(entry.id)
    const sourceKey = identityKey(entry.from.schemaIdentity)
    if (stepBySourceIdentity.has(sourceKey)) {
      return Defect.invalid(`Duplicate schema evolution source identity: ${sourceKey}`)
    }
    stepBySourceIdentity.set(sourceKey, entry)
    addDefinition(entry.from)
    addDefinition(entry.to)
    for (const migration of [...entry.models.values(), ...entry.mutations.values()]) {
      if (migrationIds.has(migration.id)) return Defect.invalid(`Duplicate component migration id: ${migration.id}`)
      migrationIds.add(migration.id)
    }
  }
  for (let index = 1; index < steps.length; index++) {
    const previous = steps[index - 1]
    const next = steps[index]
    if (!sameIdentity(previous.to.schemaIdentity, next.from.schemaIdentity)) {
      return Defect.invalid(`Schema evolution chain is not contiguous between ${previous.id} and ${next.id}`)
    }
  }
  if (steps.length > 0 && !sameIdentity(steps[steps.length - 1].to.schemaIdentity, options.current.schemaIdentity)) {
    return Defect.invalid("Schema evolution chain does not terminate at the current definition")
  }
  const legacyBaselines = [...(options.legacyBaselines ?? [])]
  const legacyBaselineByHash = new Map<string, LegacyBaseline>()
  const baselineIds = new Set<string>()
  for (const baseline of legacyBaselines) {
    if (baselineIds.has(baseline.id)) return Defect.invalid(`Duplicate legacy baseline id: ${baseline.id}`)
    if (legacyBaselineByHash.has(baseline.hash)) {
      return Defect.invalid(`Duplicate legacy baseline hash: ${baseline.hash}`)
    }
    if (!definitionByIdentity.has(identityKey(baseline.definition.schemaIdentity))) {
      return Defect.invalid(`Legacy baseline ${baseline.id} does not map to a definition in the evolution chain`)
    }
    baselineIds.add(baseline.id)
    legacyBaselineByHash.set(baseline.hash, baseline)
  }
  return Object.freeze({
    current: options.current,
    steps: Object.freeze(steps),
    legacyBaselines: Object.freeze(legacyBaselines),
    migrationHash: Identity.SchemaHash.make(Canonical.hash({
      format: 1,
      current: options.current.schemaIdentity,
      steps: steps.map((entry) => ({ id: entry.id, hash: entry.hash })),
      legacyBaselines: legacyBaselines.map((entry) => ({
        id: entry.id,
        hash: entry.hash,
        schemaIdentity: entry.definition.schemaIdentity
      })).toSorted(byId)
    })),
    definitionByIdentity,
    stepBySourceIdentity,
    legacyBaselineByHash
  })
}

const pathFrom = (
  evolution: Evolution,
  source: Identity.SchemaIdentity
): Effect.Effect<ReadonlyArray<Step>, ReplicaError.SchemaEvolutionUnsupported> => {
  if (sameIdentity(source, evolution.current.schemaIdentity)) return Effect.succeed([])
  if (!evolution.definitionByIdentity.has(identityKey(source))) {
    return Effect.fail(
      new ReplicaError.SchemaEvolutionUnsupported({
        sourceVersion: source.version,
        sourceHash: source.hash,
        targetVersion: evolution.current.schemaIdentity.version,
        targetHash: evolution.current.schemaIdentity.hash
      })
    )
  }
  const path: Array<Step> = []
  let identity = source
  while (!sameIdentity(identity, evolution.current.schemaIdentity)) {
    const next = evolution.stepBySourceIdentity.get(identityKey(identity))
    if (next === undefined) {
      return Effect.fail(
        new ReplicaError.SchemaEvolutionUnsupported({
          sourceVersion: source.version,
          sourceHash: source.hash,
          targetVersion: evolution.current.schemaIdentity.version,
          targetHash: evolution.current.schemaIdentity.hash
        })
      )
    }
    path.push(next)
    identity = next.to.schemaIdentity
  }
  return Effect.succeed(path)
}

export const migrateModel = (options: {
  readonly evolution: Evolution
  readonly source: Identity.SchemaIdentity
  readonly model: string
  readonly modelVersion: Identity.SchemaVersion
  readonly key: typeof Schema.Json.Type
  readonly value?: typeof Schema.Json.Type | undefined
}): Effect.Effect<MigratedModel, ReplicaError.SchemaEvolutionUnsupported | ReplicaError.SchemaEvolutionFailed> =>
  Effect.gen(function*() {
    const path = yield* pathFrom(options.evolution, options.source)
    const sourceDefinition = options.evolution.definitionByIdentity.get(identityKey(options.source))
    const sourceModel = sourceDefinition?.modelByName.get(options.model)
    if (sourceModel === undefined || sourceModel.version !== options.modelVersion) {
      return yield* new ReplicaError.SchemaEvolutionUnsupported({
        sourceVersion: options.source.version,
        sourceHash: options.source.hash,
        targetVersion: options.evolution.current.schemaIdentity.version,
        targetHash: options.evolution.current.schemaIdentity.hash
      })
    }
    let key = options.key
    let value = options.value
    let version = options.modelVersion
    let identity = options.source
    const aliases: Array<ModelAlias> = [{ schemaIdentity: identity, modelVersion: version, key }]
    for (const entry of path) {
      const source = entry.from.modelByName.get(options.model)
      const target = entry.to.modelByName.get(options.model)
      if (source === undefined || target === undefined || source.version !== version) {
        return yield* new ReplicaError.SchemaEvolutionUnsupported({
          sourceVersion: options.source.version,
          sourceHash: options.source.hash,
          targetVersion: options.evolution.current.schemaIdentity.version,
          targetHash: options.evolution.current.schemaIdentity.hash
        })
      }
      const migration = entry.models.get(options.model)
      const sourceKey = yield* Schema.decodeUnknownEffect(source.key)(key).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.SchemaEvolutionFailed({
            stepId: entry.id,
            componentKind: "Model",
            componentName: options.model,
            part: "Key",
            fromVersion: source.version,
            toVersion: target.version,
            cause
          })
        )
      )
      let migratedKey = sourceKey
      if (migration?.migrateKey !== undefined) migratedKey = migration.migrateKey(sourceKey)
      const targetKey = yield* Schema.decodeUnknownEffect(Schema.toType(target.key))(migratedKey).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.SchemaEvolutionFailed({
            stepId: entry.id,
            componentKind: "Model",
            componentName: options.model,
            part: "Key",
            fromVersion: source.version,
            toVersion: target.version,
            cause
          })
        )
      )
      key = yield* Schema.encodeUnknownEffect(target.key)(targetKey).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.SchemaEvolutionFailed({
            stepId: entry.id,
            componentKind: "Model",
            componentName: options.model,
            part: "Key",
            fromVersion: source.version,
            toVersion: target.version,
            cause
          })
        ),
        Effect.flatMap((encoded) =>
          Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.SchemaEvolutionFailed({
                stepId: entry.id,
                componentKind: "Model",
                componentName: options.model,
                part: "Key",
                fromVersion: source.version,
                toVersion: target.version,
                cause
              })
            )
          )
        )
      )
      if (value !== undefined) {
        const sourceValue = yield* Schema.decodeUnknownEffect(source.schema)(value).pipe(
          Effect.mapError((cause) =>
            new ReplicaError.SchemaEvolutionFailed({
              stepId: entry.id,
              componentKind: "Model",
              componentName: options.model,
              part: "Value",
              fromVersion: source.version,
              toVersion: target.version,
              cause
            })
          )
        )
        let migratedValue = sourceValue
        if (migration?.migrateValue !== undefined) {
          migratedValue = migration.migrateValue({ key: sourceKey, targetKey, value: sourceValue })
        }
        const targetValue = yield* Schema.decodeUnknownEffect(Schema.toType(target.schema))(migratedValue).pipe(
          Effect.mapError((cause) =>
            new ReplicaError.SchemaEvolutionFailed({
              stepId: entry.id,
              componentKind: "Model",
              componentName: options.model,
              part: "Value",
              fromVersion: source.version,
              toVersion: target.version,
              cause
            })
          )
        )
        value = yield* Schema.encodeUnknownEffect(target.schema)(targetValue).pipe(
          Effect.mapError((cause) =>
            new ReplicaError.SchemaEvolutionFailed({
              stepId: entry.id,
              componentKind: "Model",
              componentName: options.model,
              part: "Value",
              fromVersion: source.version,
              toVersion: target.version,
              cause
            })
          ),
          Effect.flatMap((encoded) =>
            Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.SchemaEvolutionFailed({
                  stepId: entry.id,
                  componentKind: "Model",
                  componentName: options.model,
                  part: "Value",
                  fromVersion: source.version,
                  toVersion: target.version,
                  cause
                })
              )
            )
          )
        )
      }
      version = target.version
      identity = entry.to.schemaIdentity
      aliases.push({ schemaIdentity: identity, modelVersion: version, key })
    }
    return {
      schemaIdentity: options.evolution.current.schemaIdentity,
      modelVersion: version,
      key,
      value,
      aliases
    }
  })

const migrateMutationPart = (options: {
  readonly evolution: Evolution
  readonly source: Identity.SchemaIdentity
  readonly mutation: string
  readonly mutationVersion: Identity.SchemaVersion
  readonly value: typeof Schema.Json.Type
  readonly part: "Payload" | "Success" | "Rejection"
}): Effect.Effect<
  MigratedMutationValue,
  ReplicaError.SchemaEvolutionUnsupported | ReplicaError.SchemaEvolutionFailed
> =>
  Effect.gen(function*() {
    const path = yield* pathFrom(options.evolution, options.source)
    const sourceDefinition = options.evolution.definitionByIdentity.get(identityKey(options.source))
    const sourceMutation = sourceDefinition?.mutationByName.get(options.mutation)
    if (sourceMutation === undefined || sourceMutation.version !== options.mutationVersion) {
      return yield* new ReplicaError.SchemaEvolutionUnsupported({
        sourceVersion: options.source.version,
        sourceHash: options.source.hash,
        targetVersion: options.evolution.current.schemaIdentity.version,
        targetHash: options.evolution.current.schemaIdentity.hash
      })
    }
    let value = options.value
    let version = options.mutationVersion
    for (const entry of path) {
      const source = entry.from.mutationByName.get(options.mutation)
      const target = entry.to.mutationByName.get(options.mutation)
      if (source === undefined || target === undefined || source.version !== version) {
        return yield* new ReplicaError.SchemaEvolutionUnsupported({
          sourceVersion: options.source.version,
          sourceHash: options.source.hash,
          targetVersion: options.evolution.current.schemaIdentity.version,
          targetHash: options.evolution.current.schemaIdentity.hash
        })
      }
      const migration = entry.mutations.get(options.mutation)
      let fromSchema: SchemaInput.WireSchema
      let toSchema: SchemaInput.WireSchema
      let migrate: (input: unknown) => unknown
      switch (options.part) {
        case "Payload":
          fromSchema = source.payloadSchema
          toSchema = target.payloadSchema
          if (migration?.migratePayload === undefined) migrate = (input) => input
          else migrate = migration.migratePayload.bind(undefined)
          break
        case "Success":
          fromSchema = source.successSchema
          toSchema = target.successSchema
          if (migration?.migrateSuccess === undefined) migrate = (input) => input
          else migrate = migration.migrateSuccess.bind(undefined)
          break
        case "Rejection":
          fromSchema = source.rejectionSchema
          toSchema = target.rejectionSchema
          if (migration?.migrateRejection === undefined) migrate = (input) => input
          else migrate = migration.migrateRejection.bind(undefined)
          break
      }
      const sourceValue = yield* Schema.decodeUnknownEffect(fromSchema)(value).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.SchemaEvolutionFailed({
            stepId: entry.id,
            componentKind: "Mutation",
            componentName: options.mutation,
            part: options.part,
            fromVersion: source.version,
            toVersion: target.version,
            cause
          })
        )
      )
      const migrated = migrate(sourceValue)
      const targetValue = yield* Schema.decodeUnknownEffect(Schema.toType(toSchema))(migrated).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.SchemaEvolutionFailed({
            stepId: entry.id,
            componentKind: "Mutation",
            componentName: options.mutation,
            part: options.part,
            fromVersion: source.version,
            toVersion: target.version,
            cause
          })
        )
      )
      value = yield* Schema.encodeUnknownEffect(toSchema)(targetValue).pipe(
        Effect.mapError((cause) =>
          new ReplicaError.SchemaEvolutionFailed({
            stepId: entry.id,
            componentKind: "Mutation",
            componentName: options.mutation,
            part: options.part,
            fromVersion: source.version,
            toVersion: target.version,
            cause
          })
        ),
        Effect.flatMap((encoded) =>
          Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.SchemaEvolutionFailed({
                stepId: entry.id,
                componentKind: "Mutation",
                componentName: options.mutation,
                part: options.part,
                fromVersion: source.version,
                toVersion: target.version,
                cause
              })
            )
          )
        )
      )
      version = target.version
    }
    return {
      schemaIdentity: options.evolution.current.schemaIdentity,
      mutationVersion: version,
      value
    }
  })

export const migrateMutationPayload = (options: Omit<Parameters<typeof migrateMutationPart>[0], "part">) =>
  migrateMutationPart({ ...options, part: "Payload" })

export const migrateMutationSuccess = (options: Omit<Parameters<typeof migrateMutationPart>[0], "part">) =>
  migrateMutationPart({ ...options, part: "Success" })

export const migrateMutationRejection = (options: Omit<Parameters<typeof migrateMutationPart>[0], "part">) =>
  migrateMutationPart({ ...options, part: "Rejection" })
