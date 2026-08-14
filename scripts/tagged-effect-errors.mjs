import { SyntaxKind } from "@typescript/native-preview/unstable/ast"
import {
  isArrayLiteralExpression,
  isArrowFunction,
  isBinaryExpression,
  isBindingElement,
  isCallExpression,
  isClassDeclaration,
  isClassExpression,
  isComputedPropertyName,
  isConstructorDeclaration,
  isElementAccessExpression,
  isExpression,
  isFunctionDeclaration,
  isFunctionExpression,
  isIdentifier,
  isImportDeclaration,
  isImportSpecifier,
  isMethodDeclaration,
  isNamedImports,
  isNamespaceImport,
  isNewExpression,
  isNoSubstitutionTemplateLiteral,
  isObjectBindingPattern,
  isObjectLiteralExpression,
  isOuterExpression,
  isParameterDeclaration,
  isPrivateIdentifier,
  isPropertyAccessExpression,
  isPropertyAssignment,
  isPropertyDeclaration,
  isReturnStatement,
  isShorthandPropertyAssignment,
  isStringLiteral,
  isTaggedTemplateExpression,
  isTypeNode,
  isVariableDeclaration
} from "@typescript/native-preview/unstable/ast/is"
import { API, ModifierFlags, SignatureKind, SymbolFlags, TypeFlags } from "@typescript/native-preview/unstable/sync"
import { globSync } from "glob"
import { realpathSync } from "node:fs"
import { basename, dirname, resolve, sep } from "node:path"

const effectMarker = "~effect/Effect"
const layerMarker = "~effect/Layer"
const serviceMarker = "~effect/Context/Service"
const managedRuntimeMarker = "~effect/ManagedRuntime"
const sqlClientMarker = "~effect/sql/SqlClient"

export const makeEffectTypePolicyChecker = ({ cwd }) => {
  const effectUnhandledDeclaration = realpathSync(resolve(cwd, "node_modules/effect/dist/Types.d.ts"))
  const effectUnhandledDeclarations = new Set([
    effectUnhandledDeclaration,
    effectUnhandledDeclaration.toLowerCase()
  ])
  const effectSocketDeclaration = realpathSync(
    resolve(cwd, "node_modules/effect/dist/unstable/socket/Socket.d.ts")
  )
  const effectSocketDeclarations = new Set([
    effectSocketDeclaration,
    effectSocketDeclaration.toLowerCase()
  ])
  const layerDeclaration = realpathSync(resolve(cwd, "node_modules/effect/dist/Layer.d.ts"))
  const layerDeclarations = new Set([layerDeclaration, layerDeclaration.toLowerCase()])
  const contextDeclaration = realpathSync(resolve(cwd, "node_modules/effect/dist/Context.d.ts"))
  const contextDeclarations = new Set([contextDeclaration, contextDeclaration.toLowerCase()])
  const effectDeclaration = realpathSync(resolve(cwd, "node_modules/effect/dist/Effect.d.ts"))
  const effectDeclarations = new Set([effectDeclaration, effectDeclaration.toLowerCase()])
  const functionDeclaration = realpathSync(resolve(cwd, "node_modules/effect/dist/Function.d.ts"))
  const functionDeclarations = new Set([functionDeclaration, functionDeclaration.toLowerCase()])
  const managedRuntimeDeclaration = realpathSync(
    resolve(cwd, "node_modules/effect/dist/ManagedRuntime.d.ts")
  )
  const managedRuntimeDeclarations = new Set([
    managedRuntimeDeclaration,
    managedRuntimeDeclaration.toLowerCase()
  ])
  const schemaDeclaration = realpathSync(resolve(cwd, "node_modules/effect/dist/Schema.d.ts"))
  const schemaDeclarations = new Set([schemaDeclaration, schemaDeclaration.toLowerCase()])
  const sqlClientDeclaration = realpathSync(
    resolve(cwd, "node_modules/effect/dist/unstable/sql/SqlClient.d.ts")
  )
  const sqlClientDeclarations = new Set([sqlClientDeclaration, sqlClientDeclaration.toLowerCase()])
  const sourceOverrides = new Map()
  let openFilename
  const api = new API({
    cwd,
    fs: {
      readFile: (candidate) => {
        const absoluteCandidate = resolve(candidate)
        if (sourceOverrides.has(absoluteCandidate)) return sourceOverrides.get(absoluteCandidate)
        return undefined
      }
    }
  })

  const analysisCache = new Map()
  const sharedConstructorDecisions = new Map()
  const analyze = ({ filename, sourceText }) => {
    const cachedAnalysis = analysisCache.get(filename)
    if (cachedAnalysis?.sourceText === sourceText) return cachedAnalysis.result
    const absoluteFilename = resolve(cwd, filename)
    sourceOverrides.set(absoluteFilename, sourceText)
    let snapshot
    try {
      const previousFilename = openFilename
      let update
      if (previousFilename === absoluteFilename) {
        const closingSnapshot = api.updateSnapshot({
          closeFiles: [absoluteFilename],
          fileChanges: { changed: [absoluteFilename] }
        })
        closingSnapshot.dispose()
        openFilename = undefined
        update = { openFiles: [absoluteFilename] }
      } else if (previousFilename === undefined) {
        update = { openFiles: [absoluteFilename] }
      } else {
        update = { openFiles: [absoluteFilename], closeFiles: [previousFilename] }
      }
      snapshot = api.updateSnapshot(update)
      openFilename = absoluteFilename
      const project = snapshot.getDefaultProjectForFile(absoluteFilename)
      if (project === undefined) throw new Error(`No TypeScript project contains ${absoluteFilename}`)
      const sourceFile = project.program.getSourceFile(absoluteFilename)
      if (sourceFile === undefined) throw new Error(`TypeScript did not load ${absoluteFilename}`)
      const checker = project.checker
      const expressions = []
      const layerValueCandidates = []
      const calls = []
      const binaryExpressions = []
      const classes = []
      const functionLikes = []
      const exportedNames = new Set()
      const taggedTemplates = []
      const importDeclarations = []
      const objectBindingPatterns = []
      const addLayerPropertyName = (name) => {
        if (isIdentifier(name) || isStringLiteral(name) || isNoSubstitutionTemplateLiteral(name)) {
          layerValueCandidates.push({ name: name.text, node: name, typeNode: name })
          return
        }
        if (isPrivateIdentifier(name)) {
          layerValueCandidates.push({ name: name.text.replace(/^#/, ""), node: name, typeNode: name })
          return
        }
        if (!isComputedPropertyName(name)) return
        const expression = name.expression
        if (!isStringLiteral(expression) && !isNoSubstitutionTemplateLiteral(expression)) return
        layerValueCandidates.push({ name: expression.text, node: expression, typeNode: name })
      }
      const visit = (node) => {
        const parameterProperty = isParameterDeclaration(node) &&
          isConstructorDeclaration(node.parent) &&
          (node.modifierFlags & ModifierFlags.ParameterPropertyModifier) !== 0
        if (
          (isVariableDeclaration(node) || isBindingElement(node) || parameterProperty) &&
          node.name !== undefined &&
          isIdentifier(node.name)
        ) {
          addLayerPropertyName(node.name)
        }
        if (
          isPropertyDeclaration(node) &&
          (node.initializer === undefined || !isArrayLiteralExpression(node.initializer))
        ) {
          addLayerPropertyName(node.name)
        }
        if (
          isPropertyAssignment(node) &&
          !isArrayLiteralExpression(node.initializer)
        ) {
          addLayerPropertyName(node.name)
        }
        if (isShorthandPropertyAssignment(node) && isIdentifier(node.name)) {
          addLayerPropertyName(node.name)
        }
        if (isCallExpression(node)) calls.push(node)
        if (isBinaryExpression(node)) binaryExpressions.push(node)
        if (isClassDeclaration(node) || isClassExpression(node)) classes.push(node)
        if (
          isArrowFunction(node) || isFunctionDeclaration(node) || isFunctionExpression(node) ||
          isMethodDeclaration(node)
        ) {
          functionLikes.push(node)
        }
        if (isImportDeclaration(node)) importDeclarations.push(node)
        if (isTaggedTemplateExpression(node)) taggedTemplates.push(node)
        if (isObjectBindingPattern(node)) objectBindingPatterns.push(node)
        if (
          node.kind === SyntaxKind.ExportDeclaration && node.exportClause?.elements !== undefined &&
          node.moduleSpecifier === undefined
        ) {
          for (const element of node.exportClause.elements) {
            exportedNames.add(element.propertyName?.text ?? element.name.text)
          }
        }
        const typeNode = isTypeNode(node)
        const namedNode = isIdentifier(node) && node.parent.name === node
        const literalContainer = isArrayLiteralExpression(node) || isObjectLiteralExpression(node)
        if (
          (isExpression(node) || typeNode) &&
          !namedNode &&
          !literalContainer
        ) {
          expressions.push(node)
        }
        node.forEachChild(visit)
      }
      visit(sourceFile)

      const expressionTypes = checker.getTypeAtLocation(expressions)
      const typeByExpression = new Map()
      for (let index = 0; index < expressions.length; index++) {
        const type = expressionTypes[index]
        if (type !== undefined) typeByExpression.set(expressions[index], type)
      }
      const functionLikeTypes = checker.getTypeAtLocation(functionLikes)
      for (let index = 0; index < functionLikes.length; index++) {
        const type = functionLikeTypes[index]
        if (type !== undefined) typeByExpression.set(functionLikes[index], type)
      }

      const effectChannelByType = new Map()
      const getEffectChannel = (type, expression, channel) => {
        const cacheKey = `${type.id}:${channel}`
        if (effectChannelByType.has(cacheKey)) return effectChannelByType.get(cacheKey)
        const markerSymbol = checker.getPropertyOfType(type, effectMarker)
        if (markerSymbol === undefined) {
          effectChannelByType.set(cacheKey, undefined)
          return undefined
        }
        const markerType = checker.getTypeOfSymbolAtLocation(markerSymbol, expression)
        const channelSymbol = checker.getPropertyOfType(markerType, channel)
        if (channelSymbol === undefined) {
          throw new Error(`The Effect marker on ${absoluteFilename} has no ${channel} channel`)
        }
        const channelAccessor = checker.getTypeOfSymbolAtLocation(channelSymbol, expression)
        const signatures = checker.getSignaturesOfType(channelAccessor, SignatureKind.Call)
        if (signatures.length !== 1) {
          throw new Error(`The Effect ${channel} marker on ${absoluteFilename} is not a single callable signature`)
        }
        const channelType = checker.getReturnTypeOfSignature(signatures[0])
        if (channelType === undefined) {
          throw new Error(`TypeScript could not resolve an Effect ${channel} channel in ${absoluteFilename}`)
        }
        effectChannelByType.set(cacheKey, channelType)
        return channelType
      }
      const getEffectError = (type, expression) => getEffectChannel(type, expression, "_E")

      const untaggedByType = new Map()
      const isEffectUnhandled = (type) => {
        const symbols = [type.getSymbol(), type.getAliasSymbol()].filter((symbol) => symbol !== undefined)
        return symbols.some((symbol) =>
          symbol.name === "unhandled" &&
          symbol.declarations.some((declaration) => effectUnhandledDeclarations.has(declaration.path))
        )
      }
      const isEffectSocketRunRawError = (type) => {
        const symbol = type.getSymbol()
        if (symbol?.name !== "E") return false
        return symbol.declarations.some((declaration) => {
          if (!effectSocketDeclarations.has(declaration.path)) return false
          const node = declaration.resolve(project)
          const runRaw = node?.parent?.parent
          const options = runRaw?.parent?.parent
          const make = options?.parent?.parent
          return runRaw?.name?.text === "runRaw" && options?.name?.text === "options" && make?.name?.text === "make"
        })
      }
      const getUntaggedMembers = (type, seen = new Set()) => {
        if (untaggedByType.has(type.id)) return untaggedByType.get(type.id)
        if (seen.has(type.id)) return [type]
        seen.add(type.id)

        let untagged
        if ((type.flags & (TypeFlags.Never | TypeFlags.Unknown)) !== 0 || isEffectUnhandled(type)) {
          untagged = []
        } else if ((type.flags & TypeFlags.Union) !== 0) {
          untagged = []
          const members = type.getTypes()
          for (const member of members) {
            untagged.push(...getUntaggedMembers(member, seen))
          }
        } else if ((type.flags & TypeFlags.TypeParameter) !== 0) {
          if (isEffectSocketRunRawError(type)) {
            untagged = []
          } else {
            const constraint = checker.getBaseConstraintOfType(type)
            if (constraint === undefined) {
              untagged = [type]
            } else {
              untagged = getUntaggedMembers(constraint, seen)
            }
          }
        } else {
          const tag = checker.getPropertyOfType(type, "_tag")
          if (tag === undefined || (tag.flags & SymbolFlags.Optional) !== 0) {
            untagged = [type]
          } else {
            untagged = []
          }
        }

        seen.delete(type.id)
        untaggedByType.set(type.id, untagged)
        return untagged
      }

      const violationByExpression = new Map()
      for (const expression of expressions) {
        const type = typeByExpression.get(expression)
        if (type === undefined) continue
        const errorType = getEffectError(type, expression)
        if (errorType === undefined) continue
        const untagged = getUntaggedMembers(errorType)
        if (untagged.length === 0) continue
        const memberIds = new Set(untagged.map((member) => member.id))
        violationByExpression.set(expression, { errorType, memberIds, untagged })
      }

      const descendantMemberIdsByExpression = new Map()
      const collectViolationMemberIds = (node) => {
        let descendantMemberIds
        node.forEachChild((child) => {
          const childMemberIds = collectViolationMemberIds(child)
          if (childMemberIds === undefined) return
          descendantMemberIds ??= new Set()
          for (const memberId of childMemberIds) descendantMemberIds.add(memberId)
        })

        const violation = violationByExpression.get(node)
        if (violation === undefined) return descendantMemberIds
        if (descendantMemberIds !== undefined) {
          descendantMemberIdsByExpression.set(node, new Set(descendantMemberIds))
        }
        const subtreeMemberIds = descendantMemberIds ?? new Set()
        for (const memberId of violation.memberIds) subtreeMemberIds.add(memberId)
        return subtreeMemberIds
      }
      collectViolationMemberIds(sourceFile)

      const unknownChannelByExpression = new Map()
      for (const expression of expressions) {
        const type = typeByExpression.get(expression)
        if (type === undefined) continue
        const errorType = getEffectChannel(type, expression, "_E")
        if (errorType === undefined) continue
        const requirementsType = getEffectChannel(type, expression, "_R")
        if (requirementsType === undefined) continue
        let mask = 0
        if ((errorType.flags & TypeFlags.Unknown) !== 0) mask |= 1
        if ((requirementsType.flags & TypeFlags.Unknown) !== 0) mask |= 2
        if (mask !== 0) unknownChannelByExpression.set(expression, mask)
      }
      const descendantUnknownMaskByExpression = new Map()
      const collectUnknownChannelMask = (node) => {
        let descendantMask = 0
        node.forEachChild((child) => {
          descendantMask |= collectUnknownChannelMask(child)
        })
        const mask = unknownChannelByExpression.get(node) ?? 0
        if (mask !== 0 && descendantMask !== 0) {
          descendantUnknownMaskByExpression.set(node, descendantMask)
        }
        return mask | descendantMask
      }
      collectUnknownChannelMask(sourceFile)

      const unknownEffectChannels = []
      for (const expression of expressions) {
        const mask = unknownChannelByExpression.get(expression) ?? 0
        const innermostMask = mask & ~(descendantUnknownMaskByExpression.get(expression) ?? 0)
        if (innermostMask === 0) continue
        const channels = []
        if ((innermostMask & 1) !== 0) channels.push("error")
        if ((innermostMask & 2) !== 0) channels.push("requirements")
        unknownEffectChannels.push({
          start: expression.getStart(sourceFile),
          end: expression.getEnd(),
          channels
        })
      }

      const taggedEffectErrors = []
      for (const expression of expressions) {
        const violation = violationByExpression.get(expression)
        if (violation === undefined) continue

        const descendantMemberIds = descendantMemberIdsByExpression.get(expression)
        const innermostMembers = violation.untagged.filter(
          (member) => descendantMemberIds?.has(member.id) !== true
        )
        if (innermostMembers.length === 0) continue

        const names = innermostMembers.map((member) => checker.typeToString(member))
        taggedEffectErrors.push({
          start: expression.getStart(sourceFile),
          end: expression.getEnd(),
          errorTypes: [...new Set(names)].toSorted((left, right) => left.localeCompare(right))
        })
      }
      const invalidLayerValueCandidates = layerValueCandidates.filter(
        (candidate) => !/^layer(?:[A-Z][A-Za-z0-9]*)?$/.test(candidate.name)
      )
      const layerNameTypes = checker.getTypeAtLocation(
        invalidLayerValueCandidates.map((candidate) => candidate.typeNode)
      )
      const layerByType = new Map()
      const hasOfficialLayerMarker = (type, seen = new Set()) => {
        if (layerByType.has(type.id)) return layerByType.get(type.id)
        if (seen.has(type.id)) return false
        seen.add(type.id)
        let result = false
        if ((type.flags & (TypeFlags.Any | TypeFlags.Unknown)) === 0) {
          if ((type.flags & TypeFlags.Union) !== 0) {
            const members = type.getTypes()
            result = members.length > 0 && members.every((member) => hasOfficialLayerMarker(member, seen))
          } else if ((type.flags & TypeFlags.Intersection) !== 0) {
            result = type.getTypes().some((member) => hasOfficialLayerMarker(member, seen))
          } else {
            const marker = checker.getPropertyOfType(type, layerMarker)
            result = marker?.declarations.some((declaration) => {
              if (!layerDeclarations.has(declaration.path)) return false
              const declarationNode = declaration.resolve(project)
              return declarationNode?.parent?.name?.text === "Variance"
            }) === true
            if (!result && (type.flags & (TypeFlags.TypeParameter | TypeFlags.Conditional)) !== 0) {
              const constraint = checker.getBaseConstraintOfType(type)
              if (constraint !== undefined) result = hasOfficialLayerMarker(constraint, seen)
            }
          }
        }
        seen.delete(type.id)
        layerByType.set(type.id, result)
        return result
      }
      const layerNameViolations = []
      for (let index = 0; index < invalidLayerValueCandidates.length; index++) {
        const candidate = invalidLayerValueCandidates[index]
        const type = layerNameTypes[index]
        if (type === undefined) continue
        if (!hasOfficialLayerMarker(type)) continue
        layerNameViolations.push({
          start: candidate.node.getStart(sourceFile),
          end: candidate.node.getEnd(),
          name: candidate.name
        })
      }

      const unwrapNative = (node) => {
        let expression = node
        while (isOuterExpression(expression)) expression = expression.expression
        return expression
      }
      const hasDeclarationFrom = (symbol, name, declarations) => {
        if (symbol === undefined) return false
        let resolved = symbol
        if ((symbol.flags & SymbolFlags.Alias) !== 0) resolved = checker.getAliasedSymbol(symbol)
        return resolved.name === name && resolved.declarations.some((declaration) => declarations.has(declaration.path))
      }
      const isOfficialCallee = (node, name, declarations, seen = new Set()) => {
        const expression = unwrapNative(node)
        const symbol = checker.getSymbolAtLocation(expression)
        if (hasDeclarationFrom(symbol, name, declarations)) return true
        if (symbol === undefined || seen.has(symbol.id)) return false
        seen.add(symbol.id)
        const aliasesOfficial = symbol.declarations.some((declaration) => {
          const declarationNode = declaration.resolve(project)
          if (!isVariableDeclaration(declarationNode) || (declarationNode.parent.flags & 2) === 0) {
            return false
          }
          return declarationNode.initializer !== undefined &&
            isOfficialCallee(declarationNode.initializer, name, declarations, seen)
        })
        seen.delete(symbol.id)
        return aliasesOfficial
      }
      const isNativePipeMember = (node) => {
        const expression = unwrapNative(node)
        if (isPropertyAccessExpression(expression)) return expression.name.text === "pipe"
        if (!isElementAccessExpression(expression)) return false
        const argument = unwrapNative(expression.argumentExpression)
        return argument !== undefined &&
          (isStringLiteral(argument) || isNoSubstitutionTemplateLiteral(argument)) &&
          argument.text === "pipe"
      }
      const nativeMemberObject = (node) => {
        const expression = unwrapNative(node)
        if (isPropertyAccessExpression(expression) || isElementAccessExpression(expression)) {
          return expression.expression
        }
        return undefined
      }
      const nativeStaticName = (node) => {
        const expression = unwrapNative(node)
        if (isPropertyAccessExpression(expression)) return expression.name.text
        if (!isElementAccessExpression(expression)) return undefined
        const argument = unwrapNative(expression.argumentExpression)
        if (argument === undefined) return undefined
        if (isStringLiteral(argument) || isNoSubstitutionTemplateLiteral(argument)) return argument.text
        if (
          isPropertyAccessExpression(argument) && argument.name.text === "asyncDispose" &&
          isIdentifier(argument.expression) && argument.expression.text === "Symbol"
        ) return "asyncDispose"
        return undefined
      }
      const isOfficialService = (node) => {
        const type = checker.getTypeAtLocation(unwrapNative(node))
        if (type === undefined) return false
        const marker = checker.getPropertyOfType(type, serviceMarker)
        return marker?.declarations.some((declaration) => contextDeclarations.has(declaration.path)) === true
      }
      const isMapOperator = (node) => {
        const expression = unwrapNative(node)
        return isCallExpression(expression) && isOfficialCallee(expression.expression, "map", effectDeclarations)
      }
      const serviceTagMaps = []
      const seenServiceStarts = new Set()
      const addService = (node) => {
        const service = unwrapNative(node)
        if (!isOfficialService(service)) return
        const start = service.getStart(sourceFile)
        if (seenServiceStarts.has(start)) return
        seenServiceStarts.add(start)
        serviceTagMaps.push({ start, end: service.getEnd() })
      }
      for (const call of calls) {
        const callee = unwrapNative(call.expression)
        if (isOfficialCallee(callee, "map", effectDeclarations) && call.arguments.length === 2) {
          addService(call.arguments[0])
          continue
        }
        if (isOfficialCallee(callee, "pipe", functionDeclarations)) {
          if (call.arguments.length >= 2 && isMapOperator(call.arguments[1])) addService(call.arguments[0])
          continue
        }
        if (isNativePipeMember(callee) && call.arguments.length >= 1 && isMapOperator(call.arguments[0])) {
          addService(nativeMemberObject(callee))
        }
      }

      const managedRuntimeBoundaryNames = new Set([
        "runFork",
        "runCallback",
        "runSyncExit",
        "runSync",
        "runPromiseExit",
        "runPromise",
        "context",
        "dispose",
        "asyncDispose"
      ])
      const isOfficialManagedRuntime = (node) => {
        const expression = unwrapNative(node)
        const type = typeByExpression.get(expression)
        if (type === undefined || (type.flags & (TypeFlags.Any | TypeFlags.Unknown)) !== 0) return false
        const marker = checker.getPropertyOfType(type, managedRuntimeMarker)
        return marker?.declarations.some((declaration) => managedRuntimeDeclarations.has(declaration.path)) === true
      }
      const manualRuntimeBoundaries = []
      const seenRuntimeBoundaryStarts = new Set()
      const addRuntimeBoundary = (node) => {
        const start = node.getStart(sourceFile)
        if (seenRuntimeBoundaryStarts.has(start)) return
        seenRuntimeBoundaryStarts.add(start)
        manualRuntimeBoundaries.push({ start, end: node.getEnd() })
      }
      for (const expression of expressions) {
        const name = nativeStaticName(expression)
        if (name === undefined || !managedRuntimeBoundaryNames.has(name)) continue
        const object = nativeMemberObject(expression)
        if (object !== undefined && isOfficialManagedRuntime(object)) addRuntimeBoundary(expression)
      }
      const objectBindingPatternTypes = checker.getTypeAtLocation(objectBindingPatterns)
      for (let index = 0; index < objectBindingPatterns.length; index++) {
        const pattern = objectBindingPatterns[index]
        const type = objectBindingPatternTypes[index]
        if (type === undefined || (type.flags & (TypeFlags.Any | TypeFlags.Unknown)) !== 0) continue
        const marker = checker.getPropertyOfType(type, managedRuntimeMarker)
        const official = marker?.declarations.some((declaration) =>
          managedRuntimeDeclarations.has(declaration.path)
        ) === true
        if (!official) continue
        for (const element of pattern.elements) {
          if (element.dotDotDotToken !== undefined || !isIdentifier(element.name)) continue
          const property = element.propertyName ?? element.name
          let name
          if (isIdentifier(property) || isStringLiteral(property) || isNoSubstitutionTemplateLiteral(property)) {
            name = property.text
          }
          if (name !== undefined && managedRuntimeBoundaryNames.has(name)) addRuntimeBoundary(element.name)
        }
      }

      const isEffectType = (type) => {
        if (type === undefined || (type.flags & (TypeFlags.Any | TypeFlags.Unknown)) !== 0) return false
        if (checker.getPropertyOfType(type, effectMarker) !== undefined) return true
        if ((type.flags & TypeFlags.Union) === 0) return false
        return (type.getTypes() ?? []).some(isEffectType)
      }
      const getEffectSuccess = (type, location) => {
        if (!isEffectType(type)) return undefined
        const marker = checker.getPropertyOfType(type, effectMarker)
        if (marker === undefined) return undefined
        const markerType = checker.getTypeOfSymbolAtLocation(marker, location)
        const success = checker.getPropertyOfType(markerType, "_A")
        if (success === undefined) return undefined
        const accessor = checker.getTypeOfSymbolAtLocation(success, location)
        const signatures = checker.getSignaturesOfType(accessor, SignatureKind.Call)
        if (signatures.length !== 1) return undefined
        return checker.getReturnTypeOfSignature(signatures[0])
      }

      const effectSyncReturnsEffect = []
      for (const call of calls) {
        if (!isOfficialCallee(call.expression, "sync", effectDeclarations)) continue
        const thunk = call.arguments[0]
        if (thunk === undefined) continue
        const thunkType = typeByExpression.get(thunk)
        if (thunkType === undefined) continue
        const signatures = checker.getSignaturesOfType(thunkType, SignatureKind.Call)
        if (signatures.length === 0) continue
        const returnType = checker.getReturnTypeOfSignature(signatures[0])
        if (!isEffectType(returnType)) continue
        effectSyncReturnsEffect.push({ start: call.getStart(sourceFile), end: call.getEnd() })
      }

      const effectValuedPlainTests = []
      const testBindings = new Map()
      const testNamespaces = new Map()
      for (const declaration of importDeclarations) {
        if (!isStringLiteral(declaration.moduleSpecifier)) continue
        const source = declaration.moduleSpecifier.text
        if (source !== "vitest" && source !== "@effect/vitest") continue
        const bindings = declaration.importClause?.namedBindings
        if (bindings === undefined) continue
        if (isNamespaceImport(bindings)) {
          const symbol = checker.getSymbolAtLocation(bindings.name)
          if (symbol !== undefined) testNamespaces.set(symbol.id, source)
          continue
        }
        if (!isNamedImports(bindings)) continue
        for (const specifier of bindings.elements) {
          if (!isImportSpecifier(specifier)) continue
          const imported = specifier.propertyName?.text ?? specifier.name.text
          if (imported !== "it" && imported !== "test") continue
          const symbol = checker.getSymbolAtLocation(specifier.name)
          if (symbol !== undefined) testBindings.set(symbol.id, "plain")
        }
      }
      const testModifiers = new Set(["each", "fails", "only", "runIf", "skip", "skipIf", "todo"])
      const resolveTestKind = (node, seen = new Set()) => {
        const expression = unwrapNative(node)
        if (isCallExpression(expression)) return resolveTestKind(expression.expression, seen)
        if (isIdentifier(expression)) {
          const symbol = checker.getSymbolAtLocation(expression)
          if (symbol === undefined) return undefined
          const imported = testBindings.get(symbol.id)
          if (imported !== undefined) return imported
          if (seen.has(symbol.id)) return undefined
          seen.add(symbol.id)
          for (const declaration of symbol.declarations) {
            const declarationNode = declaration.resolve(project)
            if (isVariableDeclaration(declarationNode) && declarationNode.initializer !== undefined) {
              const kind = resolveTestKind(declarationNode.initializer, seen)
              if (kind !== undefined) return kind
            }
          }
          return undefined
        }
        if (!isPropertyAccessExpression(expression) && !isElementAccessExpression(expression)) return undefined
        const name = nativeStaticName(expression)
        if (name === undefined) return undefined
        const object = nativeMemberObject(expression)
        if (object === undefined) return undefined
        if (testModifiers.has(name)) return resolveTestKind(object, seen)
        const objectExpression = unwrapNative(object)
        if (isIdentifier(objectExpression)) {
          const symbol = checker.getSymbolAtLocation(objectExpression)
          let source
          if (symbol !== undefined) source = testNamespaces.get(symbol.id)
          if (source !== undefined && (name === "it" || name === "test")) return "plain"
          if (source === "@effect/vitest" && (name === "effect" || name === "live")) return name
        }
        const objectKind = resolveTestKind(object, seen)
        if (objectKind === "plain" && (name === "effect" || name === "live")) return name
        return undefined
      }
      for (const call of calls) {
        if (resolveTestKind(call.expression) !== "plain") continue
        const callback = call.arguments.find((argument) => {
          const callbackType = typeByExpression.get(argument)
          return callbackType !== undefined &&
            checker.getSignaturesOfType(callbackType, SignatureKind.Call).length > 0
        })
        if (callback === undefined) continue
        if (isArrowFunction(callback) && !isObjectLiteralExpression(callback.body)) {
          const bodyType = typeByExpression.get(callback.body)
          if (isEffectType(bodyType)) {
            effectValuedPlainTests.push({ start: call.getStart(sourceFile), end: call.getEnd() })
            continue
          }
        }
        const callbackType = typeByExpression.get(callback)
        if (callbackType === undefined) continue
        const signatures = checker.getSignaturesOfType(callbackType, SignatureKind.Call)
        if (signatures.length === 0) continue
        const returnType = checker.getReturnTypeOfSignature(signatures[0])
        if (!isEffectType(returnType)) continue
        effectValuedPlainTests.push({ start: call.getStart(sourceFile), end: call.getEnd() })
      }

      const hasRequiredLiteralTag = (type, location) => {
        if (type === undefined || (type.flags & (TypeFlags.Any | TypeFlags.Unknown)) !== 0) return false
        const tag = checker.getPropertyOfType(type, "_tag")
        if (tag === undefined || (tag.flags & SymbolFlags.Optional) !== 0) return false
        const tagType = checker.getTypeOfSymbolAtLocation(tag, location)
        return (tagType.flags & TypeFlags.StringLiteral) !== 0
      }
      const isErrorLike = (type) =>
        type !== undefined && checker.getPropertyOfType(type, "message") !== undefined &&
        checker.getPropertyOfType(type, "name") !== undefined

      const taggedErrorInstanceof = []
      for (const binary of binaryExpressions) {
        if (binary.operatorToken.kind !== SyntaxKind.InstanceOfKeyword) continue
        const constructorType = typeByExpression.get(binary.right)
        if (constructorType === undefined) continue
        const signatures = checker.getSignaturesOfType(constructorType, SignatureKind.Construct)
        if (signatures.length === 0) continue
        const instanceType = checker.getReturnTypeOfSignature(signatures[0])
        if (!hasRequiredLiteralTag(instanceType, binary.right)) continue
        taggedErrorInstanceof.push({ start: binary.getStart(sourceFile), end: binary.getEnd() })
      }

      const packageSource = /\/packages\/[^/]+\/src\//.test(
        resolve(absoluteFilename).split(sep).join("/")
      )
      const bareSqlRowTypes = []
      if (packageSource) {
        for (const template of taggedTemplates) {
          if (template.typeArguments === undefined || template.typeArguments.length === 0) continue
          const tagType = typeByExpression.get(template.tag)
          if (tagType === undefined) continue
          const marker = checker.getPropertyOfType(tagType, sqlClientMarker)
          const official = marker?.declarations.some((declaration) =>
            sqlClientDeclarations.has(declaration.path)
          ) === true
          const constructable = checker.getSignaturesOfType(tagType, SignatureKind.Call).some((signature) => {
            const result = checker.getReturnTypeOfSignature(signature)
            if (result === undefined) return false
            const resultMarker = checker.getPropertyOfType(result, sqlClientMarker)
            return resultMarker?.declarations.some((declaration) => sqlClientDeclarations.has(declaration.path)) ===
              true
          })
          if (!official && !constructable) continue
          bareSqlRowTypes.push({ start: template.getStart(sourceFile), end: template.getEnd() })
        }
      }

      const schemaTaggedErrorViolations = []
      if (packageSource) {
        for (const classNode of classes) {
          let classLocation = classNode.name
          if (
            classLocation === undefined && isVariableDeclaration(classNode.parent) &&
            isIdentifier(classNode.parent.name)
          ) classLocation = classNode.parent.name
          if (classLocation === undefined) continue
          let classType = checker.getTypeAtLocation(classLocation)
          const constructors = checker.getSignaturesOfType(classType, SignatureKind.Construct)
          if (constructors.length > 0) classType = checker.getReturnTypeOfSignature(constructors[0])
          if (!hasRequiredLiteralTag(classType, classLocation) || !isErrorLike(classType)) continue
          const extendsClause = classNode.heritageClauses?.find(
            (clause) => clause.token === SyntaxKind.ExtendsKeyword
          )
          const base = extendsClause?.types[0]?.expression
          let schemaTagged = false
          if (base !== undefined) {
            const inspectBase = (node) => {
              if (schemaTagged) return
              const symbol = checker.getSymbolAtLocation(node)
              if (hasDeclarationFrom(symbol, "TaggedErrorClass", schemaDeclarations)) {
                schemaTagged = true
                return
              }
              node.forEachChild(inspectBase)
            }
            inspectBase(base)
          }
          if (schemaTagged) continue
          schemaTaggedErrorViolations.push({
            start: classLocation.getStart(sourceFile),
            end: classLocation.getEnd()
          })
        }
      }

      const classInstanceFactories = []
      const resolvesToClass = (symbol, seen = new Set()) => {
        if (symbol === undefined || seen.has(symbol.id)) return false
        seen.add(symbol.id)
        let resolved = symbol
        if ((symbol.flags & SymbolFlags.Alias) !== 0) resolved = checker.getAliasedSymbol(symbol)
        return resolved.declarations.some((declaration) => {
          const declarationNode = declaration.resolve(project)
          if (declarationNode === undefined) return false
          if (isClassDeclaration(declarationNode) || isClassExpression(declarationNode)) return true
          if (!isVariableDeclaration(declarationNode) || declarationNode.initializer === undefined) return false
          const initializer = unwrapNative(declarationNode.initializer)
          if (isClassExpression(initializer)) return true
          return resolvesToClass(checker.getSymbolAtLocation(initializer), seen)
        })
      }
      const returnedNewExpression = (functionNode) => {
        if (isArrowFunction(functionNode) && !isObjectLiteralExpression(functionNode.body)) {
          const expression = unwrapNative(functionNode.body)
          if (isNewExpression(expression)) return expression
        }
        if (functionNode.body === undefined || functionNode.body.statements?.length !== 1) return undefined
        const statement = functionNode.body.statements[0]
        if (!isReturnStatement(statement) || statement.expression === undefined) return undefined
        const expression = unwrapNative(statement.expression)
        if (isNewExpression(expression)) return expression
        return undefined
      }
      const immediateConsumerCallback = (functionNode) => {
        let parent = functionNode.parent
        if (isPropertyAssignment(parent)) parent = parent.parent
        return isObjectLiteralExpression(parent) && isCallExpression(parent.parent) || isCallExpression(parent)
      }
      for (const functionNode of functionLikes) {
        const constructed = returnedNewExpression(functionNode)
        if (constructed === undefined || immediateConsumerCallback(functionNode)) continue
        const targetSymbol = checker.getSymbolAtLocation(constructed.expression)
        if (targetSymbol === undefined) continue
        if (!resolvesToClass(targetSymbol)) continue
        const instanceType = typeByExpression.get(constructed)
        if (hasRequiredLiteralTag(instanceType, constructed) && isErrorLike(instanceType)) continue
        classInstanceFactories.push({
          start: functionNode.getStart(sourceFile),
          end: functionNode.getEnd()
        })
      }

      const durationInputViolations = []
      const normalizedFilename = resolve(absoluteFilename).split(sep).join("/")
      const serializedShape = new Set(["Protocol.ts", "ReplicaError.ts", "Presence.ts"]).has(
        basename(absoluteFilename)
      )
      if (packageSource && !serializedShape) {
        const normalizedOutputNames = new Set()
        for (const functionNode of functionLikes) {
          if (functionNode.type === undefined || functionNode.body === undefined) continue
          let returnsObject = false
          const inspectReturns = (node) => {
            if (
              isReturnStatement(node) && node.expression !== undefined && isObjectLiteralExpression(node.expression)
            ) {
              returnsObject = true
              return
            }
            if (
              node !== functionNode &&
              (isArrowFunction(node) || isFunctionDeclaration(node) || isFunctionExpression(node) ||
                isMethodDeclaration(node))
            ) return
            node.forEachChild(inspectReturns)
          }
          inspectReturns(functionNode)
          if (!returnsObject) continue
          const collectNames = (node) => {
            if (isIdentifier(node)) normalizedOutputNames.add(node.text)
            node.forEachChild(collectNames)
          }
          collectNames(functionNode.type)
        }
        const isExportedFunction = (functionNode) => {
          if (isFunctionDeclaration(functionNode)) {
            return functionNode.parent === sourceFile &&
              (functionNode.modifierFlags & ModifierFlags.Export) !== 0
          }
          if (!isVariableDeclaration(functionNode.parent)) return false
          const statement = functionNode.parent.parent?.parent
          if (statement?.parent === sourceFile && (statement.modifierFlags & ModifierFlags.Export) !== 0) return true
          if (!isVariableDeclaration(functionNode.parent) || !isIdentifier(functionNode.parent.name)) return false
          return exportedNames.has(functionNode.parent.name.text)
        }
        const seenReachableTypes = new Set()
        const seenDurationProperties = new Set()
        const inspectDurationProperty = (property, location) => {
          if (seenDurationProperties.has(property.id)) return
          seenDurationProperties.add(property.id)
          if (!/(?:Millis|Milliseconds|Seconds|Minutes|Hours)$/.test(property.name)) return
          const propertyType = checker.getTypeOfSymbolAtLocation(property, location)
          let typeMembers = [propertyType]
          if ((propertyType.flags & TypeFlags.Union) !== 0) typeMembers = propertyType.getTypes() ?? []
          if (
            typeMembers.length === 0 ||
            !typeMembers.every((part) => (part.flags & (TypeFlags.NumberLike | TypeFlags.Undefined)) !== 0)
          ) return
          const declaration = property.declarations.find((candidate) => {
            const declarationNode = candidate.resolve(project)
            if (declarationNode === undefined) return false
            return /\/packages\/[^/]+\/src\//.test(
              resolve(declarationNode.getSourceFile().path).split(sep).join("/")
            )
          })
          const declarationNode = declaration?.resolve(project)
          const name = declarationNode?.name
          if (name === undefined || (!isIdentifier(name) && !isStringLiteral(name))) return
          if (declarationNode.getSourceFile() === sourceFile) {
            durationInputViolations.push({ start: name.getStart(sourceFile), end: name.getEnd() })
          } else {
            durationInputViolations.push({
              start: location.getStart(sourceFile),
              end: location.getEnd(),
              property: property.name
            })
          }
        }
        const collectReachableDeclarations = (type, location, root = false) => {
          if (type === undefined || seenReachableTypes.has(type.id)) return
          if (root && normalizedOutputNames.has(checker.typeToString(type))) return
          seenReachableTypes.add(type.id)
          for (const property of checker.getPropertiesOfType(type)) {
            inspectDurationProperty(property, location)
            let packageDeclaration = false
            for (const declaration of property.declarations) {
              const declarationNode = declaration.resolve(project)
              if (
                declarationNode !== undefined && /\/packages\/[^/]+\/src\//.test(
                  resolve(declarationNode.getSourceFile().path).split(sep).join("/")
                )
              ) {
                packageDeclaration = true
              }
            }
            if (!packageDeclaration) continue
            const propertyType = checker.getTypeOfSymbolAtLocation(property, location)
            if (
              (propertyType.flags &
                (TypeFlags.Any | TypeFlags.Unknown | TypeFlags.StringLike | TypeFlags.NumberLike |
                  TypeFlags.BooleanLike)) === 0
            ) {
              collectReachableDeclarations(propertyType, location)
            }
          }
        }
        for (const functionNode of functionLikes) {
          if (!isExportedFunction(functionNode)) continue
          for (const parameter of functionNode.parameters) {
            collectReachableDeclarations(checker.getTypeAtLocation(parameter), parameter, true)
          }
        }
      }

      const moduleErrorHelpers = []
      const moduleScope = (functionNode) => {
        if (isFunctionDeclaration(functionNode)) return functionNode.parent === functionNode.getSourceFile()
        let parent = functionNode.parent
        if (isMethodDeclaration(functionNode) && isObjectLiteralExpression(parent)) parent = parent.parent
        if (isPropertyAssignment(parent) && isObjectLiteralExpression(parent.parent)) parent = parent.parent.parent
        if (isVariableDeclaration(parent)) parent = parent.parent?.parent
        return parent?.parent === functionNode.getSourceFile()
      }
      const getTagName = (type, location) => {
        if (!hasRequiredLiteralTag(type, location) || !isErrorLike(type)) return undefined
        const tag = checker.getPropertyOfType(type, "_tag")
        if (tag === undefined) return undefined
        return checker.getTypeOfSymbolAtLocation(tag, location).value
      }
      const returnedExpression = (functionNode) => {
        if (
          isArrowFunction(functionNode) && functionNode.body.statements === undefined &&
          !isObjectLiteralExpression(functionNode.body)
        ) {
          return unwrapNative(functionNode.body)
        }
        if (
          functionNode.body === undefined || functionNode.body.statements === undefined ||
          functionNode.body.statements.length === 0 || functionNode.body.statements.length > 2
        ) return undefined
        const statement = functionNode.body.statements.at(-1)
        if (!isReturnStatement(statement) || statement.expression === undefined) return undefined
        if (
          functionNode.body.statements.length === 2 &&
          functionNode.body.statements[0].kind !== SyntaxKind.ExpressionStatement
        ) return undefined
        return unwrapNative(statement.expression)
      }
      const constructedTaggedError = (functionNode) => {
        const expression = returnedExpression(functionNode)
        if (expression === undefined) return undefined
        let found
        const inspect = (node) => {
          if (found !== undefined) return
          if (isNewExpression(node)) {
            const type = checker.getTypeAtLocation(node)
            const tag = getTagName(type, node)
            if (tag !== undefined) found = { tag, type }
            return
          }
          if (
            node !== expression &&
            (isArrowFunction(node) || isFunctionDeclaration(node) || isFunctionExpression(node) ||
              isMethodDeclaration(node))
          ) return
          node.forEachChild(inspect)
        }
        inspect(expression)
        return found
      }
      const functionTaggedError = (functionNode) => {
        const functionType = checker.getTypeAtLocation(functionNode)
        const signatures = checker.getSignaturesOfType(functionType, SignatureKind.Call)
        if (signatures.length > 0) {
          const returnType = checker.getReturnTypeOfSignature(signatures[0])
          const effectError = getEffectError(returnType, functionNode)
          const returnName = checker.typeToString(returnType)
          if (!isEffectType(returnType) && effectError === undefined && !returnName.includes("Effect<")) {
            const tag = getTagName(returnType, functionNode)
            if (tag !== undefined) return { tag, type: returnType }
          }
        }
        return constructedTaggedError(functionNode)
      }
      const sharedInternalConstructor = (functionNode, tagged) => {
        if (!normalizedFilename.includes("/src/internal/")) return false
        const sourceMatch = /^(.*\/packages\/[^/]+\/src\/)/.exec(normalizedFilename)
        if (sourceMatch === null) return false
        const sourceRoot = sourceMatch[1]
        if (!sharedConstructorDecisions.has(sourceRoot)) {
          const packageFiles = globSync("**/*.ts", { cwd: sourceRoot, absolute: true })
          const packageApi = new API({ cwd })
          const packageSnapshot = packageApi.updateSnapshot({ openFiles: packageFiles })
          try {
            const packageProject = packageSnapshot.getDefaultProjectForFile(packageFiles[0])
            if (packageProject === undefined) throw new Error(`No TypeScript project contains ${sourceRoot}`)
            const packageChecker = packageProject.checker
            const candidates = []
            const localResolveSymbol = (symbol) => {
              if (symbol !== undefined && (symbol.flags & SymbolFlags.Alias) !== 0) {
                return packageChecker.getAliasedSymbol(symbol)
              }
              return symbol
            }
            const localFunctionSymbol = (node) => {
              if (isFunctionDeclaration(node) && node.name !== undefined) {
                return packageChecker.getSymbolAtLocation(node.name)
              }
              if (isVariableDeclaration(node.parent) && isIdentifier(node.parent.name)) {
                return packageChecker.getSymbolAtLocation(node.parent.name)
              }
              if (isMethodDeclaration(node) && isIdentifier(node.name)) {
                return packageChecker.getSymbolAtLocation(node.name)
              }
              return undefined
            }
            const localTag = (type, location) => {
              if (type === undefined) return undefined
              const tag = packageChecker.getPropertyOfType(type, "_tag")
              if (tag === undefined || (tag.flags & SymbolFlags.Optional) !== 0) return undefined
              if (
                packageChecker.getPropertyOfType(type, "message") === undefined ||
                packageChecker.getPropertyOfType(type, "name") === undefined
              ) return undefined
              const tagType = packageChecker.getTypeOfSymbolAtLocation(tag, location)
              if ((tagType.flags & TypeFlags.StringLiteral) === 0) return undefined
              return tagType.value
            }
            const localFunctionTag = (node) => {
              const type = packageChecker.getTypeAtLocation(node)
              const signatures = packageChecker.getSignaturesOfType(type, SignatureKind.Call)
              if (signatures.length > 0) {
                const result = packageChecker.getReturnTypeOfSignature(signatures[0])
                const resultName = packageChecker.typeToString(result)
                if (
                  packageChecker.getPropertyOfType(result, effectMarker) === undefined &&
                  !resultName.includes("Effect<")
                ) {
                  const direct = localTag(result, node)
                  if (direct !== undefined) return direct
                }
              }
              let constructed
              const expression = returnedExpression(node)
              if (expression === undefined) return undefined
              const inspectConstruction = (child) => {
                if (constructed !== undefined) return
                if (isNewExpression(child)) {
                  constructed = localTag(packageChecker.getTypeAtLocation(child), child)
                  return
                }
                if (
                  child !== expression &&
                  (isArrowFunction(child) || isFunctionDeclaration(child) || isFunctionExpression(child) ||
                    isMethodDeclaration(child))
                ) return
                child.forEachChild(inspectConstruction)
              }
              inspectConstruction(expression)
              return constructed
            }
            for (const packageFile of packageFiles) {
              const packageSourceFile = packageProject.program.getSourceFile(packageFile)
              if (packageSourceFile === undefined) continue
              const normalizedPackageFile = resolve(packageFile).split(sep).join("/")
              const inspectPackage = (node) => {
                const functionLike = isArrowFunction(node) || isFunctionDeclaration(node) ||
                  isFunctionExpression(node) || isMethodDeclaration(node)
                if (
                  functionLike && normalizedPackageFile.includes("/src/internal/") && moduleScope(node)
                ) {
                  const candidateTag = localFunctionTag(node)
                  const candidateSymbol = localResolveSymbol(localFunctionSymbol(node))
                  if (candidateTag !== undefined && candidateSymbol !== undefined) {
                    candidates.push({
                      file: normalizedPackageFile,
                      key: `${normalizedPackageFile}:${node.getStart(packageSourceFile)}:${node.getEnd()}`,
                      symbol: candidateSymbol,
                      tag: candidateTag
                    })
                  }
                }
                node.forEachChild(inspectPackage)
              }
              inspectPackage(packageSourceFile)
            }
            const decisions = new Map()
            const candidatesByTag = new Map()
            for (const candidate of candidates) {
              const group = candidatesByTag.get(candidate.tag) ?? []
              group.push(candidate)
              candidatesByTag.set(candidate.tag, group)
              candidate.consumers = new Set()
            }
            for (const consumerFile of packageFiles) {
              const normalizedConsumer = resolve(consumerFile).split(sep).join("/")
              const consumerSource = packageProject.program.getSourceFile(consumerFile)
              if (consumerSource === undefined) continue
              const directCandidates = new Map()
              const namespaceCandidates = new Map()
              const collectImports = (node) => {
                if (isImportDeclaration(node) && isStringLiteral(node.moduleSpecifier)) {
                  let target = resolve(dirname(normalizedConsumer), node.moduleSpecifier.text)
                  if (target.endsWith(".js")) target = `${target.slice(0, -3)}.ts`
                  const importedCandidates = candidates.filter((candidate) => candidate.file === target)
                  const bindings = node.importClause?.namedBindings
                  if (isNamespaceImport(bindings)) {
                    const symbol = packageChecker.getSymbolAtLocation(bindings.name)
                    if (symbol !== undefined && importedCandidates.length > 0) {
                      namespaceCandidates.set(symbol.id, importedCandidates)
                    }
                  } else if (isNamedImports(bindings)) {
                    for (const specifier of bindings.elements) {
                      const imported = specifier.propertyName?.text ?? specifier.name.text
                      const candidate = importedCandidates.find((entry) => entry.symbol.name === imported)
                      const symbol = packageChecker.getSymbolAtLocation(specifier.name)
                      if (candidate !== undefined && symbol !== undefined) directCandidates.set(symbol.id, candidate)
                    }
                  }
                }
                node.forEachChild(collectImports)
              }
              collectImports(consumerSource)
              const inspectReferences = (node) => {
                if (isIdentifier(node)) {
                  const symbol = packageChecker.getSymbolAtLocation(node)
                  let candidate
                  if (symbol !== undefined) candidate = directCandidates.get(symbol.id)
                  if (candidate !== undefined && normalizedConsumer !== candidate.file) {
                    candidate.consumers.add(normalizedConsumer)
                  }
                } else if (isPropertyAccessExpression(node) || isElementAccessExpression(node)) {
                  const name = nativeStaticName(node)
                  const object = nativeMemberObject(node)
                  if (name !== undefined && object !== undefined) {
                    const symbol = packageChecker.getSymbolAtLocation(unwrapNative(object))
                    let candidate
                    if (symbol !== undefined) {
                      candidate = namespaceCandidates.get(symbol.id)?.find((entry) => entry.symbol.name === name)
                    }
                    if (candidate !== undefined && normalizedConsumer !== candidate.file) {
                      candidate.consumers.add(normalizedConsumer)
                    }
                  }
                }
                node.forEachChild(inspectReferences)
              }
              inspectReferences(consumerSource)
            }
            for (const candidate of candidates) {
              decisions.set(
                candidate.key,
                candidatesByTag.get(candidate.tag)?.length === 1 && candidate.consumers.size >= 2
              )
            }
            sharedConstructorDecisions.set(sourceRoot, decisions)
          } finally {
            packageSnapshot.dispose()
            packageApi.close()
          }
        }
        const key = `${normalizedFilename}:${functionNode.getStart(sourceFile)}:${functionNode.getEnd()}`
        return sharedConstructorDecisions.get(sourceRoot).get(key) === true
      }
      for (const functionNode of functionLikes) {
        if (!packageSource || !moduleScope(functionNode)) continue
        const tagged = functionTaggedError(functionNode)
        if (tagged === undefined) continue
        if (sharedInternalConstructor(functionNode, tagged)) continue
        moduleErrorHelpers.push({ start: functionNode.getStart(sourceFile), end: functionNode.getEnd() })
      }

      const result = {
        bareSqlRowTypes,
        classInstanceFactories,
        durationInputViolations,
        effectSyncReturnsEffect,
        effectValuedPlainTests,
        layerNameViolations,
        manualRuntimeBoundaries,
        moduleErrorHelpers,
        schemaTaggedErrorViolations,
        serviceTagMaps,
        taggedEffectErrors,
        taggedErrorInstanceof,
        unknownEffectChannels
      }
      analysisCache.set(filename, { sourceText, result })
      return result
    } finally {
      if (snapshot !== undefined) snapshot.dispose()
      sourceOverrides.delete(absoluteFilename)
    }
  }

  return {
    analyze,
    find: (input) => analyze(input).taggedEffectErrors,
    close: () => {
      api.close()
      openFilename = undefined
      sourceOverrides.clear()
    }
  }
}
