import {
  isArrayLiteralExpression,
  isBindingElement,
  isCallExpression,
  isComputedPropertyName,
  isConstructorDeclaration,
  isElementAccessExpression,
  isExpression,
  isIdentifier,
  isNoSubstitutionTemplateLiteral,
  isObjectBindingPattern,
  isObjectLiteralExpression,
  isOuterExpression,
  isParameterDeclaration,
  isPrivateIdentifier,
  isPropertyAccessExpression,
  isPropertyAssignment,
  isPropertyDeclaration,
  isShorthandPropertyAssignment,
  isStringLiteral,
  isTypeNode,
  isVariableDeclaration
} from "@typescript/native-preview/unstable/ast/is"
import { API, ModifierFlags, SignatureKind, SymbolFlags, TypeFlags } from "@typescript/native-preview/unstable/sync"
import { realpathSync } from "node:fs"
import { resolve } from "node:path"

const effectMarker = "~effect/Effect"
const layerMarker = "~effect/Layer"
const serviceMarker = "~effect/Context/Service"
const managedRuntimeMarker = "~effect/ManagedRuntime"

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

  let cachedAnalysis
  const analyze = ({ filename, sourceText }) => {
    if (cachedAnalysis?.filename === filename && cachedAnalysis.sourceText === sourceText) {
      return cachedAnalysis.result
    }
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
        if (isObjectBindingPattern(node)) objectBindingPatterns.push(node)
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

      const effectErrorByType = new Map()
      const getEffectError = (type, expression) => {
        if (effectErrorByType.has(type.id)) return effectErrorByType.get(type.id)
        const markerSymbol = checker.getPropertyOfType(type, effectMarker)
        if (markerSymbol === undefined) {
          effectErrorByType.set(type.id, undefined)
          return undefined
        }
        const markerType = checker.getTypeOfSymbolAtLocation(markerSymbol, expression)
        const errorSymbol = checker.getPropertyOfType(markerType, "_E")
        if (errorSymbol === undefined) {
          throw new Error(`The Effect marker on ${absoluteFilename} has no _E channel`)
        }
        const errorAccessor = checker.getTypeOfSymbolAtLocation(errorSymbol, expression)
        const signatures = checker.getSignaturesOfType(errorAccessor, SignatureKind.Call)
        if (signatures.length !== 1) {
          throw new Error(`The Effect _E marker on ${absoluteFilename} is not a single callable signature`)
        }
        const errorType = checker.getReturnTypeOfSignature(signatures[0])
        if (errorType === undefined) {
          throw new Error(`TypeScript could not resolve an Effect error channel in ${absoluteFilename}`)
        }
        effectErrorByType.set(type.id, errorType)
        return errorType
      }

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
        if ((type.flags & TypeFlags.Never) !== 0 || isEffectUnhandled(type)) {
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

      const result = { layerNameViolations, manualRuntimeBoundaries, serviceTagMaps, taggedEffectErrors }
      cachedAnalysis = { filename, sourceText, result }
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
