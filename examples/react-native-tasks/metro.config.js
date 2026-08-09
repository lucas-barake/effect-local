const { getDefaultConfig } = require("expo/metro-config")

/** @type {import('expo/metro-config').MetroConfig} */
const config = getDefaultConfig(__dirname)

// The workspace packages ship TypeScript sources whose relative imports keep their
// emitted ".js" specifiers. Vite/esbuild maps those back to ".ts" automatically; Metro
// does not, so rewrite them here. Published builds resolve to dist/*.js and never hit
// this hook.
const defaultResolveRequest = config.resolver.resolveRequest
config.resolver.resolveRequest = (context, moduleName, platform) => {
  if (moduleName.startsWith(".") && moduleName.endsWith(".js")) {
    try {
      return context.resolveRequest(context, `${moduleName.slice(0, -3)}.ts`, platform)
    } catch {
      // fall through to the default resolution
    }
  }
  if (typeof defaultResolveRequest === "function") {
    return defaultResolveRequest(context, moduleName, platform)
  }
  return context.resolveRequest(context, moduleName, platform)
}

// effect's Migrator.fromFileSystem contains a computed dynamic import (a Node-only
// loader local-sql never calls). Metro rejects non-static dynamic imports at bundle
// time; "throwAtRuntime" defers the error to a code path that never executes.
config.transformer.dynamicDepsInPackages = "throwAtRuntime"

module.exports = config
