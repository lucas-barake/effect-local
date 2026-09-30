const { getDefaultConfig } = require("expo/metro-config")

const config = getDefaultConfig(__dirname)

const resolveTypeScriptSource = (context, moduleName, platform) => {
  const base = moduleName.slice(0, -3)
  try {
    return context.resolveRequest(context, `${base}.ts`, platform)
  } catch {
    return context.resolveRequest(context, `${base}.tsx`, platform)
  }
}

config.resolver.resolveRequest = (context, moduleName, platform) => {
  if (moduleName.startsWith(".") && moduleName.endsWith(".js")) {
    try {
      return resolveTypeScriptSource(context, moduleName, platform)
    } catch {
      return context.resolveRequest(context, moduleName, platform)
    }
  }
  return context.resolveRequest(context, moduleName, platform)
}

module.exports = config
