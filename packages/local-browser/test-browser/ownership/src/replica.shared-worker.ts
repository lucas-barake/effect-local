import * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"

OwnershipCoordinator.runSharedWorker(() =>
  import("./replica.shared-worker-runtime.ts").then((module) => module.options)
)
