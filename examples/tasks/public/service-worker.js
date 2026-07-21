const cacheName = "effect-local-tasks-v2"
const cachePrefix = "effect-local-tasks-"
const shellPaths = new Set(["/", "/manifest.webmanifest", "/task-icon.svg"])
const shellDestinations = new Set(["font", "image", "manifest", "script", "sharedworker", "style", "worker"])

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(cacheName).then((cache) => cache.addAll(["/", "/manifest.webmanifest", "/task-icon.svg"]))
  )
  void self.skipWaiting()
})

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((key) => key.startsWith(cachePrefix) && key !== cacheName).map((key) => caches.delete(key))
      )
    )
  )
  event.waitUntil(self.clients.claim())
})

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return
  const url = new URL(event.request.url)
  if (
    url.origin !== self.location.origin ||
    (!shellPaths.has(url.pathname) && !shellDestinations.has(event.request.destination) &&
      !url.pathname.endsWith(".wasm"))
  ) return
  event.respondWith(
    fetch(event.request).then(async (response) => {
      if (response.ok) {
        const cache = await caches.open(cacheName)
        await cache.put(event.request, response.clone())
      }
      return response
    }).catch(async () => {
      const cached = await caches.open(cacheName).then((cache) => cache.match(event.request))
      if (cached !== undefined) return cached
      if (event.request.mode === "navigate") {
        return caches.open(cacheName).then((cache) => cache.match("/"))
      }
      return Response.error()
    })
  )
})
