const CACHE_VERSION = "foco-cache-v8";
const CORE_ASSETS = ["/", "/index.html", "/manifest.json", "/foco-logo.png"];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION).then((cache) => cache.addAll(CORE_ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;

  // Network-first for HTML to pick up fresh deploys
  const accept = event.request.headers.get("accept") || "";
  const isHtml = accept.includes("text/html");

  if (isHtml) {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const clone = response.clone();
          caches.open(CACHE_VERSION).then(cache => cache.put(event.request, clone));
          return response;
        })
        .catch(() => caches.match(event.request))
    );
    return;
  }

  // Cache-first for other GETs
  event.respondWith(
    caches.match(event.request).then((cached) => {
      if (cached) return cached;
      return fetch(event.request).then((resp) => {
        const clone = resp.clone();
        caches.open(CACHE_VERSION).then(cache => cache.put(event.request, clone));
        return resp;
      });
    })
  );
});
