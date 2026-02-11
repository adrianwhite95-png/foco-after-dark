importScripts("https://www.gstatic.com/firebasejs/12.6.0/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/12.6.0/firebase-messaging-compat.js");

firebase.initializeApp({
  apiKey: "AIzaSyBSkRskzE8Kj0CR-ckU3Kas1ZjFpNBZCss",
  authDomain: "foco-after-dark.firebaseapp.com",
  projectId: "foco-after-dark",
  storageBucket: "foco-after-dark.firebasestorage.app",
  messagingSenderId: "962947086594",
  appId: "1:962947086594:web:9c931c93a90992f4826a1b",
  measurementId: "G-FCLG1RNRYZ"
});

const messaging = firebase.messaging();
messaging.onBackgroundMessage((payload) => {
  const notice = payload?.notification || {};
  const title = notice.title || "FoCo After Dark";
  const body = notice.body || "New update from a venue.";
  const link = payload?.fcmOptions?.link || payload?.data?.link || "/";
  self.registration.showNotification(title, {
    body,
    icon: "/foco-logo.png",
    badge: "/foco-logo.png",
    data: { link }
  });
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const target = event.notification?.data?.link || "/";
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if ("focus" in client) {
          client.navigate(target);
          return client.focus();
        }
      }
      if (clients.openWindow) {
        return clients.openWindow(target);
      }
    })
  );
});

const CACHE_VERSION = "foco-cache-v11";
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
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) {
    // Don't intercept cross-origin requests (prevents opaque SW responses)
    return;
  }

  // Network-first for HTML to pick up fresh deploys
  const accept = event.request.headers.get("accept") || "";
  const isHtml = accept.includes("text/html");

  if (isHtml) {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          if (response && response.ok && response.type === "basic") {
            const clone = response.clone();
            caches.open(CACHE_VERSION).then(cache => cache.put(event.request, clone));
          }
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
        if (resp && resp.ok && resp.type === "basic") {
          const clone = resp.clone();
          caches.open(CACHE_VERSION).then(cache => cache.put(event.request, clone));
        }
        return resp;
      });
    })
  );
});
