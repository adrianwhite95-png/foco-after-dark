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
let lastPushKey = "";
let lastPushAt = 0;

async function setAppBadgeValue(value = 1) {
  try {
    if (self.registration && typeof self.registration.setAppBadge === "function") {
      await self.registration.setAppBadge(value);
      return;
    }
  } catch (_) {}
  try {
    if (self.navigator && typeof self.navigator.setAppBadge === "function") {
      await self.navigator.setAppBadge(value);
    }
  } catch (_) {}
}

async function clearAppBadgeValue() {
  try {
    if (self.registration && typeof self.registration.clearAppBadge === "function") {
      await self.registration.clearAppBadge();
      return;
    }
  } catch (_) {}
  try {
    if (self.navigator && typeof self.navigator.clearAppBadge === "function") {
      await self.navigator.clearAppBadge();
    }
  } catch (_) {}
}

messaging.onBackgroundMessage((payload) => {
  setAppBadgeValue(1).catch(() => {});
  // If FCM already provides a notification payload, the browser can auto-display it.
  // Avoid showing a second copy manually.
  if (payload?.notification) return;
  const notice = payload?.notification || payload?.data || {};
  const title = notice.title || "FoCo After Dark";
  const body = notice.body || "New update from a venue.";
  const link = payload?.fcmOptions?.link || payload?.data?.link || notice.link || "/";
  const sentAt = payload?.data?.sentAt || String(Date.now());
  const dedupeKey = `${sentAt}|${title}|${body}`;
  const now = Date.now();
  if (dedupeKey === lastPushKey && (now - lastPushAt) < 12000) return;
  lastPushKey = dedupeKey;
  lastPushAt = now;
  self.registration.showNotification(title, {
    body,
    icon: "/foco-logo.png",
    badge: "/foco-logo.png",
    tag: `foco-${sentAt}`,
    data: { link, sentAt }
  });
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  clearAppBadgeValue().catch(() => {});
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

self.addEventListener("message", (event) => {
  const type = event?.data?.type;
  if (type === "CLEAR_APP_BADGE") {
    clearAppBadgeValue().catch(() => {});
  } else if (type === "SET_APP_BADGE") {
    const count = Number(event?.data?.count || 1);
    setAppBadgeValue(Math.max(1, count)).catch(() => {});
  }
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
