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

function parsePushPayload(payload) {
  const notice = payload?.notification || payload?.data || {};
  const title = String(notice?.title || "FoCo After Dark").trim();
  const body = String(notice?.body || "New update from a venue.").trim();
  const link = payload?.fcmOptions?.link || payload?.data?.link || notice?.link || "/";
  const sentAt = String(payload?.data?.sentAt || Date.now());
  const dedupeKey = `${sentAt}|${title}|${body}`;
  const hasNotificationPayload = !!payload?.notification;
  return { title, body, link, sentAt, dedupeKey, hasNotificationPayload };
}

function isDuplicatePush(dedupeKey) {
  const now = Date.now();
  if (dedupeKey === lastPushKey && (now - lastPushAt) < 12000) return true;
  lastPushKey = dedupeKey;
  lastPushAt = now;
  return false;
}

function showPushNotification({ title, body, link, sentAt }) {
  return self.registration.showNotification(title, {
    body,
    icon: "/foco-logo.png",
    badge: "/foco-logo.png",
    tag: `foco-${sentAt}`,
    data: { link, sentAt }
  });
}

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
  const parsed = parsePushPayload(payload || {});
  if (isDuplicatePush(parsed.dedupeKey)) return;
  // If notification payload exists, browser handles display.
  if (parsed.hasNotificationPayload) return;
  showPushNotification(parsed).catch(() => {});
});

self.addEventListener("push", (event) => {
  setAppBadgeValue(1).catch(() => {});
  if (!event?.data) return;
  let payload = {};
  try {
    payload = event.data.json() || {};
  } catch (_) {
    return;
  }
  const parsed = parsePushPayload(payload);
  if (!parsed.title && !parsed.body) return;
  if (isDuplicatePush(parsed.dedupeKey)) return;
  // Browser auto-displays notification payloads; only render data-only here.
  if (parsed.hasNotificationPayload) return;
  event.waitUntil(showPushNotification(parsed));
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
  } else if (type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

const CACHE_VERSION = "foco-cache-v15";
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
  // Never cache Firebase internals or dynamic API-ish paths.
  if (url.pathname.startsWith("/__/")) return;
  if (url.searchParams.has("v") || url.searchParams.has("ts") || url.searchParams.has("_")) return;

  // Always read deploy version fresh so forced-update logic can trigger reliably.
  if (url.pathname === "/version.json") {
    event.respondWith(
      fetch(event.request, { cache: "no-store" }).catch(() => caches.match("/version.json"))
    );
    return;
  }

  // Network-first for HTML to pick up fresh deploys
  const accept = event.request.headers.get("accept") || "";
  const isHtml = accept.includes("text/html");
  const isJson = accept.includes("application/json") || url.pathname.endsWith(".json");

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

  if (isJson) {
    event.respondWith(
      fetch(event.request, { cache: "no-store" }).catch(() => caches.match(event.request))
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
