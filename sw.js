/**
 * sw.js — Service Worker — État des Eaux en Extérieur
 * =====================================================
 * Stratégie inspirée des patterns ODK (offline-first) et OpenAQ
 * (stale-while-revalidate pour données vivantes).
 *
 * Architecture :
 *   Cache SHELL    → cache-first          (HTML, CSS, JS, icons)
 *   Cache TILES    → stale-while-revalidate (tuiles OSM carte)
 *   Cache API      → network-first + fallback (données Hub'Eau)
 *   SYNC QUEUE     → Background Sync API  (remontées terrain futures)
 *
 * Version : 1.0.0-pwa
 */

const VERSION      = 'v1.0.0';
const CACHE_SHELL  = `eaux-shell-${VERSION}`;
const CACHE_TILES  = `eaux-tiles-${VERSION}`;
const CACHE_API    = `eaux-api-${VERSION}`;
const SYNC_QUEUE   = 'eaux-sync-queue';

// ── Ressources à précacher au moment de l'installation ──────────────────────
const SHELL_ASSETS = [
  './surveillance_eaux_v4.html',
  './manifest.json',
  './icons/icon-192.png',
  './icons/icon-512.png',
  // CDN — cachés dynamiquement à la première visite (voir fetch handler)
];

// ── URLs CDN à mettre en cache dès la première utilisation ──────────────────
const CDN_ORIGINS = [
  'https://cdnjs.cloudflare.com',
  'https://unpkg.com',
];

// ── Tuiles cartographiques (OpenStreetMap) ───────────────────────────────────
const TILE_ORIGINS = [
  'https://a.tile.openstreetmap.org',
  'https://b.tile.openstreetmap.org',
  'https://c.tile.openstreetmap.org',
];

// ── Endpoints Hub'Eau ────────────────────────────────────────────────────────
const API_ORIGINS = [
  'https://hubeau.eaufrance.fr',
  'https://api.sandre.eaufrance.fr',
];

// ────────────────────────────────────────────────────────────────────────────
// INSTALL — précache le shell
// ────────────────────────────────────────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_SHELL)
      .then(cache => cache.addAll(SHELL_ASSETS))
      .then(() => self.skipWaiting())
  );
});

// ────────────────────────────────────────────────────────────────────────────
// ACTIVATE — nettoie les anciens caches
// ────────────────────────────────────────────────────────────────────────────
self.addEventListener('activate', event => {
  const CURRENT = [CACHE_SHELL, CACHE_TILES, CACHE_API];
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => !CURRENT.includes(k)).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

// ────────────────────────────────────────────────────────────────────────────
// FETCH — routing par type de ressource
// ────────────────────────────────────────────────────────────────────────────
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // Ignorer les requêtes non-GET et les devtools
  if (request.method !== 'GET') return;
  if (url.protocol === 'chrome-extension:') return;

  // ── Tuiles OSM → stale-while-revalidate ──────────────────────────────────
  if (TILE_ORIGINS.some(o => request.url.startsWith(o))) {
    event.respondWith(staleWhileRevalidate(request, CACHE_TILES));
    return;
  }

  // ── API Hub'Eau / SANDRE → network-first (5s timeout) + fallback cache ──
  if (API_ORIGINS.some(o => request.url.startsWith(o))) {
    event.respondWith(networkFirstWithFallback(request, CACHE_API, 5000));
    return;
  }

  // ── CDN (Chart.js, Leaflet…) → stale-while-revalidate ───────────────────
  if (CDN_ORIGINS.some(o => request.url.startsWith(o))) {
    event.respondWith(staleWhileRevalidate(request, CACHE_SHELL));
    return;
  }

  // ── Shell local → cache-first ────────────────────────────────────────────
  event.respondWith(cacheFirst(request, CACHE_SHELL));
});

// ────────────────────────────────────────────────────────────────────────────
// BACKGROUND SYNC — remontées terrain (futur)
// Déclenché automatiquement quand la connexion revient
// ────────────────────────────────────────────────────────────────────────────
self.addEventListener('sync', event => {
  if (event.tag === SYNC_QUEUE) {
    event.waitUntil(flushSyncQueue());
  }
});

async function flushSyncQueue() {
  // Ouvre la queue IndexedDB et rejoue les requêtes en attente
  // Implémentation complète dans data-sync.js (v1.1)
  const clients = await self.clients.matchAll();
  clients.forEach(c => c.postMessage({ type: 'SYNC_COMPLETE' }));
}

// ────────────────────────────────────────────────────────────────────────────
// STRATÉGIES DE CACHE
// ────────────────────────────────────────────────────────────────────────────

/** Cache-first : sert depuis le cache, récupère depuis le réseau sinon */
async function cacheFirst(request, cacheName) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('Ressource non disponible hors ligne.', {
      status: 503,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    });
  }
}

/** Stale-while-revalidate : sert le cache immédiatement, met à jour en arrière-plan */
async function staleWhileRevalidate(request, cacheName) {
  const cache    = await caches.open(cacheName);
  const cached   = await cache.match(request);
  const fetchPromise = fetch(request)
    .then(response => {
      if (response.ok) cache.put(request, response.clone());
      return response;
    })
    .catch(() => null);
  return cached || await fetchPromise || new Response('', { status: 503 });
}

/** Network-first avec timeout : essaie le réseau, bascule sur le cache */
async function networkFirstWithFallback(request, cacheName, timeoutMs) {
  const cache = await caches.open(cacheName);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(request, { signal: controller.signal });
    clearTimeout(timeout);
    if (response.ok) cache.put(request, response.clone());
    return response;
  } catch {
    clearTimeout(timeout);
    const cached = await cache.match(request);
    if (cached) return cached;
    return new Response(JSON.stringify({ error: 'offline', cached: false }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

// ────────────────────────────────────────────────────────────────────────────
// MESSAGE — communication avec la page principale
// ────────────────────────────────────────────────────────────────────────────
self.addEventListener('message', event => {
  if (event.data?.type === 'SKIP_WAITING') self.skipWaiting();
  if (event.data?.type === 'CACHE_STATUS') {
    caches.keys().then(keys => {
      event.source.postMessage({ type: 'CACHE_STATUS_REPLY', caches: keys });
    });
  }
});
