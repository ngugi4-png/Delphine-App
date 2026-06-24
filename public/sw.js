// Delphine PWA Service Worker v1.0
const CACHE_NAME = 'delphine-v3';
const OFFLINE_URLS = [
  '/',
  '/index.html'
];

// Install: cache the shell
self.addEventListener('install', function(event) {
  event.waitUntil(
    caches.open(CACHE_NAME).then(function(cache) {
      return cache.addAll(OFFLINE_URLS);
    })
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', function(event) {
  event.waitUntil(
    caches.keys().then(function(keys) {
      return Promise.all(
        keys.filter(function(k) { return k !== CACHE_NAME; })
            .map(function(k) { return caches.delete(k); })
      );
    })
  );
  self.clients.claim();
});

// Fetch: network-first for API/Firebase, cache-first for app shell
self.addEventListener('fetch', function(event) {
  var url = event.request.url;

  // Always go network for Firebase, Google APIs, CDN resources
  if (url.includes('firebase') || url.includes('googleapis') ||
      url.includes('fontshare') || url.includes('jsdelivr') ||
      url.includes('cdnjs') || url.includes('gstatic')) {
    event.respondWith(fetch(event.request).catch(function() {
      return new Response('', { status: 503 });
    }));
    return;
  }

  // Cache-first for the app shell (HTML, CSS, JS)
  event.respondWith(
    caches.match(event.request).then(function(cached) {
      if (cached) return cached;
      return fetch(event.request).then(function(response) {
        // Cache successful GET responses for app shell files
        if (event.request.method === 'GET' && response.status === 200) {
          var clone = response.clone();
          caches.open(CACHE_NAME).then(function(cache) {
            cache.put(event.request, clone);
          });
        }
        return response;
      }).catch(function() {
        // Offline fallback — serve cached index.html for navigation requests
        if (event.request.mode === 'navigate') {
          return caches.match('/index.html');
        }
        return new Response('Offline', { status: 503 });
      });
    })
  );
});
