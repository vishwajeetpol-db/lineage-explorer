// Browser stub for the Node.js `web-worker` polyfill package.
// elkjs's commonjs source contains `require('web-worker')` as a server-side
// fallback. In the browser we have a native Worker, so we just re-export it.
// Aliased in vite.config.ts so vite does not emit a bare `import "web-worker"`
// that the browser cannot resolve.
const W = typeof globalThis !== "undefined" ? (globalThis as { Worker?: typeof Worker }).Worker : undefined;
export default W;
