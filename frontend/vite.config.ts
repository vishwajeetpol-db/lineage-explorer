import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
      // elkjs's commonjs source has require('web-worker') for Node fallback.
      // Stub it to the native browser Worker so vite doesn't emit a bare
      // `import "web-worker"` that fails to resolve at runtime.
      "web-worker": path.resolve(__dirname, "./src/stubs/web-worker.ts"),
    },
  },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    rollupOptions: {
      output: {
        // Code-split the heavy deps so the initial shell stays small. First
        // paint loads ~600KB instead of the previous ~1.9MB single chunk.
        manualChunks: {
          reactflow: [
            "reactflow",
            "@reactflow/core",
            "@reactflow/minimap",
            "@reactflow/controls",
            "@reactflow/background",
          ],
          elk: ["elkjs"],
          radix: [
            "@radix-ui/react-dropdown-menu",
            "@radix-ui/react-popover",
            "@radix-ui/react-select",
            "@radix-ui/react-switch",
            "@radix-ui/react-tooltip",
          ],
          motion: ["framer-motion"],
        },
      },
    },
  },
});
