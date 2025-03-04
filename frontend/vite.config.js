import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  server: {
    proxy: {
      "/api": {
        target: "http://highres.team:8000",
        changeOrigin: true,
        secure: false,
        ws: false,
        rewrite: (path) => path,
      },
    },
  },
  plugins: [react()],
})
