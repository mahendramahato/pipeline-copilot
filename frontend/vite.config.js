import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// In development, Vite serves the UI on :5173 and forwards /api to FastAPI on :8000.
// In production, `npm run build` writes dist/, which FastAPI serves itself.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: { '/api': 'http://127.0.0.1:8000' },
  },
})
