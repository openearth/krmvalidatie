import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import vuetify, { transformAssetUrls } from 'vite-plugin-vuetify'

export default defineConfig({
  plugins: [
    vue({ 
      template: { transformAssetUrls }
    }),
    vuetify({
      autoImport: true,
    }),
  ],
  define: { 
    'process.env': {},
    global: 'globalThis',
  },
  resolve: {
    extensions: [
      '.js',
      '.json',
      '.jsx',
      '.mjs',
      '.ts',
      '.tsx',
      '.vue',
    ],
  },
  build: {
    target: 'es2020',
    rollupOptions: {
      external: [],
      output: {
        manualChunks: undefined,
      }
    }
  },
  optimizeDeps: {
    include: ['vuetify'],
    exclude: []
  }
})