/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      // Slate-tinted gray palette — replaces the default near-black grays
      // with slightly blue/slate tones for a less oppressive dark UI.
      // Every existing `gray-*` class in the codebase auto-updates.
      colors: {
        gray: {
          950: '#0f1117',  // page bg        (was #030712)
          900: '#1a1f2e',  // surface/panel  (was #111827)
          800: '#2d3a50',  // border/grid    (was #1f2937)
          700: '#374357',  // inactive bdr   (was #374151)
          600: '#536070',  // faint text     (was #4b5563)
          500: '#637585',  // muted text     (was #6b7280)
          400: '#9fb3cc',  // label text     (was #9ca3af)
          300: '#c5d5e8',  // secondary      (was #d1d5db)
          200: '#dde9f4',
          100: '#f0f6fc',  // primary text   (was #f3f4f6)
          50:  '#f8fafc',
        },
      },
    },
  },
  plugins: [],
}
