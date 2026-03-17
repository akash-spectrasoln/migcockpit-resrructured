/** @type {import("eslint").Linter.Config} */
module.exports = {
  root: true,
  env: {
    browser: true,
    es2020: true,
    node: true,
  },
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 'latest',
    sourceType: 'module',
    ecmaFeatures: { jsx: true },
  },
  plugins: [
    '@typescript-eslint',
    'react-hooks',
    'react-refresh',
  ],
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
    'plugin:react-hooks/recommended',
  ],
  rules: {
    // ── React ────────────────────────────────────────────────────────────
    'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],
    'react-hooks/rules-of-hooks': 'error',       // hooks must be called correctly
    'react-hooks/exhaustive-deps': 'warn',        // missing effect dependencies

    // ── TypeScript ────────────────────────────────────────────────────────
    '@typescript-eslint/no-explicit-any': 'warn',             // flag any usage
    '@typescript-eslint/no-unused-vars': ['warn', {
      argsIgnorePattern: '^_',                   // allow _unused params
      varsIgnorePattern: '^_',
    }],
    '@typescript-eslint/no-non-null-assertion': 'warn',       // flag ! assertions
    '@typescript-eslint/consistent-type-imports': ['warn', {  // use type imports
      prefer: 'type-imports',
    }],

    // ── Code quality ──────────────────────────────────────────────────────
    'no-console': ['warn', { allow: ['warn', 'error'] }],     // no stray console.log
    'no-debugger': 'error',
    'no-duplicate-imports': 'error',
    'no-var': 'error',                            // always let/const
    'prefer-const': 'warn',
    'eqeqeq': ['error', 'always', { null: 'ignore' }],       // always ===

    // ── Import hygiene ─────────────────────────────────────────────────────
    'no-restricted-imports': ['error', {
      patterns: [
        {
          group: ['../**/components/Canvas/*'],   // force lowercase canvas/
          message: "Import from 'components/canvas/' (lowercase) instead.",
        },
      ],
    }],
  },
  ignorePatterns: [
    'dist/',
    'build/',
    'node_modules/',
    '*.config.js',
    '*.config.cjs',
    '*.config.ts',
    'vite.config.ts',
  ],
}
