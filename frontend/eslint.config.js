import js from '@eslint/js'
import globals from 'globals'
import react from 'eslint-plugin-react'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'

export default [
  { ignores: ['dist'] },
  {
    // TypeScript sources are now linted too (they were previously skipped,
    // which let undeclared-variable bugs like the AgentsPage sniffer state
    // slip through). The typescript-eslint parser is required to parse
    // .ts/.tsx at all.
    files: ['**/*.{js,jsx,ts,tsx}'],
    languageOptions: {
      parser: tseslint.parser,
      ecmaVersion: 2020,
      globals: globals.browser,
      parserOptions: {
        ecmaVersion: 'latest',
        ecmaFeatures: { jsx: true },
        sourceType: 'module',
      },
    },
    settings: { react: { version: '18.3' } },
    plugins: {
      react,
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
    },
    rules: {
      ...js.configs.recommended.rules,
      ...react.configs.recommended.rules,
      ...react.configs['jsx-runtime'].rules,
      ...reactHooks.configs.recommended.rules,
      // TypeScript catches undefined identifiers at compile time (tsc runs in
      // CI); no-undef misfires on TS type-space declarations.
      'no-undef': 'off',
      // Prop types are expressed (progressively) via TypeScript interfaces.
      'react/prop-types': 'off',
      // Leading-underscore params are intentionally unused (common in
      // callback/interface signatures).
      'no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      'react/jsx-no-target-blank': 'off',
      'react-refresh/only-export-components': [
        'warn',
        { allowConstantExport: true },
      ],
    },
  },
  {
    // TS-aware unused-vars: the base rule false-positives on TS syntax such
    // as constructor parameter properties.
    files: ['**/*.{ts,tsx}'],
    plugins: {
      '@typescript-eslint': tseslint.plugin,
    },
    rules: {
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
    },
  },
]
