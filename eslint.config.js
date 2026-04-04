import tsPlugin from '@typescript-eslint/eslint-plugin'
import tsParser from '@typescript-eslint/parser'
import prettierConfig from 'eslint-config-prettier'

export default [
    {
        files: ['src/**/*.ts', 'tests/**/*.ts'],
        languageOptions: {
            parser: tsParser,
            parserOptions: {
                project: './tsconfig.json'
            }
        },
        plugins: {
            '@typescript-eslint': tsPlugin
        },
        rules: {
            ...tsPlugin.configs['recommended'].rules,
            '@typescript-eslint/no-explicit-any': 'warn',
            '@typescript-eslint/explicit-function-return-type': 'warn',
            '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }]
        }
    },
    prettierConfig,
    {
        ignores: ['bin/**', 'node_modules/**', '**/*.d.ts']
    }
]
