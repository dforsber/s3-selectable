module.exports = {
  env: {
    commonjs: true,
    node: true,
  },
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/eslint-recommended",
    "plugin:@typescript-eslint/recommended",
    "plugin:@typescript-eslint/recommended-requiring-type-checking",
    "plugin:jest/recommended",
    "plugin:prettier/recommended",
    "prettier/@typescript-eslint",
  ],
  globals: {
    Atomics: "readonly",
    SharedArrayBuffer: "readonly",
  },
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: 2019,
    project: ["./tsconfig.json"],
  },
  plugins: ["@typescript-eslint", "jest", "prettier"],
  rules: {
    "@typescript-eslint/adjacent-overload-signatures": "error",
    "@typescript-eslint/array-type": [
      "error",
      {
        default: "array-simple",
      },
    ],
    "@typescript-eslint/await-thenable": "error",
    "@typescript-eslint/camelcase": [
      "warn",
      {
        properties: "always",
      },
    ],
    "@typescript-eslint/class-name-casing": "error",
    "@typescript-eslint/consistent-type-assertions": [
      "error",
      {
        assertionStyle: "angle-bracket",
        objectLiteralTypeAssertions: "allow",
      },
    ],
    "@typescript-eslint/consistent-type-definitions": ["error", "interface"],
    "@typescript-eslint/explicit-function-return-type": [
      "error",
      {
        allowExpressions: true,
        allowTypedFunctionExpressions: true,
      },
    ],
    "@typescript-eslint/explicit-member-accessibility": [
      "error",
      {
        accessibility: "explicit",
        overrides: {
          constructors: "no-public",
        },
      },
    ],
    "@typescript-eslint/generic-type-naming": ["error", "^T[A-Z][a-zA-Z]+$"],
    "@typescript-eslint/interface-name-prefix": ["error", "always"],
    "@typescript-eslint/no-dupe-class-members": ["error"],
    "@typescript-eslint/no-empty-function": "error",
    "@typescript-eslint/no-explicit-any": [
      "warn",
      {
        fixToUnknown: false,
        ignoreRestArgs: true,
      },
    ],
    "@typescript-eslint/no-floating-promises": "error",
    "@typescript-eslint/no-inferrable-types": [
      "error",
      {
        ignoreProperties: true,
      },
    ],
    "@typescript-eslint/no-misused-new": "error",
    "@typescript-eslint/no-misused-promises": "error",
    "@typescript-eslint/no-namespace": "error",
    "@typescript-eslint/no-non-null-assertion": "error",
    "@typescript-eslint/no-require-imports": "error",
    "@typescript-eslint/no-this-alias": [
      "error",
      {
        allowDestructuring: true,
      },
    ],
    "@typescript-eslint/no-unnecessary-type-arguments": "error",
    "@typescript-eslint/no-unnecessary-type-assertion": "error",
    "@typescript-eslint/no-unused-expressions": ["error"],
    "@typescript-eslint/no-unused-vars": [
      "error",
      {
        argsIgnorePattern: "^_",
        varsIgnorePattern: "^_",
      },
    ],
    "@typescript-eslint/no-useless-constructor": "error",
    "@typescript-eslint/no-var-requires": "error",
    "@typescript-eslint/prefer-for-of": "error",
    "@typescript-eslint/prefer-function-type": "error",
    "@typescript-eslint/prefer-includes": "error",
    "@typescript-eslint/prefer-namespace-keyword": "error",
    "@typescript-eslint/prefer-nullish-coalescing": "error",
    "@typescript-eslint/prefer-optional-chain": "error",
    "@typescript-eslint/prefer-string-starts-ends-with": "error",
    "@typescript-eslint/require-await": "error",
    "@typescript-eslint/restrict-plus-operands": "error",
    "@typescript-eslint/return-await": "error",
    "@typescript-eslint/triple-slash-reference": "error",
    "@typescript-eslint/unbound-method": "warn",
    "@typescript-eslint/unified-signatures": "error",
    camelcase: "off",
    "linebreak-style": ["error", "unix"],
    "max-classes-per-file": ["error", 1],
    "max-len": [
      "error",
      {
        code: 120,
        ignoreComments: true,
        ignorePattern: "^import*",
        ignoreTemplateLiterals: true,
      },
    ],
    "no-dupe-class-members": "off",
    "no-empty-function": "off",
    "no-unused-expressions": "off",
    "no-useless-constructor": "off",
    "object-shorthand": "error",
    "require-await": "off",
    "no-return-await": "off",
  },
};
