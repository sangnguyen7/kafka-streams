module.exports = {
  "env": {
    "es6": true,
    "node": true,
    "mocha": true
  },
  "parser": "@typescript-eslint/parser",
  "plugins": [
    "@typescript-eslint",
  ],
  "extends": [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended",
  ],
  "rules": {
    "linebreak-style": [
      "error",
      "unix"
    ],
    "semi": [
      "error",
      "always"
    ],
    "no-console": 0,
    "no-mixed-spaces-and-tabs": ["error", "smart-tabs"]
  }
};
