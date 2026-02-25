import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.intg.test.ts"],
    testTimeout: 15_000,
  },
});
