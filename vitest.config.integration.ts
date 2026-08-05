import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.intg.test.ts"],
    testTimeout: 15_000,
    // These tests share one Postgres database (introspecting the whole public schema, creating
    // and dropping tables). Running the files in parallel races on that shared state, so keep
    // them to one at a time.
    fileParallelism: false,
  },
});
