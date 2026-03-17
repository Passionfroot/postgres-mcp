import { describe, expect, it } from "vitest";

import { mergeSessionVars } from "../src/server.js";

describe("mergeSessionVars", () => {
  it("returns undefined when neither config nor request have vars", () => {
    expect(mergeSessionVars(undefined, undefined)).toBeUndefined();
  });

  it("returns config defaults when no request vars provided", () => {
    const config = { "app.tenant_id": "default_tenant", "app.env": "production" };
    expect(mergeSessionVars(config, undefined)).toEqual({
      "app.tenant_id": "default_tenant",
      "app.env": "production",
    });
  });

  it("filters out empty-string config defaults when no request vars provided", () => {
    const config = { "app.tenant_id": "", "app.env": "production" };
    expect(mergeSessionVars(config, undefined)).toEqual({
      "app.env": "production",
    });
  });

  it("returns undefined when all config defaults are empty and no request vars", () => {
    const config = { "app.tenant_id": "" };
    expect(mergeSessionVars(config, undefined)).toBeUndefined();
  });

  it("request vars override config defaults", () => {
    const config = { "app.tenant_id": "default", "app.env": "production" };
    const request = { "app.tenant_id": "tenant_abc" };
    expect(mergeSessionVars(config, request)).toEqual({
      "app.tenant_id": "tenant_abc",
      "app.env": "production",
    });
  });

  it("request vars fill in empty config placeholders", () => {
    const config = { "app.tenant_id": "" };
    const request = { "app.tenant_id": "tenant_abc" };
    expect(mergeSessionVars(config, request)).toEqual({
      "app.tenant_id": "tenant_abc",
    });
  });

  it("throws on request vars when config has no session_vars", () => {
    expect(() =>
      mergeSessionVars(undefined, { "app.tenant_id": "t_1" })
    ).toThrow("does not accept session_vars");
  });

  it("throws on unknown request var keys", () => {
    const config = { "app.tenant_id": "" };
    expect(() =>
      mergeSessionVars(config, { "app.unknown_key": "value" })
    ).toThrow("Unknown session_vars keys: app.unknown_key");
  });

  it("throws listing all unknown keys", () => {
    const config = { "app.tenant_id": "" };
    expect(() =>
      mergeSessionVars(config, { "app.foo": "1", "app.bar": "2" })
    ).toThrow("app.foo, app.bar");
  });

  it("allows empty request vars object without error", () => {
    const config = { "app.tenant_id": "default" };
    expect(mergeSessionVars(config, {})).toEqual({
      "app.tenant_id": "default",
    });
  });

  it("allows empty request vars when config is undefined", () => {
    expect(mergeSessionVars(undefined, {})).toBeUndefined();
  });
});
