import test from "node:test";
import assert from "node:assert/strict";
import { gunzipSync } from "node:zlib";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import plugin, { defaults } from "../index.mjs";
import { retryFetch } from "../src/retry.mjs";

const completed = () => new Response(JSON.stringify({
  id: "synthetic-response", status: "completed",
  output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "OK" }] }],
}), { headers: { "content-type": "application/json" } });

test("public entry enables AgentRouter and default phase settings", async () => {
  assert.equal(plugin.id, "agentrouter-retry");
  assert.equal(defaults.phaseTimeouts.firstOutputMs, 120_000);
  const server = await plugin.server({}, {});
  assert.equal(server.auth.provider, "agentrouter-retry");
  const config = {};
  await server.config(config);
  assert.ok(config.provider[plugin.id].models["gpt-6.1-sol"]);
});

test("public entry compresses large Responses bodies and sends small bodies unchanged", async () => {
  const temp = await mkdtemp(join(tmpdir(), "magpie-agentrouter-release-"));
  const originalFetch = globalThis.fetch;
  const sent = [];
  try {
    globalThis.fetch = async (url, init) => {
      sent.push({ url: String(url), headers: new Headers(init.headers), body: init.body });
      return completed();
    };
    const server = await plugin.server({}, {
      providersFile: join(temp, "providers.json"), nativeAlignment: false,
    });
    const loader = await server.auth.loader(async () => ({ type: "api", key: "synthetic-test-key" }));
    for (const size of [100_000, 32]) {
      const body = { model: "gpt-6.1-sol", input: "synthetic-input-".repeat(size), stream: false };
      const response = await loader.fetch("https://agentrouter.org/v1/responses", {
        method: "POST", headers: { "content-type": "application/json", "content-length": "1" },
        body: JSON.stringify(body),
      });
      assert.equal(response.status, 200);
      assert.match(await response.text(), /OK/);
      const captured = sent.at(-1);
      assert.equal(captured.headers.has("content-length"), false);
      if (size > 32) {
        assert.equal(captured.headers.get("content-encoding"), "gzip");
        const decoded = gunzipSync(captured.body).toString();
        assert.deepEqual(JSON.parse(decoded).input, body.input);
        assert.ok(captured.body.byteLength < Buffer.byteLength(decoded));
        console.log(JSON.stringify({ plainBytes: Buffer.byteLength(decoded), gzipBytes: captured.body.byteLength }));
      } else {
        assert.equal(captured.headers.has("content-encoding"), false);
        assert.equal(typeof captured.body, "string");
        assert.deepEqual(JSON.parse(captured.body).input, body.input);
      }
    }
    assert.equal(sent.length, 2);
  } finally {
    globalThis.fetch = originalFetch;
    await rm(temp, { recursive: true, force: true });
  }
});

test("retry engine retries a transient 503 and recovers", async () => {
  let attempts = 0;
  const response = await retryFetch("https://example.invalid/v1/responses", { method: "POST", body: "{}" }, {
    maxAttempts: 3, budgetMs: 2_000, baseDelayMs: 0, maxDelayMs: 0,
    attempt: () => ({ account: "synthetic-account" }),
    fetchImpl: async () => ++attempts === 1
      ? new Response(JSON.stringify({ error: { type: "server_error", message: "busy" } }), {
        status: 503, headers: { "content-type": "application/json" },
      }) : completed(),
  });
  assert.equal(attempts, 2);
  assert.equal(response.status, 200);
  await response.text();
});

test("invalid request errors return without retrying", async () => {
  let attempts = 0;
  const response = await retryFetch("https://example.invalid/v1/responses", {}, {
    maxAttempts: 3, budgetMs: 2_000, baseDelayMs: 0, maxDelayMs: 0,
    attempt: () => ({ account: "synthetic-account" }),
    fetchImpl: async () => {
      attempts++;
      return new Response(JSON.stringify({ error: { type: "invalid_request_error", message: "bad request" } }), {
        status: 400, headers: { "content-type": "application/json" },
      });
    },
  });
  assert.equal(response.status, 400);
  assert.equal(attempts, 1);
  await response.text();
});

test("a cancelled client does not send a request", async () => {
  const controller = new AbortController();
  controller.abort(new Error("synthetic client cancelled"));
  let calls = 0;
  await assert.rejects(retryFetch("https://example.invalid", { signal: controller.signal }, {
    attempt: () => ({ account: "synthetic-account" }),
    fetchImpl: async () => { calls++; return completed(); },
  }), /synthetic client cancelled/);
  assert.equal(calls, 0);
});
