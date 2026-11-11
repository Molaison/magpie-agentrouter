import { appendFile, readFile, rename, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { gzip } from "node:zlib";
import { promisify } from "node:util";
import { retryFetch } from "./retry.mjs";
import { createManagedRoute } from "./transport.mjs";
import nativeTools from "./codex-native-tools.mjs";
import { createAccountAffinity } from "./account-affinity.mjs";
import { portableInput, hasResponseHistory, migrationReady } from "../agentrouter/portable-items.mjs";
import { deduplicateToolDefinitions } from "../agentrouter/tool-definitions.mjs";
import { describeRequest, requestAuditIdentity } from "../agentrouter/request-shape.mjs";
import { sessionFailover } from "./session-failover.mjs";
import { normalizeToolSearchHistory } from "./anyrouter-tool-search.mjs";

const gzipBody = promisify(gzip);
const ID = "anyrouter-retry";
const FIELD = "__magpie_anyrouter_retry";
const MODEL_SPECS = {
  "gpt-6.1-sol": {
    name: "AnyRouter GPT-6.1 Sol",
    reasoning: true,
    modalities: { input: ["text", "image"], output: ["text"] },
    limit: { context: 922000, output: 128000 },
  },
  "gpt-6-astra": {
    name: "AnyRouter GPT-6 Astra",
    reasoning: true,
    modalities: { input: ["text", "image"], output: ["text"] },
    limit: { context: 922000, output: 128000 },
  },
  "claude-fable-5-2": {
    name: "AnyRouter Claude Fable 5.2",
    provider: { npm: "@ai-sdk/anthropic" },
    reasoning: true,
    modalities: { input: ["text", "image"], output: ["text"] },
    limit: { context: 1_000_000, output: 128_000 },
  },
};
const MODELS = Object.keys(MODEL_SPECS);
const ANTHROPIC_BETA = [
  "claude-code-20250219",
  "context-1m-2025-08-07",
  "interleaved-thinking-2025-05-14",
  "thinking-token-count-2026-05-13",
  "context-management-2025-06-27",
  "prompt-caching-scope-2026-01-05",
  "mid-conversation-system-2026-04-07",
  "effort-2025-11-24",
].join(",");
const CLAUDE_CODE_VERSION = "2.1.0";
const CLAUDE_DEVICE_ID = randomBytes(32).toString("hex");
const DEFAULT_MAX_ATTEMPTS = 100_000;
const STATE_CAPACITY = 1024;
const STATE_IDLE_TTL_MS = 15 * 60_000;
const active = new Map();
let auditQueue = Promise.resolve();

function audit(path, data) {
  if (!path) return;
  const row = JSON.stringify({ time: new Date().toISOString(), ...data }) + "\n";
  auditQueue = auditQueue.then(async () => {
    const size = await stat(path).then((s) => s.size, () => 0);
    if (size >= 2 * 1024 * 1024) await rename(path, path + ".1");
    await appendFile(path, row, { mode: 0o600 });
  }).catch(() => {});
}

function failure(status, code) {
  return new Response(JSON.stringify({ error: { type: "anyrouter_retry_error", code, message: code } }), {
    status, headers: { "content-type": "application/json" },
  });
}

function hasNativeAccountKey(auth) {
  return auth?.type === "api" && Boolean(auth.key) && auth.key !== "local-provider-reference";
}

function routeUnavailable(kind, retryAfterMs) {
  return new Response(JSON.stringify({
    error: { type: `agentrouter_${kind}_route_unavailable`, message: `AgentRouter ${kind} route is waiting for a connection probe.` },
  }), {
    status: 503,
    headers: { "content-type": "application/json", "retry-after": String(Math.ceil(retryAfterMs / 1000)) },
  });
}

function upstreamURL(base, path, allowLoopback) {
  const u = new URL(base);
  const local = ["127.0.0.1", "localhost", "[::1]"].includes(u.hostname);
  if (u.username || u.password || u.search || u.hash ||
      !(u.protocol === "https:" || allowLoopback && local && u.protocol === "http:")) {
    throw new Error("provider endpoint is outside the configured HTTPS upstream boundary");
  }
  return u.href.replace(/\/$/, "") + path;
}

function sourceProviderIDs(options) {
  const ids = options.sourceProviders;
  if (!Array.isArray(ids) || !ids.length || ids.some((id) => typeof id !== "string" || !id)) {
    throw new Error("sourceProviders must name the configured provider ids to rotate");
  }
  return [...new Set(ids)];
}

// Magpie keeps a provider's extra keys in keys[] (and the ids those keys had
// before a merge in was[]). Each key is its own account for rotation; the
// primary key stays the account that carries the provider id. Ignoring the
// extras silently shrinks the pool, so every key a source record holds counts.
function providerAccounts(provider) {
  if (!provider) return [];
  const extra = Array.isArray(provider.keys)
    ? provider.keys.filter((entry) => entry && typeof entry.key === "string" && entry.key.trim())
    : [];
  if (!extra.length) return [provider];
  const former = Array.isArray(provider.was)
    ? provider.was.filter((id) => typeof id === "string" && id)
    : [];
  const { keys: _keys, was: _was, ...base } = provider;
  const accounts = [base];
  extra.forEach((entry, index) => {
    accounts.push({
      ...base,
      id: former[index] || `${provider.id}#${index + 1}`,
      name: entry.name || provider.name,
      key: entry.key,
      ...(entry.name ? { keyName: entry.name } : {}),
    });
  });
  return accounts;
}

// A merged provider can also have been renamed: the record carries the new id
// and keeps the old one in was[], so a pool that still names the old id must
// still find it instead of silently ending up with one account fewer.
function sourceRecord(providers, id) {
  if (!Array.isArray(providers)) return undefined;
  return providers.find((provider) => provider?.id === id)
    || providers.find((provider) => Array.isArray(provider?.was) && provider.was.includes(id));
}

function sourceRecords(stored, options) {
  const seen = new Set();
  return sourceProviderIDs(options)
    .flatMap((id) => providerAccounts(sourceRecord(stored.providers, id)))
    .filter((account) => !seen.has(account.id) && seen.add(account.id));
}

function configuredBase(stored, options, anthropic) {
  if (options.apiBase) return options.apiBase;
  const source = sourceRecords(stored, options).find((provider) => providerBase(provider, anthropic));
  return source ? providerBase(source, anthropic) : "";
}

function providerBase(provider, anthropic) {
  const base = anthropic
    ? provider.anthropic || provider.chat || provider.responses
    : provider.responses || provider.chat;
  // Magpie stores an explicit Anthropic origin without /v1. The retry
  // transport appends /messages, so normalize here for both discovery and fetch.
  if (!anthropic || !provider.anthropic || !base) return base;
  const url = new URL(base);
  const path = url.pathname.replace(/\/$/, "");
  url.pathname = path.endsWith("/v1") ? path : path + "/v1";
  return url.href.replace(/\/$/, "");
}

function restoreAnthropic(wire) {
  const marker = wire[FIELD];
  const restored = { ...wire };
  delete restored[FIELD];
  return { restored, marker };
}

function anthropicSessionId(body, fallback) {
  try {
    const userId = JSON.parse(body.metadata.user_id);
    if (typeof userId.session_id === "string" && userId.session_id) return userId.session_id;
  } catch {}
  return fallback || randomUUID();
}

function prepareAnthropicBody(body) {
  const prepared = { ...body };
  if (!prepared.max_tokens) prepared.max_tokens = 32000;
  if (!Array.isArray(prepared.system) || prepared.system.length === 0) {
    prepared.system = [
      { type: "text", text: `x-anthropic-billing-header: cc_version=${CLAUDE_CODE_VERSION}; cc_entrypoint=cli;` },
      { type: "text", text: "You are a Claude agent, built on Anthropic's Claude Agent SDK.", cache_control: { type: "ephemeral" } },
    ];
  }
  if (!prepared.context_management) {
    prepared.context_management = { edits: [{ type: "clear_thinking_20251015", keep: "all" }] };
  }
  return prepared;
}

function anthropicHeaders(headers, key, body, marker) {
  const h = new Headers(headers);
  h.delete("content-length");
  h.delete("content-encoding");
  h.delete("transfer-encoding");
  h.set("content-type", "application/json");
  h.set("accept", "application/json");
  h.set("x-api-key", key);
  h.set("authorization", "Bearer " + key);
  h.set("anthropic-version", "2023-06-01");
  h.set("anthropic-beta", ANTHROPIC_BETA);
  h.set("anthropic-dangerous-direct-browser-access", "true");
  h.set("user-agent", `claude-cli/${CLAUDE_CODE_VERSION} (external, cli)`);
  h.set("x-app", "cli");
  h.set("x-claude-code-session-id", anthropicSessionId(body, marker?.requestId));
  h.set("x-stainless-retry-count", "0");
  h.set("x-stainless-timeout", "600");
  h.set("x-stainless-lang", "js");
  h.set("x-stainless-package-version", "0.70.0");
  h.set("x-stainless-os", "Linux");
  h.set("x-stainless-arch", "x64");
  h.set("x-stainless-runtime", "node");
  h.set("x-stainless-runtime-version", process.version);
  return h;
}

export function requestState(marker) {
  const now = Date.now();
  for (const [key, value] of active) {
    const activeFetches = Number.isFinite(value.activeFetches) ? value.activeFetches : 0;
    const lastUsedAt = Number.isFinite(value.lastUsedAt) ? value.lastUsedAt : value.startedAt;
    if (activeFetches === 0 && Number.isFinite(lastUsedAt) && now - lastUsedAt > STATE_IDLE_TTL_MS) {
      active.delete(key);
    }
  }
  const key = marker?.requestId;
  if (key && active.has(key)) return active.get(key);
  const startedAt = Number.isFinite(marker?.startedAt) ? Math.min(now, marker.startedAt) : now;
  const state = { startedAt, attempts: 0, lastUsedAt: now, activeFetches: 0 };
  if (!key) return state;
  if (active.size >= STATE_CAPACITY) {
    let oldestKey;
    let oldestLastUsedAt = Infinity;
    for (const [candidateKey, candidate] of active) {
      if (candidate.activeFetches !== 0) continue;
      const candidateLastUsedAt = Number.isFinite(candidate.lastUsedAt)
        ? candidate.lastUsedAt
        : (Number.isFinite(candidate.startedAt) ? candidate.startedAt : 0);
      if (candidateLastUsedAt < oldestLastUsedAt) {
        oldestLastUsedAt = candidateLastUsedAt;
        oldestKey = candidateKey;
      }
    }
    if (oldestKey === undefined) return null;
    active.delete(oldestKey);
  }
  active.set(key, state);
  return state;
}

function restoreRequest(wire) {
  const marker = wire[FIELD];
  const restored = marker?.request && typeof marker.request === "object" ? { ...marker.request } : { ...wire };
  delete restored[FIELD];
  restored.model = wire.model;
  // Keep an explicit effort/tier picked by Magpie without losing Lite context.
  if (wire.reasoning?.effort !== undefined) restored.reasoning = { ...restored.reasoning, effort: wire.reasoning.effort };
  if (wire.service_tier !== undefined) restored.service_tier = wire.service_tier;
  for (const field of ["max_output_tokens", "max_completion_tokens", "max_tokens"]) {
    if (wire[field] !== undefined) restored[field] = wire[field];
  }
  return { restored, marker };
}

function alignCodexTurnMetadata(body, model) {
  const metadata = body?.client_metadata;
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) return null;
  const raw = metadata["x-codex-turn-metadata"];
  if (typeof raw !== "string") return null;
  try {
    const turn = JSON.parse(raw);
    if (!turn || typeof turn !== "object" || Array.isArray(turn)) return raw;
    if (turn.model === model) return raw;
    turn.model = model;
    const aligned = JSON.stringify(turn);
    body.client_metadata = { ...metadata, "x-codex-turn-metadata": aligned };
    return aligned;
  } catch {
    return raw;
  }
}

function codexLiteShape(body, pinToolItemIds = false, sessionId) {
  if (!body || !Array.isArray(body.input)) return false;
  let changed = false;
  const inputs = [...body.input];
  const tools = Array.isArray(body.tools) ? body.tools : [];
  const liteIndex = inputs.findIndex((item) => item?.type === "additional_tools");

  if (tools.length) {
    if (liteIndex >= 0) {
      const item = { ...inputs[liteIndex] };
      const merged = Array.isArray(item.tools) ? [...item.tools] : [];
      const seen = new Set(merged.map((tool) => JSON.stringify(tool)));
      for (const tool of tools) {
        const key = JSON.stringify(tool);
        if (!seen.has(key)) {
          seen.add(key);
          merged.push(tool);
        }
      }
      if (merged.length !== (item.tools?.length ?? 0)) {
        item.tools = merged;
        inputs[liteIndex] = item;
      }
    } else {
      // A new ID on every request invalidates AgentRouter's cached tool prefix.
      // Existing Lite items keep their client-owned IDs; other pools stay unchanged.
      const cacheIdentity = pinToolItemIds && [body.prompt_cache_key, sessionId, body.client_metadata?.session_id]
        .find(value => typeof value === "string" && value.length > 0);
      const itemId = cacheIdentity
        ? "at_" + createHash("sha256").update(JSON.stringify(cacheIdentity)).digest("hex").slice(0, 32)
        : "at_" + randomUUID();
      inputs.unshift({
        type: "additional_tools",
        id: itemId,
        role: "developer",
        tools,
      });
    }
    delete body.tools;
    body.input = inputs;
    changed = true;
  } else if (Array.isArray(body.tools)) {
    delete body.tools;
    changed = true;
  }

  const reasoning = body.reasoning && typeof body.reasoning === "object" && !Array.isArray(body.reasoning)
    ? body.reasoning
    : {};
  if (reasoning.context !== "all_turns" || reasoning.summary !== undefined) {
    body.reasoning = { ...reasoning, context: "all_turns" };
    delete body.reasoning.summary;
    changed = true;
  }
  if (body.parallel_tool_calls !== false) {
    body.parallel_tool_calls = false;
    changed = true;
  }
  if (body.stream_options !== undefined) {
    delete body.stream_options;
    changed = true;
  }
  if (body.text === undefined || body.text === null) {
    body.text = { verbosity: "low" };
    changed = true;
  }
  return changed;
}

function nativeCatalogEntry(catalog, model) {
  if (!Array.isArray(catalog?.models)) return null;
  return catalog.models.find((entry) => entry?.slug === model) ?? null;
}

async function readNativeCatalog(path, model) {
  if (!path) return null;
  try {
    const catalog = JSON.parse(await readFile(path, "utf8"));
    return nativeCatalogEntry(catalog, model);
  } catch {
    return null;
  }
}

function messageText(item) {
  if (!Array.isArray(item?.content)) return "";
  return item.content.map((part) => typeof part?.text === "string" ? part.text : "").join("");
}

function replaceMessageText(item, text) {
  const first = item.content?.[0] && typeof item.content[0] === "object" ? item.content[0] : {};
  item.content = [{ ...first, type: "input_text", text }];
}

function alignNativeCodex(body, model, tools, alignTools) {
  if (!body || !Array.isArray(body.input)) return false;
  const baseInstructions = typeof model?.base_instructions === "string"
    ? model.base_instructions
    : typeof model?.model_messages?.instructions_template === "string"
      ? model.model_messages.instructions_template
      : "";
  const developers = body.input.filter((item) =>
    item?.type === "message" && item.role === "developer"
  );

  let changed = false;
  if (baseInstructions && developers[0]) {
    replaceMessageText(developers[0], baseInstructions);
    changed = true;
  }

  const collaboration = model?.model_messages?.collaboration_modes?.default;
  if (typeof collaboration === "string" && collaboration && developers[1]) {
    const text = messageText(developers[1]);
    if (!text.includes("</collaboration_mode>")) {
      replaceMessageText(
        developers[1],
        text + "<collaboration_mode>" + collaboration + "</collaboration_mode>",
      );
      changed = true;
    }
  }

  if (alignTools && model?.tool_mode === "code_mode_only" && Array.isArray(tools) && tools.length) {
    const lite = body.input.find((item) => item?.type === "additional_tools");
    if (lite) {
      lite.tools = JSON.parse(JSON.stringify(tools));
      changed = true;
    }
  }
  return changed;
}

export function createRetryPlugin(definition = {}) {
  const providerID = definition.id || ID;
  const providerName = definition.name || "AnyRouter Retry";
  const accountLabel = definition.accountLabel || "AnyRouter";
  const modelIDs = definition.models || MODELS;
  // The reasoning levels published for every one of its models. Codex reads a
  // model offering Ultra as multi-agent V2, and a V2 lead seals its subagents'
  // tasks, which a magpie-served subagent cannot open; a pool that must stay
  // on multi-agent V1 therefore leaves Ultra out.
  const efforts = definition.efforts || ["low", "medium", "high", "xhigh", "max", "ultra"];
  if (!modelIDs.length || modelIDs.some((id) => !MODEL_SPECS[id])) throw new Error("unsupported configured model");
  const modelSpecs = Object.fromEntries(modelIDs.map((id) => [id, {
    ...MODEL_SPECS[id],
    name: definition.modelBrand ? MODEL_SPECS[id].name.replace(/^AnyRouter/, definition.modelBrand) : MODEL_SPECS[id].name,
  }]));
return {
  id: providerID,
  server: async (_input, options = {}) => {
    const nativeAccountAuth = definition.nativeAccountAuth === true;
    const nativeBase = options.apiBase || definition.apiBase;
    const configuredIDs = options.sourceProviders === undefined ? [] : sourceProviderIDs(options);
    if (definition.sourcePrefix && configuredIDs.some((id) => !id.startsWith(definition.sourcePrefix))) {
      throw new Error("sourceProviders crosses the configured provider-pool boundary");
    }
    const providersFile = options.providersFile || join(process.env.XDG_CONFIG_HOME || join(homedir(), ".config"), "magpie", "providers.json");
    const affinity = definition.responsesAffinity ? createAccountAffinity({
      providerID, file: options.affinityFile || join(dirname(providersFile), providerID + "-affinity.json"),
    }) : null;
    const nativeCatalogFile = options.nativeCatalogFile ||
      process.env.CODEX_NATIVE_CATALOG_FILE ||
      join(process.env.CODEX_HOME || join(homedir(), ".codex"), "managed-models-v1.json");
    const nativeToolAlignment = options.nativeToolAlignment === true;
    const requestedFailoverAfter = Number(options.failoverAfter ?? 2);
    const failoverAfter = Number.isFinite(requestedFailoverAfter) && requestedFailoverAfter >= 1
      ? Math.floor(requestedFailoverAfter) : 2;
    const requestedMaxAttempts = Number(options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS);
    const maxAttempts = Number.isFinite(requestedMaxAttempts)
      ? Math.min(DEFAULT_MAX_ATTEMPTS, Math.max(1, requestedMaxAttempts))
      : DEFAULT_MAX_ATTEMPTS;
    const requestedBudgetMs = Number(options.budgetMs ?? 0);
    const budgetMs = Number.isFinite(requestedBudgetMs) ? requestedBudgetMs : 0;
    const phaseTimeouts = options.phaseTimeouts;
    const agentRouterPhases = nativeAccountAuth && phaseTimeouts ? {
      headersMs: phaseTimeouts.headersMs ?? options.attemptTimeoutMs ?? 45_000,
      firstOutputMs: phaseTimeouts.firstOutputMs ?? options.attemptTimeoutMs ?? 45_000,
      outputIdleMs: phaseTimeouts.outputIdleMs ?? 90_000,
      requireContent: true,
    } : null;
    let route;
    let proxyChoice;
    const configuredRoute = async () => {
      let provider;
      try {
        const stored = JSON.parse(await readFile(providersFile, "utf8"));
        provider = stored.providers?.find((p) => p.id === providerID);
      } catch { return; }
      // Per-account and inherited proxy choices belong to Magpie's host fetch.
      const next = Object.values(provider?.accountProxies ?? {}).some(Boolean) ? undefined : provider?.proxy;
      if (next === proxyChoice) return route;
      proxyChoice = next;
      route?.close();
      route = undefined;
      if (next === "direct" || /^https?:\/\//i.test(next ?? "")) {
        route = createManagedRoute(phaseTimeouts.connectMs ?? 5_000,
          (event) => audit(options.auditFile, event), next === "direct" ? undefined : next);
      }
      return route;
    };
    const trackDelivery = providerID === "agentrouter-retry" && options.deliveryLog === true && Boolean(options.auditFile);
    const undelivered = new Map();
    let detailedAuditSamples = 0;
    return {
      ...(nativeAccountAuth ? {
        provider: { id: providerID, models: (given, { auth }) => hasNativeAccountKey(auth) ? given.models : {} },
      } : {}),
      config: async (config) => {
        let api = nativeAccountAuth ? nativeBase : options.apiBase || "";
        if (!api) {
          try {
            const stored = JSON.parse(await readFile(providersFile, "utf8"));
            const base = configuredBase(stored, options, true);
            api = base ? new URL(base).origin + new URL(base).pathname.replace(/\/$/, "") : "";
          } catch {}
        }
        config.provider ||= {};
        config.provider[providerID] = {
          name: providerName,
          npm: "@ai-sdk/openai",
          api,
          searches: true,
          models: Object.fromEntries(modelIDs.map((id) => [id, {
            ...modelSpecs[id],
            ...(modelSpecs[id].provider ? {
              provider: { ...modelSpecs[id].provider, api },
            } : {}),
            variants: Object.fromEntries(efforts.map((effort) => [effort, { reasoningEffort: effort }])),
          }])),
        };
      },
      auth: {
        provider: providerID,
        single: !nativeAccountAuth,
        methods: nativeAccountAuth ? [{ type: "api", label: `${accountLabel} API key` }] : [{
          type: "api", label: `Use existing ${accountLabel} accounts`, keyless: true,
          authorize: async () => ({ type: "success", key: "local-provider-reference" }),
        }],
        loader: async (getAuth) => {
          let baseURL = nativeBase;
          if (!nativeAccountAuth) {
            let stored;
            try { stored = JSON.parse(await readFile(providersFile, "utf8")); }
            catch { throw new Error("the configured source providers are unreadable"); }
            baseURL = configuredBase(stored, options, true);
          }
          if (!baseURL) throw new Error("the configured source providers have no API base");
          return {
            ...(!nativeAccountAuth ? { apiKey: "local-provider-reference" } : {}),
            baseURL,
            fetch: async (url, init) => {
              let wire;
              try { wire = JSON.parse(init.body); } catch { return failure(400, "invalid_json_request"); }
              const pathname = new URL(url).pathname;
              const anthropic = pathname.endsWith("/messages");
              const { restored, marker } = anthropic ? restoreAnthropic(wire) : restoreRequest(wire);
              if (!modelIDs.includes(restored.model)) return failure(400, "unsupported_anyrouter_model");
              const requestId = marker?.requestId || randomUUID();
              const earlyState = trackDelivery ? requestState({ ...marker, requestId: providerID + ":" + requestId }) : undefined;
              if (earlyState === null) return failure(503, "anyrouter_retry_state_capacity");
              const sessionId = trackDelivery && (marker?.sessionId || new Headers(init.headers).get("x-magpie-session") ||
                (anthropic && anthropicSessionId(restored, "")));
              const sessionRef = sessionId ? createHash("sha256").update(sessionId).digest("hex").slice(0, 24) : undefined;
              const recordDelivery = trackDelivery ? (event) => audit(options.auditFile, {
                requestId, sessionRef, model: restored.model, ...event,
              }) : undefined;
              const activeRoute = agentRouterPhases ? await configuredRoute() : undefined;
              const blockedResponse = (retryAfterMs) => {
                if (earlyState && !earlyState.routeBlockedLogged) {
                  earlyState.routeBlockedLogged = true;
                  recordDelivery?.({ kind: `${activeRoute.kind}_route_request_blocked`, retryAfterMs });
                }
                return routeUnavailable(activeRoute.kind, retryAfterMs);
              };
              if (sessionRef && !earlyState.deliverySeen) {
                earlyState.deliverySeen = true;
                const previous = undelivered.get(sessionRef);
                if (previous && Date.now() - previous.at > STATE_IDLE_TTL_MS) undelivered.delete(sessionRef);
                else if (previous && previous.requestId !== requestId && previous.at <= earlyState.startedAt) {
                  recordDelivery({
                    kind: "request_reissued_after_no_output_timeout",
                    previousRequestId: previous.requestId,
                    elapsedSinceTimeoutMs: Date.now() - previous.at,
                  });
                }
              }
              let sources;
              if (nativeAccountAuth) {
                const retryAfterMs = activeRoute?.blockedForMs();
                if (retryAfterMs) return blockedResponse(retryAfterMs);
                const auth = await getAuth();
                if (!hasNativeAccountKey(auth)) return failure(401, "agentrouter_native_sign_in_required");
                sources = [{ id: "magpie-native", key: auth.key, responses: baseURL }];
              } else {
                let current;
                try { current = JSON.parse(await readFile(providersFile, "utf8")); }
                catch { return failure(503, "anyrouter_provider_config_unreadable"); }
                sources = sourceRecords(current, options).filter((p) => p?.key);
              }
              if (definition.allowedOrigins && sources.some((source) => {
                try { return !definition.allowedOrigins.includes(new URL(providerBase(source, anthropic)).origin); }
                catch { return true; }
              })) return failure(400, "source_endpoint_outside_provider_pool");
              if (!sources.length) return failure(503, "no_configured_anyrouter_accounts");
              const path = anthropic
                ? "/messages?beta=true"
                : pathname.endsWith("/responses/compact") ? "/responses/compact" : "/responses";
              const portable = !anthropic && definition.portableResponseItems ? portableInput(restored) : { body: restored, removed: 0 };
              const toolDefinitions = !anthropic && path === "/responses" && nativeAccountAuth
                ? deduplicateToolDefinitions(portable.body) : { body: portable.body, removed: 0 };
              const body = anthropic ? prepareAnthropicBody(restored)
                : providerID === ID ? normalizeToolSearchHistory(portable.body) : toolDefinitions.body;
              const codexLite = anthropic ? false : path === "/responses" && codexLiteShape(body, !!affinity || nativeAccountAuth, marker?.sessionId);
              if (!anthropic && path === "/responses" && options.nativeAlignment !== false) {
                const nativeModel = await readNativeCatalog(nativeCatalogFile, body.model);
                if (nativeModel) alignNativeCodex(body, nativeModel, nativeTools.tools, nativeToolAlignment);
              }
              let headers;
              if (anthropic) {
                headers = anthropicHeaders(init.headers, "", body, marker);
              } else {
                headers = new Headers(init.headers);
                headers.delete("content-length");
                headers.delete("x-api-key");
                headers.set("content-type", "application/json");
                headers.set("accept", body.stream ? "text/event-stream" : "application/json");
                if (!headers.get("user-agent")) {
                  headers.set("user-agent", options.userAgent || "codex_cli_rs/0.164.0 (Linux; x86_64)");
                }
                if (!headers.has("originator")) headers.set("originator", "codex_cli_rs");
                if (codexLite || Array.isArray(body.input) && body.input.some((item) => item.type === "additional_tools")) {
                  headers.set("x-openai-internal-codex-responses-lite", "true");
                }
                const turn = alignCodexTurnMetadata(body, body.model);
                if (turn) headers.set("x-codex-turn-metadata", turn);
              }
              let cursor = 0;
              const state = earlyState ?? requestState(definition.id ? { ...marker, requestId: providerID + ":" + requestId } : marker);
              if (!state) return failure(503, "anyrouter_retry_state_capacity");
              let pinned;
              let binding;
              let affinityArgs;
              let migration;
              if (!anthropic && affinity) {
                try {
                  affinityArgs = { body, headers, sessionId: marker?.sessionId,
                    requestId: marker?.requestId || randomUUID(), sources, hasHistory: hasResponseHistory(body) };
                  binding = await affinity.resolve(affinityArgs);
                  pinned = binding.source;
                } catch {
                  return failure(503, "agentrouter_affinity_state_unavailable");
                }
                if (options.accountFailover !== false) {
                  state.failover ||= { account: state.account || binding.account };
                  state.disabledAccounts ||= new Set();
                  migration = sessionFailover({ sources, account: binding.account,
                    portable: migrationReady(body), state: state.failover,
                    after: failoverAfter,
                    onMove: (event) => audit(options.auditFile, {
                      requestId: marker?.requestId, model: body.model, kind: "account_migration_attempt", ...event,
                    }),
                  });
                } else if (!pinned) return failure(503, "agentrouter_bound_account_unavailable");
              }
              const permit = activeRoute?.admit();
              if (permit && !permit.fetch) return blockedResponse(permit.retryAfterMs);
              state.activeFetches = (Number.isFinite(state.activeFetches) ? state.activeFetches : 0) + 1;
              try {
                const serializedBody = JSON.stringify(body);
                if (options.requestBodyAudit === true && recordDelivery) {
                  // Bounded, opt-in item fingerprints contain no request text.
                  // Audit identity does not change routing or delivery state.
                  const identities = requestAuditIdentity(body, headers, marker);
                  const detailed = detailedAuditSamples < 16 &&
                    Array.isArray(options.requestBodyDetailSessions) &&
                    identities.some(id => options.requestBodyDetailSessions.includes(id));
                  try {
                    const shape = describeRequest(body, serializedBody, detailed);
                    if (detailed) detailedAuditSamples++;
                    recordDelivery({
                      kind: "request_prepared", attempt: state.attempts + 1,
                      wireBodyBytes: Buffer.byteLength(init.body),
                      upstreamBodyBytes: Buffer.byteLength(serializedBody),
                      bodySha256: createHash("sha256").update(serializedBody).digest("hex"),
                      auditSessionRef: identities[0] ? createHash("sha256").update(identities[0]).digest("hex").slice(0, 24) : undefined,
                      toolDefinitionsRemoved: toolDefinitions.removed + (marker?.toolDefinitionsRemoved || 0),
                      ...shape,
                    });
                  } catch {
                    // Diagnostics must never prevent forwarding a valid request.
                  }
                }
                let nativeFetch = permit?.fetch;
                if (providerID === "agentrouter-retry" && agentRouterPhases &&
                    !anthropic && path === "/responses") {
                  const plainBytes = Buffer.byteLength(serializedBody);
                  const compressionStarted = performance.now();
                  const compressed = plainBytes >= 64 * 1024 ? await gzipBody(serializedBody) : null;
                  const wireBody = compressed && compressed.length < plainBytes ? compressed : serializedBody;
                  const encoding = typeof wireBody === "string" ? "identity" : "gzip";
                  const compressionMs = Math.round((performance.now() - compressionStarted) * 100) / 100;
                  const send = nativeFetch ?? globalThis.fetch;
                  // The retry layer retains JSON; encode only at its final fetch boundary.
                  nativeFetch = (input, requestInit) => {
                    const wireHeaders = new Headers(requestInit.headers);
                    wireHeaders.delete("content-length");
                    wireHeaders.delete("content-encoding");
                    if (encoding === "gzip") wireHeaders.set("content-encoding", encoding);
                    recordDelivery?.({ kind: "upstream_body", encoding, plainBytes,
                      wireBytes: Buffer.byteLength(wireBody), compressionMs });
                    return send(input, { ...requestInit, headers: wireHeaders, body: wireBody });
                  };
                }
                let response = await retryFetch(url, { ...init, body: serializedBody, headers }, {
                  state, budgetMs,
                  maxAttempts: nativeAccountAuth ? Math.min(maxAttempts, state.attempts + 1) : maxAttempts,
                  onDelivery: recordDelivery && ((event) => {
                    if (event.kind === "first_content_forwarded") {
                      state.delivered = true;
                      const previous = undelivered.get(sessionRef);
                      if (previous && previous.startedAt <= state.startedAt) undelivered.delete(sessionRef);
                    }
                    recordDelivery(event);
                  }),
                  ...(agentRouterPhases ? { phaseTimeouts: agentRouterPhases, fetchImpl: nativeFetch,
                    onPhase: (event) => {
                      if (["upstream_failure", "phase_timeout", "client_abort"].includes(event.kind)) {
                        recordDelivery?.(event);
                      }
                    },
                  } : anthropic && phaseTimeouts ? { phaseTimeouts,
                    onPhase: (event) => audit(options.auditFile, {
                      requestId: marker?.requestId, sessionId: anthropicSessionId(body, marker?.requestId),
                      model: body.model, ...event,
                    }),
                  } : {}),
                  accountFailover: !!migration,
                  disabledAccounts: migration ? state.disabledAccounts : undefined,
                  attemptTimeoutMs: options.attemptTimeoutMs ?? 45_000,
                  baseDelayMs: options.baseDelayMs ?? 2000,
                  maxDelayMs: options.maxDelayMs ?? 4000,
                  jitter: options.jitter,
                  retryDelayMode: options.retryDelayMode ?? "uniform",
                  attempt: (_n, disabled) => {
                    const preferred = migration ? migration.next(disabled) : pinned;
                    if (migration && !preferred) return null;
                    for (let i = 0; i < (preferred ? 1 : sources.length); i++) {
                      const source = preferred || sources[cursor++ % sources.length];
                      if (disabled.has(source.id)) continue;
                      if (migration) state.account = source.id;
                      const h = new Headers(headers);
                      if (anthropic) {
                        h.set("x-api-key", source.key);
                        h.set("authorization", "Bearer " + source.key);
                      } else {
                        h.set("authorization", "Bearer " + source.key);
                      }
                      const base = providerBase(source, anthropic);
                      return {
                        url: upstreamURL(base, path, options.allowLoopback === true),
                        account: source.id,
                        init: { headers: h },
                      };
                    }
                    return null;
                  },
                  onRetry: (event) => {
                    state.lastUsedAt = Date.now();
                    migration?.observe(event);
                    console.error("[anyrouter-retry] " + JSON.stringify({ requestId: marker?.requestId, ...event }));
                    audit(options.auditFile, { requestId: marker?.requestId, model: restored.model, ...event });
                  },
                });
                // Do not add a plugin cooldown. Magpie still owns native
                // account cooldowns, bounded retry rounds and their pacing.
                if (nativeAccountAuth && !response.ok) {
                  response.headers.delete("retry-after");
                }
                if (recordDelivery && response.status === 504 && !state.delivered && !state.deliveryTimeoutRecorded) {
                  state.deliveryTimeoutRecorded = true;
                  const at = Date.now();
                  if (sessionRef && (undelivered.get(sessionRef)?.startedAt ?? -Infinity) <= state.startedAt) {
                    undelivered.delete(sessionRef);
                    if (undelivered.size >= STATE_CAPACITY) undelivered.delete(undelivered.keys().next().value);
                    undelivered.set(sessionRef, { at, requestId, startedAt: state.startedAt });
                  }
                  recordDelivery({ kind: "request_no_output_timeout", status: 504, elapsedMs: at - state.startedAt });
                }
                if (migration?.state.blocked) {
                  response.body?.cancel().catch(() => {});
                  audit(options.auditFile, { requestId: marker?.requestId, model: body.model,
                    kind: "account_migration_blocked", reason: "unresolved_resource_reference" });
                  return failure(409, "agentrouter_migration_requires_full_history");
                }
                const accepted = response.headers.get("x-anyrouter-account");
                if (migration && response.ok && accepted) {
                  migration.state.failures = 0;
                  migration.state.move = false;
                  if (accepted !== binding.account) {
                    try {
                      const committed = await affinity.rebind(affinityArgs, {
                        expectedAccount: binding.account, account: accepted,
                      });
                      audit(options.auditFile, { requestId: marker?.requestId, model: body.model,
                        kind: "account_migrated", from: binding.account, to: accepted,
                        bindingUpdated: committed.changed, bindingAccount: committed.account });
                    } catch {
                      // An accepted model/tool response must survive a metadata
                      // write failure. A later request can retry the migration.
                      audit(options.auditFile, { requestId: marker?.requestId, model: body.model,
                        kind: "affinity_commit_failed", account: accepted });
                    }
                  }
                }
                audit(options.auditFile, {
                  requestId: marker?.requestId, model: body.model, kind: "response_started",
                  attempt: state.attempts, account: response.headers.get("x-anyrouter-account"), status: response.status,
                  ...(binding ? { accountAffinity: "session", accountFailover: !!migration,
                    portableItemIds: portable.removed + (marker?.portableItemIds || 0) } : {}),
                });
                return response;
              } finally {
                permit?.release();
                state.activeFetches = Math.max(0, state.activeFetches - 1);
                state.lastUsedAt = Date.now();
              }
            },
          };
        },
      },
    };
  },
};

}

export default createRetryPlugin();
