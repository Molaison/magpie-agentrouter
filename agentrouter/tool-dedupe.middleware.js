import { deduplicateToolDefinitions } from "./tool-definitions.mjs";
const FIELD = "__magpie_anyrouter_retry";

// Hot-load counterpart for an already running provider.
export function onProvider(body, ctx) {
  if (ctx.provider?.id !== "agentrouter-retry" || ctx.protocol !== "responses") return;
  if (ctx.path?.endsWith("/compact")) return;
  const marker = body?.[FIELD];
  if (!marker?.request) return;
  const normalized = deduplicateToolDefinitions(marker.request);
  if (normalized.body === marker.request) return;
  return { ...body, [FIELD]: { ...marker, request: normalized.body,
    toolDefinitionsRemoved: normalized.removed } };
}
