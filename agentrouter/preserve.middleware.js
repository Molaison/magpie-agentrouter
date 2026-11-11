import { portableInput } from "./portable-items.mjs";
const field = "__magpie_anyrouter_retry";
export function onRequest(body, ctx) {
  ctx.state.agentrouterRetry = { requestId: Date.now().toString(36) + "-" + Math.random().toString(36).slice(2), startedAt: Date.now(),
    sessionId: typeof ctx.session === "string" ? ctx.session : undefined };
}
export function onProvider(body, ctx) {
  if (ctx.provider?.id !== "agentrouter-retry" || !["responses", "anthropic"].includes(ctx.protocol)) return;
  const portable = ctx.protocol === "responses" ? portableInput(body) : { body, removed: 0 };
  const original = { ...portable.body }; delete original[field];
  return { ...original, [field]: { ...ctx.state.agentrouterRetry, portableItemIds: portable.removed, request: original } };
}
