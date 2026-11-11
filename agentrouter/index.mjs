import { createRetryPlugin } from "../src/index.mjs";
export default createRetryPlugin({
  id: "agentrouter-retry", name: "AgentRouter Retry", accountLabel: "AgentRouter", modelBrand: "AgentRouter",
  models: ["gpt-6.1-sol", "gpt-6-astra", "claude-fable-5-2"], sourcePrefix: "agentrouter-", allowedOrigins: ["https://agentrouter.org"],
  nativeAccountAuth: true,
  apiBase: "https://agentrouter.org/v1",
  portableResponseItems: true,
  // No Ultra: Codex stamps a model offering Ultra as multi-agent V2, and its
  // V2 lead seals subagent tasks that a magpie-served subagent cannot open.
  // Keeping these entries V1 is what makes the AgentRouter groups usable as
  // leads over magpie's other models.
  efforts: ["low", "medium", "high", "xhigh", "max"],
});
