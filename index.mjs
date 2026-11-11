import agentRouter from "./agentrouter/index.mjs";

// Carry the installed AgentRouter settings into a standalone installation.
// Native account rotation and retry pacing remain owned by Magpie.
export const defaults = Object.freeze({
  maxAttempts: 100_000,
  budgetMs: 0,
  attemptTimeoutMs: 45_000,
  baseDelayMs: 2_000,
  maxDelayMs: 4_000,
  retryDelayMode: "uniform",
  phaseTimeouts: Object.freeze({
    connectMs: 5_000,
    headersMs: 45_000,
    firstOutputMs: 120_000,
    outputIdleMs: 90_000,
  }),
});

export default {
  ...agentRouter,
  server(input, options = {}) {
    return agentRouter.server(input, {
      ...defaults,
      ...options,
      phaseTimeouts: options.phaseTimeouts === null ? null : {
        ...defaults.phaseTimeouts,
        ...options.phaseTimeouts,
      },
    });
  },
};
