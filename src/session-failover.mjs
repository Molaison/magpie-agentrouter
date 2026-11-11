// Per-request selection state is retained across native gateway re-entry.
// Successful session ownership is committed separately by account-affinity.
export function sessionFailover({ sources, account, portable, state = {}, after = 2, onMove }) {
  if (!state.account) state.account = account;
  if (!Number.isFinite(state.failures)) state.failures = 0;
  state.blocked = false;
  return {
    state,
    observe(event) {
      if (event.account !== state.account) return;
      state.failures++;
      state.reason = event.kind;
      if (event.kind === 'account' || event.kind === 'resource' || state.failures >= after) state.move = true;
    },
    next(disabled) {
      const current = sources.find(source => source.id === state.account);
      if (current && !disabled.has(current.id) && !state.move) return current;
      if (!portable) { state.blocked = true; return null; }
      const start = sources.findIndex(source => source.id === state.account);
      for (let offset = 1; offset <= sources.length; offset++) {
        const source = sources[(start + offset + sources.length) % sources.length];
        if (disabled.has(source.id)) continue;
        const from = state.account;
        state.account = source.id;
        state.failures = 0;
        state.move = false;
        if (from !== source.id) onMove?.({ from, to: source.id, reason: state.reason || 'bound_account_missing' });
        return source;
      }
      return null;
    },
  };
}
