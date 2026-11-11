// Bun's built-in "undici" shim ignores parts of the dispatcher API. Resolve
// the pinned package explicitly so its real connector deadlines are used.
import { Agent, EnvHttpProxyAgent, Pool, ProxyAgent, buildConnector, fetch } from 'undici/index.js';

const MAGPIE_ONLY = [
  'x-magpie-work-dir', 'x-magpie-work-git', 'x-magpie-fallback', 'x-magpie-session', 'x-magpie-subagent',
];
const CONNECTION_FAILURES = new Set([
  'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_PRX_TLS', 'UND_ERR_SOCKET',
  'ECONNRESET', 'ECONNREFUSED', 'EPIPE',
  'ENETUNREACH', 'EHOSTUNREACH', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN',
  'CERT_HAS_EXPIRED', 'DEPTH_ZERO_SELF_SIGNED_CERT', 'SELF_SIGNED_CERT_IN_CHAIN', 'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
]);
const ROUTE_FAILURE_LIMIT = 3;
const ROUTE_PROBE_INTERVAL_MS = 15_000;

// Standard fetch cannot distinguish connecting from waiting for response headers.
// Undici's connector deadline stops at TCP/TLS establishment (including DNS).
export function createPhasedFetch(connectTimeoutMs) {
  const all = process.env.all_proxy || process.env.ALL_PROXY;
  const dispatcher = new EnvHttpProxyAgent({
    connect: { timeout: connectTimeoutMs },
    proxyTls: { timeout: connectTimeoutMs },
    requestTls: { timeout: connectTimeoutMs },
    headersTimeout: 0,
    bodyTimeout: 0,
    httpProxy: process.env.http_proxy || process.env.HTTP_PROXY || all,
    httpsProxy: process.env.https_proxy || process.env.HTTPS_PROXY || all,
    noProxy: process.env.no_proxy ?? process.env.NO_PROXY,
  });
  return (input, init) => fetch(input, { ...init, dispatcher });
}

function connectionFailure(error) {
  const code = error?.cause?.code ?? error?.code;
  return typeof code === 'string' && (CONNECTION_FAILURES.has(code) ||
    code.startsWith('ERR_TLS_') || code.startsWith('ERR_SSL_')) ? code : null;
}

// Accounts using the same proxy or direct connection share one route. An HTTP
// response, even an error, confirms that the path reached the upstream.
export function createManagedRoute(connectTimeoutMs, onState, proxy) {
  const kind = proxy ? 'proxy' : 'direct';
  const report = (event, fields) => onState?.({ kind: `${kind}_${event}`, ...fields });
  const observeConnection = (connect) => (options, callback) => {
    const startedAt = Date.now();
    let finished = false;
    const timer = setTimeout(() => {
      finish(Object.assign(new Error('AgentRouter connection deadline exceeded'), { code: 'UND_ERR_CONNECT_TIMEOUT' }));
    }, connectTimeoutMs);
    timer.unref?.();
    function finish(error, socket) {
      if (finished) {
        socket?.destroy();
        return;
      }
      finished = true;
      clearTimeout(timer);
      report('connect', { elapsedMs: Date.now() - startedAt, connected: !error,
        ...(error ? { code: connectionFailure(error) } : {}) });
      callback(error, socket);
    }
    connect(options, finish);
  };
  const dispatcher = proxy ? new ProxyAgent({
    uri: proxy,
    proxyTls: { timeout: connectTimeoutMs },
    requestTls: { timeout: connectTimeoutMs },
    clientFactory: (origin, options) => new Pool(origin, { ...options, headersTimeout: connectTimeoutMs }),
    factory: (origin, options) => new Pool(origin, { ...options, connect: observeConnection(options.connect) }),
  }) : new Agent({ connect: observeConnection(buildConnector({ timeout: connectTimeoutMs })) });
  let failures = 0;
  let failureWindowAt = 0;
  let probeAt = 0;
  let probing = false;
  let generation = 0;
  const blockedForMs = () => probing ? ROUTE_PROBE_INTERVAL_MS : Math.max(0, probeAt - Date.now());

  return {
    kind,
    close: () => dispatcher.close().catch(() => {}),
    blockedForMs,
    admit() {
      const retryAfterMs = blockedForMs();
      if (retryAfterMs) return { retryAfterMs };
      const probe = probeAt !== 0;
      if (probe) probing = true;
      const admitted = generation;
      const release = () => {
        if (probe && admitted === generation) probing = false;
      };
      return {
        release,
        async fetch(input, init) {
          const headers = new Headers(init.headers);
          for (const name of MAGPIE_ONLY) headers.delete(name);
          if (probe) report('route_probe');
          try {
            const response = await fetch(input, { ...init, headers, dispatcher, redirect: 'manual' });
            if (admitted === generation) {
              failures = 0;
              if (probe) report('route_recovered', { status: response.status });
              probeAt = 0;
            }
            return response;
          } catch (error) {
            const code = connectionFailure(error);
            if (code && !init.signal?.aborted && admitted === generation) {
              const at = Date.now();
              if (!failures || at - failureWindowAt > ROUTE_PROBE_INTERVAL_MS) {
                failures = 0;
                failureWindowAt = at;
              }
              failures++;
              if (probe || failures >= ROUTE_FAILURE_LIMIT) {
                probeAt = at + ROUTE_PROBE_INTERVAL_MS;
                probing = false;
                generation++;
                report('route_opened', { code, retryAfterMs: ROUTE_PROBE_INTERVAL_MS });
              }
            }
            throw error;
          } finally {
            release();
          }
        },
      };
    },
  };
}
