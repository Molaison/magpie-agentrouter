const DEFAULTS = Object.freeze({
  maxAttempts: 100_000,
  budgetMs: 0,
  attemptTimeoutMs: 45_000,
  baseDelayMs: 2_000,
  maxDelayMs: 4_000,
  jitter: 0.2,
  retryDelayMode: 'uniform',
});

const ERROR_BODY_LIMIT = 64 * 1024;
const ERROR_BODY_WAIT_MS = 250;
const JSON_BODY_LIMIT = 1024 * 1024;
const SSE_BUFFER_LIMIT = 1024 * 1024;
const BUSY_ERROR_CODES = new Set([
  'get_channel_failed',
  'server_is_overloaded',
  'overloaded_error',
  'server_error',
  'internal_error',
  'internal_server_error',
  'no_capacity',
  'upstream_response_failed',
]);
const ACCOUNT_ERROR_CODES = new Set(['insufficient_quota']);
const INVALID_ERROR_CODES = new Set([
  'invalid_codex_request',
  'invalid_responses_request',
  'context_length_exceeded',
  'invalid_request_error',
  'validation_error',
]);
const RATE_ERROR_CODES = new Set(['rate_limit_exceeded', 'rate_limit_error']);
const KNOWN_ERROR_CODES = new Set([
  ...BUSY_ERROR_CODES,
  ...ACCOUNT_ERROR_CODES,
  ...INVALID_ERROR_CODES,
  ...RATE_ERROR_CODES,
]);

class RetryDeadlineError extends Error {
  constructor(kind) {
    super(kind === 'budget' ? 'retry budget exceeded' : 'attempt timeout exceeded');
    this.name = 'RetryDeadlineError';
    this.kind = kind;
  }
}

export async function retryFetch(input, init = {}, options = {}) {
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  if (typeof fetchImpl !== 'function') {
    throw new TypeError('options.fetchImpl must be a function');
  }
  if (typeof options.attempt !== 'function') {
    throw new TypeError('options.attempt must be a function');
  }

  const maxAttempts = numberOption(options.maxAttempts, DEFAULTS.maxAttempts);
  const budgetMs = numberOption(options.budgetMs, DEFAULTS.budgetMs);
  const attemptTimeoutMs = numberOption(options.attemptTimeoutMs, DEFAULTS.attemptTimeoutMs);
  const baseDelayMs = numberOption(options.baseDelayMs, DEFAULTS.baseDelayMs);
  const maxDelayMs = numberOption(options.maxDelayMs, DEFAULTS.maxDelayMs);
  const jitter = numberOption(options.jitter, DEFAULTS.jitter);
  const retryDelayMode = options.retryDelayMode === 'exponential'
    ? 'exponential'
    : DEFAULTS.retryDelayMode;

  const state = options.state ?? { startedAt: Date.now(), attempts: 0 };
  if (!Number.isFinite(state.startedAt)) state.startedAt = Date.now();
  if (!Number.isFinite(state.attempts)) state.attempts = 0;
  const sharedFailure = options.phaseTimeouts?.requireContent === true ? state : {};

  const deadlineAt = budgetMs === 0 ? Infinity : state.startedAt + budgetMs;
  const disabledAccounts = options.disabledAccounts ?? new Set();
  const clientSignal = options.signal ?? init?.signal ?? input?.signal;
  const onDelivery = typeof options.onDelivery === 'function' ? options.onDelivery : null;
  let replayBody;
  let retryIndex = 0;
  let lastFailure = sharedFailure.lastFailure ?? null;
  let lastAccount = 'unknown';

  if (clientSignal?.aborted) throw abortReason(clientSignal);

  while (true) {
    if (clientSignal?.aborted) throw abortReason(clientSignal);
    if (state.attempts >= maxAttempts) {
      return failureResponse(lastFailure, state.attempts, lastAccount, 'max_attempts');
    }
    if (Date.now() >= deadlineAt) {
      return failureResponse(lastFailure, state.attempts, lastAccount, 'budget');
    }

    const attemptNo = state.attempts + 1;
    let selected;
    try {
      selected = await raceSignal(Promise.resolve(options.attempt(attemptNo, disabledAccounts)), clientSignal);
    } catch (error) {
      if (clientSignal?.aborted) throw abortReason(clientSignal);
      throw error;
    }
    if (selected == null) {
      return failureResponse(lastFailure, state.attempts, lastAccount, 'no_accounts');
    }

    lastAccount = safeAccountLabel(selected.account);
    if (disabledAccounts.has(accountKey(selected.account, lastAccount))) {
      return failureResponse(lastFailure, state.attempts, lastAccount, 'account_disabled');
    }

    const attemptInit = selected.init ?? {};
    const body = Object.prototype.hasOwnProperty.call(attemptInit, 'body')
      ? attemptInit.body
      : init?.body;
    validateReplayBody(body, replayBody);
    if (body != null) replayBody = body;

    const requestInit = buildRequestInit(init, attemptInit, body);
    const attemptSignal = attemptInit.signal ?? init?.signal ?? clientSignal;
    if (attemptSignal?.aborted) throw abortReason(attemptSignal);

    const delivery = onDelivery
      ? createDeliveryObserver(onDelivery, attemptNo, lastAccount)
      : null;
    const ctx = options.phaseTimeouts
      ? createPhasedDeadline(attemptSignal, deadlineAt, options.phaseTimeouts,
        event => options.onPhase?.({ attempt: attemptNo, account: lastAccount, ...event }), delivery)
      : createDeadline(attemptSignal, attemptTimeoutMs, deadlineAt, delivery);
    requestInit.signal = ctx.signal;

    state.attempts += 1;
    let response;
    try {
      if (ctx.signal.aborted) {
        throw ctx.signal.reason ?? new RetryDeadlineError('attempt_timeout');
      }
      delivery?.requestSent();
      response = await raceAbort(
        Promise.resolve(fetchImpl(selected.url ?? input, requestInit)),
        ctx,
      );
      ctx.receivedHeaders?.(response.status);
    } catch (error) {
      ctx.failed?.(error);
      const failure = failureFromThrown(error, ctx, attemptSignal);
      ctx.clearTimers();
      ctx.dispose();
      if (failure.clientAbort) throw abortReason(attemptSignal);
      lastFailure = sharedFailure.lastFailure = failure.failure;
      if (failure.stop) return failureResponse(lastFailure, state.attempts, lastAccount, failure.failure.kind);
      if (state.attempts >= maxAttempts) {
        return failureResponse(lastFailure, state.attempts, lastAccount, 'max_attempts');
      }
      const delayMs = retryDelay(retryIndex++, baseDelayMs, maxDelayMs, jitter, null, retryDelayMode);
      if (delayMs > deadlineAt - Date.now()) {
        return failureResponse(lastFailure, state.attempts, lastAccount, 'budget');
      }
      emitRetry(options.onRetry, state.attempts, lastAccount, lastFailure.kind, null, delayMs);
      await abortableSleep(delayMs, attemptSignal);
      continue;
    }

    let outcome;
    try {
      outcome = await inspectResponse(response, ctx, state.attempts, lastAccount, options.accountFailover === true,
        delivery);
    } catch (error) {
      ctx.failed?.(error);
      cancelBody(response);
      const failure = failureFromThrown(error, ctx, attemptSignal);
      ctx.clearTimers();
      ctx.dispose();
      if (failure.clientAbort) throw abortReason(attemptSignal);
      lastFailure = sharedFailure.lastFailure = failure.failure;
      if (failure.stop) return failureResponse(lastFailure, state.attempts, lastAccount, failure.failure.kind);
      if (state.attempts >= maxAttempts) {
        return failureResponse(lastFailure, state.attempts, lastAccount, 'max_attempts');
      }
      const delayMs = retryDelay(retryIndex++, baseDelayMs, maxDelayMs, jitter, null, retryDelayMode);
      if (delayMs > deadlineAt - Date.now()) {
        return failureResponse(lastFailure, state.attempts, lastAccount, 'budget');
      }
      emitRetry(options.onRetry, state.attempts, lastAccount, lastFailure.kind, null, delayMs);
      await abortableSleep(delayMs, attemptSignal);
      continue;
    }

    if (outcome.type === 'return') {
      return outcome.response;
    }

    if (options.preserveRetryAfter === true && outcome.type === 'retry' && outcome.retryAfterMs != null) {
      outcome.failure.retryAfterAt = Date.now() + outcome.retryAfterMs;
    }

    if (outcome.type === 'account') {
      disabledAccounts.add(accountKey(selected.account, lastAccount));
      lastFailure = sharedFailure.lastFailure = outcome.failure;
      if (state.attempts >= maxAttempts) {
        ctx.clearTimers();
        ctx.dispose();
        cancelBody(response);
        return failureResponse(lastFailure, state.attempts, lastAccount, 'max_attempts');
      }
      const delayMs = retryDelay(
        retryIndex++, baseDelayMs, maxDelayMs, jitter,
        parseRetryAfter(response.headers, Date.now()), retryDelayMode,
      );
      ctx.clearTimers();
      ctx.dispose();
      cancelBody(response);
      if (delayMs > deadlineAt - Date.now()) {
        return failureResponse(lastFailure, state.attempts, lastAccount, 'budget');
      }
      emitRetry(options.onRetry, state.attempts, lastAccount, 'account', outcome.failure.status, delayMs);
      await abortableSleep(delayMs, attemptSignal);
      continue;
    }

    lastFailure = sharedFailure.lastFailure = outcome.failure;
    if (state.attempts >= maxAttempts) {
      ctx.clearTimers();
      ctx.dispose();
      cancelBody(response);
      return failureResponse(lastFailure, state.attempts, lastAccount, 'max_attempts');
    }
    const delayMs = retryDelay(
      retryIndex++,
      baseDelayMs,
      maxDelayMs,
      jitter,
      outcome.retryAfterMs,
      retryDelayMode,
    );
    ctx.clearTimers();
    ctx.dispose();
    cancelBody(response);

    if (delayMs > deadlineAt - Date.now()) {
      return failureResponse(lastFailure, state.attempts, lastAccount, 'budget');
    }
    emitRetry(
      options.onRetry,
      state.attempts,
      lastAccount,
      lastFailure.kind,
      lastFailure.status,
      delayMs,
    );
    await abortableSleep(delayMs, attemptSignal);
  }
}

function numberOption(value, fallback) {
  return Number.isFinite(value) ? Number(value) : fallback;
}

function createDeliveryObserver(callback, attempt, account) {
  const startedAt = Date.now();
  let stage = 'response_headers';
  let contentForwarded = false;
  let terminalReported = false;

  const emit = (type, extra = {}) => {
    try {
      callback({ kind: type, attempt, account, elapsedMs: Date.now() - startedAt, ...extra });
    } catch {
      // Diagnostics must not affect retry behavior.
    }
  };

  return {
    requestSent() {
      emit('request_sent');
    },
    headers(status) {
      stage = 'first_output';
      emit('upstream_headers', { status });
    },
    upstreamError(info, httpStatus, normalizedStatus) {
      const fields = { httpStatus, normalizedStatus };
      for (const key of ['type', 'code']) {
        if (typeof info[key] === 'string' && /^[a-zA-Z0-9_.-]{1,80}$/.test(info[key])) fields[key] = info[key];
      }
      emit('upstream_error', fields);
    },
    content() {
      if (contentForwarded) return;
      contentForwarded = true;
      emit('first_content_forwarded');
    },
    failed() {
      if (!contentForwarded) terminalReported = true;
    },
    deadline(kind, phase) {
      if (terminalReported || (contentForwarded && phase !== 'output_idle')) return;
      terminalReported = true;
      emit('delivery_timeout', {
        stage: phase ?? stage,
        deadline: kind === 'budget' ? 'budget' : 'attempt',
      });
    },
    cancel() {
      if (contentForwarded || terminalReported) return;
      terminalReported = true;
      emit('client_cancel', { stage });
    },
  };
}

function validateReplayBody(body, replayBody) {
  if (body != null && typeof body !== 'string') {
    throw new TypeError('retryFetch requires a replayable string request body');
  }
  if (replayBody !== undefined && body !== replayBody) {
    throw new TypeError('request body changed between retry attempts');
  }
}

function buildRequestInit(baseInit, attemptInit, body) {
  const requestInit = { ...(baseInit ?? {}), ...attemptInit };
  const headers = new Headers(
    Object.prototype.hasOwnProperty.call(attemptInit, 'headers')
      ? attemptInit.headers
      : baseInit?.headers,
  );
  headers.delete('content-length');
  headers.delete('content-encoding');
  headers.delete('transfer-encoding');
  requestInit.headers = headers;
  if (body !== undefined) requestInit.body = body;
  return requestInit;
}

function createDeadline(clientSignal, attemptTimeoutMs, deadlineAt, delivery) {
  const controller = new AbortController();
  let timedOut = null;
  let disposed = false;
  let budgetTimer;
  let attemptTimer;

  const abortWith = (error) => {
    if (controller.signal.aborted) return;
    delivery?.deadline(error?.kind);
    timedOut = error.kind;
    controller.abort(error);
  };
  const onClientAbort = () => {
    delivery?.cancel();
    if (!controller.signal.aborted) controller.abort(abortReason(clientSignal));
  };

  if (clientSignal) {
    if (clientSignal.aborted) onClientAbort();
    else clientSignal.addEventListener('abort', onClientAbort, { once: true });
  }

  const remainingBudget = deadlineAt - Date.now();
  if (!controller.signal.aborted && Number.isFinite(remainingBudget)) {
    if (remainingBudget <= 0) {
      abortWith(new RetryDeadlineError('budget'));
    } else {
      budgetTimer = setTimeout(() => abortWith(new RetryDeadlineError('budget')), remainingBudget);
      budgetTimer.unref?.();
    }
  }
  if (!controller.signal.aborted && attemptTimeoutMs > 0) {
    attemptTimer = setTimeout(
      () => abortWith(new RetryDeadlineError('attempt_timeout')),
      attemptTimeoutMs,
    );
    attemptTimer.unref?.();
  }

  return {
    signal: controller.signal,
    deliveryEnabled: delivery != null,
    get timedOut() {
      return timedOut;
    },
    receivedHeaders(status) {
      delivery?.headers(status);
    },
    forwardContent() {
      delivery?.content();
    },
    clientCanceled() {
      delivery?.cancel();
    },
    failed() {
      delivery?.failed();
    },
    clearTimers() {
      if (budgetTimer) clearTimeout(budgetTimer);
      if (attemptTimer) clearTimeout(attemptTimer);
      budgetTimer = undefined;
      attemptTimer = undefined;
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      this.clearTimers();
      clientSignal?.removeEventListener('abort', onClientAbort);
    },
  };
}

function createPhasedDeadline(clientSignal, deadlineAt, policy, emit, delivery) {
  const controller = new AbortController();
  const startedAt = Date.now();
  const requireContent = policy.requireContent === true;
  let phase = 'response_headers';
  let phaseAt = startedAt;
  let phaseTimer;
  let budgetTimer;
  let timedOut = null;
  let disposed = false;
  let outputStarted = false;
  let bytes = 0;
  const report = (kind, extra = {}) => {
    try { emit({ kind, phase, elapsedMs: Date.now() - startedAt, phaseElapsedMs: Date.now() - phaseAt, bytes, ...extra }); } catch {}
  };
  const arm = ms => {
    clearTimeout(phaseTimer);
    if (ms > 0) {
      phaseTimer = setTimeout(() => {
        timedOut = phase;
        report('phase_timeout', { timeoutMs: ms });
        delivery?.deadline('attempt_timeout', phase);
        controller.abort(new RetryDeadlineError(phase));
      }, ms);
      phaseTimer.unref?.();
    }
  };
  const enterOutputIdle = () => {
    if (outputStarted) return;
    outputStarted = true;
    report('first_output');
    clearTimeout(budgetTimer);
    phase = 'output_idle';
    phaseAt = Date.now();
    arm(policy.outputIdleMs ?? 300_000);
  };
  const onClientAbort = () => {
    report('client_abort');
    delivery?.cancel();
    controller.abort(abortReason(clientSignal));
  };
  report('attempt_started', { timeouts: policy });
  if (clientSignal?.aborted) onClientAbort();
  else clientSignal?.addEventListener('abort', onClientAbort, { once: true });
  if (Number.isFinite(deadlineAt)) {
    budgetTimer = setTimeout(() => {
      timedOut = 'budget';
      report('phase_timeout', { timeoutMs: deadlineAt - startedAt });
      delivery?.deadline('budget', phase);
      controller.abort(new RetryDeadlineError('budget'));
    }, Math.max(0, deadlineAt - Date.now()));
    budgetTimer.unref?.();
  }
  arm(policy.headersMs ?? 600_000);
  return {
    signal: controller.signal,
    deliveryEnabled: delivery != null,
    requiresContent: requireContent,
    get timedOut() { return timedOut; },
    receivedHeaders(status) {
      delivery?.headers(status);
      report('upstream_headers', { status });
      phase = 'first_output';
      phaseAt = Date.now();
      arm(policy.firstOutputMs ?? 0);
    },
    touch(size) {
      bytes += size;
      if (outputStarted) {
        phaseAt = Date.now();
        arm(policy.outputIdleMs ?? 300_000);
      }
    },
    commit() {
      if (!requireContent) enterOutputIdle();
    },
    forwardContent() {
      delivery?.content();
      if (requireContent) enterOutputIdle();
    },
    clientCanceled() {
      delivery?.cancel();
    },
    complete() { report('response_complete'); },
    failed(error) {
      if (clientSignal?.aborted || timedOut) return;
      delivery?.failed();
      const rawCode = error?.cause?.code ?? error?.code;
      const code = typeof rawCode === 'string' && /^[a-zA-Z0-9_.-]{1,80}$/.test(rawCode) ? rawCode : undefined;
      report('upstream_failure', { code, timeoutPhase: code === 'UND_ERR_CONNECT_TIMEOUT' ? 'connection' : timedOut });
    },
    clearTimers() { clearTimeout(phaseTimer); clearTimeout(budgetTimer); },
    dispose() {
      if (disposed) return;
      disposed = true;
      this.clearTimers();
      clientSignal?.removeEventListener('abort', onClientAbort);
    },
  };
}

function failureFromThrown(error, ctx, clientSignal) {
  if (clientSignal?.aborted) return { clientAbort: true };
  if (ctx.timedOut) {
    return {
      failure: {
        kind: ctx.timedOut === 'budget' ? 'budget' : 'timeout',
        code: ctx.timedOut === 'budget' ? 'retry_budget_exhausted' : 'attempt_timeout',
        normalizedStatus: 504,
        status: null,
      },
      stop: ctx.timedOut === 'budget',
    };
  }
  if (error instanceof RetryDeadlineError) {
    return {
      failure: {
        kind: error.kind,
        code: error.kind === 'budget' ? 'retry_budget_exhausted' : 'attempt_timeout',
        normalizedStatus: 504,
        status: null,
      },
      stop: error.kind === 'budget',
    };
  }
  return {
    failure: {
      kind: 'network',
      code: 'upstream_network_error',
      normalizedStatus: 502,
      status: null,
    },
    stop: false,
  };
}

async function inspectResponse(response, ctx, attempts, account, accountFailover, delivery) {
  const status = response.status;
  const retryAfterMs = parseRetryAfter(response.headers, Date.now());

  if (status === 400 && accountFailover) {
    const read = await readBodyUpTo(response.body, ERROR_BODY_LIMIT, ctx, true);
    const decision = classifyError(errorInfoFromBytes(read.bytes, status), status, accountFailover);
    if (decision.kind === 'resource') {
      read.reader?.cancel().catch(() => {});
      ctx.dispose();
      return { type: 'retry', retryAfterMs, failure: {
        kind: decision.kind, code: decision.code, normalizedStatus: 400, status,
      } };
    }
    return { type: 'return', response: bufferedTailResponse(response, read, ctx, attempts, account) };
  }

  if (status >= 400 && status < 500 && status !== 408 && status !== 429 && status !== 499) {
    ctx.clearTimers();
    if (status === 401 || status === 402 || status === 403) {
      ctx.dispose();
      return {
        type: 'account',
        failure: {
          kind: 'account_disabled',
          code: status === 402 ? 'insufficient_quota' : 'account_auth_failed',
          normalizedStatus: status,
          status,
        },
      };
    }
    return { type: 'return', response: passthroughResponse(response, ctx, attempts, account) };
  }

  if (status === 408 || status === 499 || status === 502 || status === 503 || status === 504 || status === 529) {
    const code = status === 499 ? 'upstream_request_cancelled' : 'upstream_temporarily_unavailable';
    delivery?.upstreamError({ code }, status, 502);
    ctx.dispose();
    return {
      type: 'retry',
      retryAfterMs,
      failure: {
        kind: 'busy',
        code,
        normalizedStatus: 502,
        status,
      },
    };
  }

  if (status === 429 || status === 500) {
    const read = await readBodyUpTo(response.body, ERROR_BODY_LIMIT, ctx, true, ERROR_BODY_WAIT_MS);
    const info = read.timedOut ? { hasError: true, status } : errorInfoFromBytes(read.bytes, status);
    const decision = classifyError(info, status, accountFailover);
    delivery?.upstreamError(info, status, decision.normalizedStatus);
    if (decision.accountDisabled) {
      read.reader?.cancel().catch(() => {});
      ctx.dispose();
      return {
        type: 'account',
        failure: {
          kind: 'account_disabled',
          code: decision.code,
          normalizedStatus: decision.normalizedStatus,
          status,
        },
      };
    }
    if (decision.retryable) {
      read.reader?.cancel().catch(() => {});
      ctx.dispose();
      return {
        type: 'retry',
        retryAfterMs,
        failure: {
          kind: decision.kind,
          code: decision.code,
          normalizedStatus: decision.normalizedStatus,
          status,
        },
      };
    }
    return {
      type: 'return',
      response: bufferedTailResponse(response, read, ctx, attempts, account),
    };
  }

  if (status === 200) {
    const contentType = response.headers.get('content-type') ?? '';
    if (contentType.toLowerCase().includes('text/event-stream')) {
      const inspected = await inspectSse(response, ctx, accountFailover);
      if (inspected.info) delivery?.upstreamError(inspected.info, status, inspected.failure.normalizedStatus);
      if (inspected.type === 'error') {
        ctx.dispose();
        if (inspected.failure.terminal) {
          return {
            type: 'return',
            response: normalizedErrorResponse(
              inspected.failure.normalizedStatus || 502,
              inspected.failure.code || 'upstream_semantic_error',
              attempts,
              account,
            ),
          };
        }
        return {
          type: 'retry',
          retryAfterMs,
          failure: inspected.failure,
        };
      }
      if (inspected.type === 'account') {
        ctx.dispose();
        return inspected;
      }
      const observeChunk = ctx.deliveryEnabled || ctx.requiresContent
        ? createSseContentObserver(ctx)
        : null;
      return {
        type: 'return',
        response: streamingResponse(
          response,
          inspected.reader,
          inspected.prefix,
          ctx,
          attempts,
          account,
          observeChunk,
        ),
      };
    }

    if (isJsonContentType(contentType)) {
      const read = await readBodyUpTo(response.body, JSON_BODY_LIMIT, ctx, true);
      if (read.tooLarge) {
        const observeChunk = ctx.deliveryEnabled || ctx.requiresContent
          ? createFirstContentChunkObserver(ctx)
          : null;
        return {
          type: 'return',
          response: bufferedTailResponse(response, read, ctx, attempts, account, observeChunk),
        };
      }
      const info = errorInfoFromBytes(read.bytes, 200);
      if (!info.hasError) {
        if (hasForwardableBytes(read.bytes)) ctx.forwardContent?.();
        return {
          type: 'return',
          response: bytesResponse(response, read.bytes, ctx, attempts, account),
        };
      }
      read.reader?.cancel().catch(() => {});
      const decision = classifyError(info, 200, accountFailover);
      delivery?.upstreamError(info, status, decision.normalizedStatus);
      if (decision.accountDisabled) {
        ctx.dispose();
        return {
          type: 'account',
          failure: {
            kind: 'account_disabled',
            code: decision.code,
            normalizedStatus: decision.normalizedStatus,
            status: 200,
          },
        };
      }
      if (decision.retryable) {
        ctx.dispose();
        return {
          type: 'retry',
          retryAfterMs,
          failure: {
            kind: decision.kind,
            code: decision.code,
            normalizedStatus: decision.normalizedStatus,
            status: 200,
          },
        };
      }
      ctx.dispose();
      return {
        type: 'return',
        response: normalizedErrorResponse(
          decision.normalizedStatus || 502,
          decision.code || 'upstream_error',
          attempts,
          account,
        ),
      };
    }
  }

  ctx.clearTimers();
  return {
    type: 'return',
    response: passthroughResponse(
      response,
      ctx,
      attempts,
      account,
      status >= 200 && status < 300 && (ctx.deliveryEnabled || ctx.requiresContent)
        ? createFirstContentChunkObserver(ctx)
        : null,
    ),
  };
}

async function inspectSse(response, ctx, accountFailover) {
  const reader = response.body?.getReader();
  if (!reader) {
    return { type: 'return', reader: null, prefix: [] };
  }

  const prefix = [];
  const decoder = new TextDecoder();
  let parseBuffer = '';
  let size = 0;
  const undelivered = { type: 'error', failure: {
    kind: 'empty_stream', code: 'upstream_empty_stream', normalizedStatus: 502, status: 200,
  } };

  while (true) {
    const { done, value } = await readOrCancel(reader, ctx);
    if (done) {
      parseBuffer += decoder.decode();
      const finalEvent = parseEvent(parseBuffer);
      if (finalEvent) {
        const result = inspectSseEvent(finalEvent, accountFailover);
        if (result.type === 'end' && ctx.requiresContent) return undelivered;
        if (result.type === 'error' || result.type === 'account') {
          reader.cancel().catch(() => {});
          return result;
        }
        if (canCommitSse(finalEvent, result, ctx)) return { type: 'return', reader, prefix };
      }
      return ctx.requiresContent ? undelivered : { type: 'return', reader, prefix };
    }

    if (value) {
      prefix.push(value);
      size += value.byteLength;
      if (size > SSE_BUFFER_LIMIT) {
        if (ctx.requiresContent) {
          reader.cancel().catch(() => {});
          return undelivered;
        }
        return { type: 'return', reader, prefix };
      }
      parseBuffer += decoder.decode(value, { stream: true });
    }

    let boundary;
    while ((boundary = findSseBoundary(parseBuffer)) !== -1) {
      const separatorLength = parseBuffer.slice(boundary, boundary + 4).startsWith('\r\n\r\n') ? 4 : 2;
      const rawEvent = parseBuffer.slice(0, boundary);
      parseBuffer = parseBuffer.slice(boundary + separatorLength);
      const event = parseEvent(rawEvent);
      if (!event) continue;
      const result = inspectSseEvent(event, accountFailover);
      if (result.type === 'end' && ctx.requiresContent) {
        reader.cancel().catch(() => {});
        return undelivered;
      }
      if (result.type === 'error' || result.type === 'account') {
        reader.cancel().catch(() => {});
        return result;
      }
      if (canCommitSse(event, result, ctx)) return { type: 'return', reader, prefix };
    }
  }
}

function canCommitSse(event, result, ctx) {
  return (result.type === 'commit' || result.type === 'end') && (!ctx.requiresContent || hasForwardableOutput(event)
    || ['response.completed', 'response.incomplete', 'message_stop'].some(type => type === event.type || type === event.event));
}

function inspectSseEvent(event, accountFailover) {
  const type = typeof event.type === 'string'
    ? event.type
    : (typeof event.event === 'string' ? event.event : '');

  if (type === '[DONE]') return { type: 'end' };

  if (type === 'error' || type === 'response.failed' || type === 'response.incomplete'
    || event.event === 'error' || event.event === 'response.failed') {
    const info = errorInfoFromObject(event, 200);
    if (info.hasError) return classifySseError(info, accountFailover);
  }

  if (type === 'response.completed' || event.event === 'response.completed') {
    const info = errorInfoFromObject(event, 200);
    if (info.hasError) return classifySseError(info, accountFailover);
    return { type: 'commit' };
  }

  if (event.error != null || event.response?.error != null
    || event.status === 'failed' || event.response?.status === 'failed') {
    const info = errorInfoFromObject(event, 200);
    if (info.hasError) return classifySseError(info, accountFailover);
  }

  if (type === 'response.created' || type === 'response.in_progress' || type === 'ping') {
    return { type: 'stash' };
  }

  if (isSsePlaceholder(event, type)) return { type: 'stash' };
  if (type === '' && event.data == null && event.done == null) return { type: 'stash' };
  return { type: 'commit' };
}

function hasForwardableOutput(value) {
  if (typeof value === 'string') return value.length > 0;
  if (Array.isArray(value)) return value.some((part) => hasForwardableOutput(part));
  if (!value || typeof value !== 'object') return false;
  if (['function_call', 'custom_tool_call', 'tool_call', 'tool_use', 'server_tool_use',
    'web_search_call', 'file_search_call', 'code_interpreter_call', 'computer_call', 'mcp_call', 'mcp_list_tools',
    'image_generation_call', 'tool_search_call'].includes(lower(value.type))) {
    return true;
  }
  return hasForwardableOutput(value.text)
    || hasForwardableOutput(value.thinking)
    || hasForwardableOutput(value.reasoning)
    || hasForwardableOutput(value.reasoning_content)
    || hasForwardableOutput(value.arguments)
    || hasForwardableOutput(value.input)
    || hasForwardableOutput(value.content)
    || hasForwardableOutput(value.output)
    || hasForwardableOutput(value.delta)
    || hasForwardableOutput(value.partial_json)
    || hasForwardableOutput(value.message)
    || hasForwardableOutput(value.response)
    || hasForwardableOutput(value.part)
    || hasForwardableOutput(value.item)
    || hasForwardableOutput(value.output_item)
    || hasForwardableOutput(value.content_block)
    || hasForwardableOutput(value.choices)
    || hasForwardableOutput(value.tool_calls)
    || hasForwardableOutput(value.function)
    || hasForwardableOutput(value.function_call)
    || hasForwardableOutput(value.custom_tool_call);
}

// inspectSse may commit on a non-content event. Keep parsing only the chunks
// actually forwarded to the host until the first real output is visible.
function createSseContentObserver(ctx) {
  const decoder = new TextDecoder();
  let parseBuffer = '';
  let bytes = 0;
  let stopped = false;

  const inspectEvent = (event) => {
    if (!event || !hasForwardableOutput(event)) return false;
    stopped = true;
    ctx.forwardContent?.();
    return true;
  };

  const observe = (chunk) => {
    if (stopped || !chunk?.byteLength) return;
    bytes += chunk.byteLength;
    if (bytes > SSE_BUFFER_LIMIT) {
      stopped = true;
      return;
    }
    parseBuffer += decoder.decode(chunk, { stream: true });
    let boundary;
    while ((boundary = findSseBoundary(parseBuffer)) !== -1) {
      const separatorLength = parseBuffer.slice(boundary, boundary + 4).startsWith('\r\n\r\n') ? 4 : 2;
      const rawEvent = parseBuffer.slice(0, boundary);
      parseBuffer = parseBuffer.slice(boundary + separatorLength);
      if (inspectEvent(parseEvent(rawEvent))) return;
    }
  };

  observe.finish = () => {
    if (stopped) return;
    parseBuffer += decoder.decode();
    inspectEvent(parseEvent(parseBuffer));
  };

  return observe;
}

function createFirstContentChunkObserver(ctx) {
  let stopped = false;
  const observe = (chunk) => {
    if (stopped || !hasForwardableBytes(chunk)) return;
    stopped = true;
    ctx.forwardContent?.();
  };
  observe.finish = () => {};
  return observe;
}

function isSsePlaceholder(event, type) {
  // Anthropic sends these before any content. Keep them private until a real
  // delta or tool block commits the attempt, just like Responses placeholders.
  if (type === 'message_start') {
    return Array.isArray(event.message?.content) && event.message.content.length === 0;
  }
  if (type === 'content_block_start') {
    const block = event.content_block;
    return (block?.type === 'text' || block?.type === 'thinking')
      && !hasNonEmptyValue(block.text)
      && !hasNonEmptyValue(block.thinking)
      && !hasNonEmptyValue(block.signature);
  }
  if (type === 'content_block_delta') {
    const delta = event.delta;
    if (delta?.type === 'text_delta') return delta.text === '';
    if (delta?.type === 'thinking_delta') return delta.thinking === '';
  }
  if (type === 'content_block_stop') return true;

  const outputItem = event.item ?? event.response?.output_item;
  if (type === 'response.output_item.added' && outputItem && typeof outputItem === 'object') {
    if (hasForwardableOutput(outputItem)) return false;
    if (outputItem.name != null || outputItem.arguments != null || outputItem.input != null) return false;
    if (hasNonEmptyContent(outputItem.content)) return false;
    if (hasNonEmptyValue(outputItem.text) || hasNonEmptyValue(outputItem.reasoning)) return false;
    return true;
  }

  if (type === 'response.content_part.added') {
    const part = event.part ?? event.content_part ?? event.response?.part;
    if (hasNonEmptyContent(part)) return false;
    if (hasNonEmptyValue(part?.text) || hasNonEmptyValue(part?.reasoning)) return false;
    return true;
  }

  if (type.includes('.delta')) {
    return !hasNonEmptyValue(event.delta)
      && !hasNonEmptyValue(event.text)
      && !hasNonEmptyValue(event.arguments)
      && !hasNonEmptyValue(event.input);
  }

  return false;
}

function hasNonEmptyContent(content) {
  if (content == null) return false;
  if (typeof content === 'string') return content.length > 0;
  if (Array.isArray(content)) return content.some((part) => hasNonEmptyContent(part));
  if (typeof content === 'object') {
    return hasNonEmptyValue(content.text)
      || hasNonEmptyValue(content.reasoning)
      || hasNonEmptyValue(content.arguments)
      || hasNonEmptyValue(content.input);
  }
  return false;
}

function hasNonEmptyValue(value) {
  return typeof value === 'string' && value.length > 0;
}

function hasForwardableBytes(bytes) {
  return bytes?.byteLength > 0 && /[^\s]/.test(new TextDecoder().decode(bytes));
}

function classifySseError(info, accountFailover) {
  const decision = classifyError(info, 200, accountFailover);
  if (decision.accountDisabled) {
    return {
      type: 'account',
      info,
      failure: {
        kind: 'account_disabled',
        code: decision.code,
        normalizedStatus: decision.normalizedStatus,
        status: 200,
      },
    };
  }
  if (decision.retryable) {
    return {
      type: 'error',
      info,
      failure: {
        kind: decision.kind,
        code: decision.code,
        normalizedStatus: decision.normalizedStatus,
        status: 200,
      },
    };
  }
  return {
    type: 'error',
    info,
    failure: {
      kind: 'protocol',
      code: decision.code || 'upstream_semantic_error',
      normalizedStatus: decision.normalizedStatus || 502,
      status: 200,
      terminal: true,
    },
  };
}

function findSseBoundary(text) {
  const crlf = text.indexOf('\r\n\r\n');
  const lf = text.indexOf('\n\n');
  if (crlf === -1) return lf;
  if (lf === -1) return crlf;
  return Math.min(crlf, lf);
}

function parseEvent(rawEvent) {
  const dataLines = [];
  let eventType = '';
  for (const line of rawEvent.split(/\r?\n/)) {
    if (line.startsWith(':')) continue;
    if (line.startsWith('event:')) eventType = line.slice(6).trim();
    if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
  }
  if (!dataLines.length) return null;
  const data = dataLines.join('\n');
  if (data === '[DONE]') return { type: '[DONE]' };
  try {
    const parsed = JSON.parse(data);
    if (parsed && typeof parsed === 'object') return eventType ? { ...parsed, event: eventType } : parsed;
  } catch {}
  return eventType === 'error' || eventType === 'response.failed'
    ? { type: 'error', event: eventType }
    : null;
}

async function readBodyUpTo(body, limit, ctx, stopOnJsonError = false, maxWaitMs = 0) {
  if (!body) return { bytes: new Uint8Array(), reader: null, tooLarge: false };
  const reader = body.getReader();
  const chunks = [];
  const bodyDeadline = maxWaitMs > 0 ? new AbortController() : null;
  const timer = bodyDeadline ? setTimeout(() => bodyDeadline.abort(), maxWaitMs) : null;
  let size = 0;
  try {
    while (true) {
      let result;
      try {
        result = await raceSignal(readOrCancel(reader, ctx), bodyDeadline?.signal);
      } catch (error) {
        if (!bodyDeadline?.signal.aborted || ctx.signal.aborted) throw error;
        reader.cancel().catch(() => {});
        return { bytes: concatBytes(chunks), reader: null, tooLarge: false, timedOut: true };
      }
      const { done, value } = result;
      if (done) {
        return { bytes: concatBytes(chunks), reader: null, tooLarge: false };
      }
      if (!value) continue;
      chunks.push(value);
      size += value.byteLength;
      if (stopOnJsonError) {
        const bytes = concatBytes(chunks);
        try {
          const parsed = JSON.parse(new TextDecoder().decode(bytes));
          if (errorInfoFromObject(parsed, 200).hasError) {
            return { bytes, reader, tooLarge: false };
          }
        } catch {}
      }
      if (size > limit) {
        return { bytes: concatBytes(chunks), reader, tooLarge: true };
      }
    }
  } finally {
    clearTimeout(timer);
  }
}

function errorInfoFromBytes(bytes, status) {
  const text = new TextDecoder().decode(bytes);
  try {
    const parsed = JSON.parse(text);
    const info = errorInfoFromObject(parsed, status);
    if (info.hasError) return info;
    return { ...info, text };
  } catch {
    // Broken JSON is not an error envelope: its output may quote error codes.
    if (status === 200) return { hasError: true, status, code: 'invalid_upstream_json' };
    return { hasError: Boolean(text.trim()), status, message: text.slice(0, 512), text };
  }
}

function errorInfoFromObject(object, fallbackStatus) {
  if (!object || typeof object !== 'object') {
    return { hasError: false, status: fallbackStatus };
  }

  const candidates = [
    object.error,
    object.response?.error,
    object.data?.error,
  ].filter((value) => value != null);
  const objectType = lower(object.type);
  const objectCode = lower(object.code);
  const failed = objectType === 'response.failed' || object.event === 'response.failed'
    || object.status === 'failed' || object.response?.status === 'failed';
  const looksLikeTopLevelError = failed || fallbackStatus >= 400
    || objectType === 'error'
    || object.event === 'error'
    || objectType === 'new_api_error'
    || KNOWN_ERROR_CODES.has(objectCode)
    || numericValue(object.status) >= 400;
  if (looksLikeTopLevelError && !candidates.includes(object)) candidates.push(object);

  if (!candidates.length) {
    return { hasError: false, status: object.status ?? fallbackStatus };
  }

  let type = '';
  let code = '';
  let message = '';
  let status = null;

  for (const candidate of candidates) {
    if (typeof candidate === 'string') {
      message = message || candidate;
      continue;
    }
    if (candidate && typeof candidate === 'object' && candidate !== object) {
      type = type || stringValue(candidate.type);
      code = code || stringValue(candidate.code);
      message = message || stringValue(candidate.message);
      status = status ?? numericValue(candidate.status);
    }
  }

  // Event types such as response.failed describe framing, not the error. Only
  // fall back to the outer object when the structured error omitted metadata.
  type = type || stringValue(object.type);
  code = code || stringValue(object.code) || (failed ? 'upstream_response_failed' : '');
  message = message || stringValue(object.message);
  status = status ?? numericValue(object.status);

  return {
    hasError: true,
    type,
    code,
    message,
    status: status ?? fallbackStatus,
  };
}

function classifyError(info, httpStatus, accountFailover = false) {
  const type = lower(info?.type);
  const code = lower(info?.code);
  const message = lower(info?.message);
  const combined = `${type} ${code} ${message}`;
  const objectStatus = numericValue(info?.status);
  const effectiveStatus = objectStatus >= 400 && objectStatus <= 599 ? objectStatus
    : type === 'authentication_error' ? 401
      : type === 'permission_error' ? 403 : httpStatus;

  if (accountFailover && /the requested item was created under a different azure openai resource/.test(message)) {
    return { accountDisabled: false, retryable: true, kind: 'resource',
      code: 'azure_resource_mismatch', normalizedStatus: 400 };
  }

  if (
    effectiveStatus === 401
    || effectiveStatus === 403
    || effectiveStatus === 402
    || ACCOUNT_ERROR_CODES.has(code)
    || type === 'insufficient_quota'
    || accountFailover && (type === 'authentication_error'
      || ['invalid_api_key', 'authentication_error', 'account_deactivated', 'account_disabled', 'invalid_token', 'expired_token'].includes(code)
      || /invalid api key|authentication failed|api key (?:is )?(?:invalid|expired)/.test(message))
    || /insufficient_quota|quota exhausted|balance exhausted|额度耗尽|余额不足/.test(combined)
  ) {
    return {
      accountDisabled: true,
      retryable: false,
      kind: 'account_disabled',
      code: code || (effectiveStatus === 402 ? 'insufficient_quota' : 'account_auth_failed'),
      normalizedStatus: effectiveStatus === 401 || effectiveStatus === 402 || effectiveStatus === 403
        ? effectiveStatus
        : 402,
    };
  }

  if (
    INVALID_ERROR_CODES.has(code)
    || /context[_ ]length|maximum context|invalid[_ ]request|validation failed/.test(combined)
  ) {
    return {
      accountDisabled: false,
      retryable: false,
      kind: 'invalid_request',
      code: code || 'invalid_request',
      normalizedStatus: effectiveStatus === 422 ? 422 : 400,
    };
  }

  if (
    effectiveStatus === 429
    || RATE_ERROR_CODES.has(code)
    || RATE_ERROR_CODES.has(type)
    || type === 'too_many_requests'
    || /token rate limit|rate_limit_exceeded|too_many_requests/.test(combined)
  ) {
    return {
      accountDisabled: false,
      retryable: true,
      kind: 'rate_limit',
      code: 'rate_limit_exceeded',
      normalizedStatus: 429,
    };
  }

  if (isBusyInfo(info, effectiveStatus)) {
    return {
      accountDisabled: false,
      retryable: true,
      kind: 'busy',
      code: code || 'upstream_temporarily_unavailable',
      normalizedStatus: 502,
    };
  }

  return {
    accountDisabled: false,
    retryable: false,
    kind: 'protocol',
    code: code || type || 'upstream_semantic_error',
    normalizedStatus: effectiveStatus >= 400 ? effectiveStatus : 502,
  };
}

function isBusyInfo(info, httpStatus) {
  const type = lower(info?.type);
  const code = lower(info?.code);
  const message = lower(info?.message);
  const text = lower(info?.text);
  const combined = `${type} ${code} ${message} ${text}`;
  if (httpStatus === 500) return true;
  if (httpStatus === 408 || httpStatus === 499 || httpStatus === 502 || httpStatus === 503 || httpStatus === 504 || httpStatus === 529) {
    return true;
  }
  if (BUSY_ERROR_CODES.has(code)) return true;
  if (type === 'new_api_error' && /当前模型.*负载已经达到上限|模型.*负载.*上限/.test(combined)) {
    return true;
  }
  return containsAny(combined, BUSY_ERROR_CODES)
    || /当前模型.*负载已经达到上限|模型.*负载.*上限/.test(combined);
}

function parseRetryAfter(headers, now) {
  const value = headers.get('retry-after');
  if (!value) return null;
  const trimmed = value.trim();
  if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
    return Math.max(0, Number(trimmed) * 1_000);
  }
  const at = Date.parse(trimmed);
  if (!Number.isFinite(at)) return null;
  return Math.max(0, at - now);
}

export function retryDelay(retryIndex, baseDelayMs, maxDelayMs, jitter, retryAfterMs, mode = 'uniform') {
  const low = Math.min(baseDelayMs, maxDelayMs);
  const high = Math.max(baseDelayMs, maxDelayMs);
  let sampled;
  if (mode === 'exponential') {
    const exponential = Math.min(high, low * (2 ** Math.min(retryIndex, 10)));
    sampled = Math.min(high, exponential + exponential * Math.max(0, jitter) * Math.random());
  } else {
    sampled = Math.min(high, Math.floor(low + Math.random() * (high - low + 1)));
  }
  return Math.max(0, Math.ceil(Math.max(sampled, retryAfterMs ?? 0)));
}

function emitRetry(onRetry, attempt, account, kind, status, delayMs) {
  if (typeof onRetry !== 'function') return;
  try {
    onRetry({ attempt, account, kind, status, delayMs });
  } catch {
    // Diagnostics must not affect retry behavior.
  }
}

async function abortableSleep(delayMs, signal) {
  if (signal?.aborted) throw abortReason(signal);
  if (delayMs <= 0) return;
  await new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => signal?.removeEventListener('abort', onAbort);
    const onAbort = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      cleanup();
      reject(abortReason(signal));
    };
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve();
    }, delayMs);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

function raceAbort(promise, ctx) {
  if (ctx.signal.aborted) return Promise.reject(ctx.signal.reason ?? new DOMException('Aborted', 'AbortError'));
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(ctx.signal.reason ?? new DOMException('Aborted', 'AbortError'));
    ctx.signal.addEventListener('abort', onAbort, { once: true });
    Promise.resolve(promise).then(
      (value) => {
        ctx.signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error) => {
        ctx.signal.removeEventListener('abort', onAbort);
        reject(error);
      },
    );
  });
}

async function readOrCancel(reader, ctx) {
  try {
    const result = await raceAbort(reader.read(), ctx);
    if (result.value?.byteLength) ctx.touch?.(result.value.byteLength);
    return result;
  } catch (error) {
    reader.cancel().catch(() => {});
    throw error;
  }
}

function raceSignal(promise, signal) {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(abortReason(signal));
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(abortReason(signal));
    signal.addEventListener('abort', onAbort, { once: true });
    Promise.resolve(promise).then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      },
    );
  });
}

function abortReason(signal) {
  if (signal?.reason != null) return signal.reason;
  return new DOMException('The operation was aborted', 'AbortError');
}

function passthroughResponse(response, ctx, attempts, account, observeChunk = null) {
  if (!ctx.requiresContent) ctx.clearTimers();
  const reader = response.body?.getReader();
  return streamingResponse(response, reader, [], ctx, attempts, account, observeChunk);
}

function bufferedTailResponse(response, read, ctx, attempts, account, observeChunk = null) {
  if (!ctx.requiresContent) ctx.clearTimers();
  if (!read.tooLarge) {
    read.reader?.cancel().catch(() => {});
    return bytesResponse(response, read.bytes, ctx, attempts, account);
  }
  return streamingResponse(
    response,
    read.reader,
    [read.bytes],
    ctx,
    attempts,
    account,
    observeChunk,
  );
}

function bytesResponse(response, bytes, ctx, attempts, account) {
  ctx.commit?.();
  ctx.complete?.();
  ctx.dispose();
  return new Response(bytes.byteLength ? bytes : null, {
    status: response.status,
    statusText: response.statusText,
    headers: responseHeaders(response.headers, attempts, account),
  });
}

function streamingResponse(response, reader, prefix, ctx, attempts, account, observeChunk = null) {
  if (ctx.commit) ctx.commit();
  else ctx.clearTimers();
  if (!reader) {
    for (const chunk of prefix) observeChunk?.(chunk);
    observeChunk?.finish?.();
    ctx.dispose();
    return new Response(prefix.length ? concatBytes(prefix) : null, {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders(response.headers, attempts, account),
    });
  }
  const body = prefixedReaderStream(reader, prefix, ctx, observeChunk);
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: responseHeaders(response.headers, attempts, account),
  });
}

function prefixedReaderStream(reader, prefix, ctx, observeChunk) {
  let finished = false;
  const finish = () => {
    if (finished) return;
    finished = true;
    ctx.dispose();
  };

  return new ReadableStream({
    start(controller) {
      for (const chunk of prefix) {
        if (chunk?.byteLength) {
          observeChunk?.(chunk);
          controller.enqueue(chunk);
        }
      }
      const onAbort = () => {
        if (finished) return;
        try {
          controller.error(abortReason(ctx.signal));
        } catch {
          // Stream may already be closed.
        }
        reader.cancel().catch(() => {});
        finish();
      };
      if (ctx.signal.aborted) onAbort();
      else ctx.signal.addEventListener('abort', onAbort, { once: true });
    },
    async pull(controller) {
      try {
        const { done, value } = await readOrCancel(reader, ctx);
        if (value?.byteLength) {
          observeChunk?.(value);
          controller.enqueue(value);
        }
        if (done) {
          observeChunk?.finish?.();
          ctx.complete?.();
          controller.close();
          finish();
        }
      } catch (error) {
        ctx.failed?.(error);
        controller.error(error);
        finish();
      }
    },
    cancel(reason) {
      if (!finished) ctx.clientCanceled?.();
      reader.cancel(reason).catch(() => {});
      finish();
    },
  });
}

function normalizedErrorResponse(status, code, attempts, account, retryAfterAt = null) {
  const safeStatus = status >= 400 && status <= 599 ? status : 502;
  const message = statusMessage(safeStatus);
  const headers = new Headers({ 'content-type': 'application/json; charset=utf-8' });
  headers.set('x-anyrouter-attempts', String(attempts));
  headers.set('x-anyrouter-account', account);
  if (Number.isFinite(retryAfterAt)) {
    headers.set('retry-after', String(Math.max(0, Math.ceil((retryAfterAt - Date.now()) / 1_000))));
  }
  return new Response(JSON.stringify({ error: { type: code, message } }), {
    status: safeStatus,
    headers,
  });
}

function failureResponse(failure, attempts, account, fallbackKind) {
  if (!failure) {
    const status = fallbackKind === 'budget' ? 504 : 503;
    return normalizedErrorResponse(
      status,
      fallbackKind === 'budget' ? 'retry_budget_exhausted' : 'no_available_account',
      attempts,
      account,
    );
  }
  return normalizedErrorResponse(
    failure.normalizedStatus || (failure.kind === 'budget' ? 504 : 502),
    failure.code || failure.kind || fallbackKind || 'upstream_error',
    attempts,
    account,
    failure.retryAfterAt,
  );
}

function statusMessage(status) {
  if (status === 429) return 'AnyRouter rate limit remained after retries.';
  if (status === 504) return 'Retry budget or attempt timeout was exhausted.';
  if (status === 401 || status === 403) return 'No enabled account could complete the request.';
  if (status === 402) return 'Account quota is exhausted.';
  if (status === 400 || status === 422) return 'The upstream rejected the request.';
  return 'AnyRouter did not recover before the retry policy ended.';
}

function responseHeaders(source, attempts, account) {
  const headers = new Headers(source);
  headers.delete('content-length');
  headers.delete('content-encoding');
  headers.delete('transfer-encoding');
  headers.set('x-anyrouter-attempts', String(attempts));
  headers.set('x-anyrouter-account', account);
  return headers;
}

function isJsonContentType(contentType) {
  const value = lower(contentType);
  return value.includes('application/json') || value.includes('+json');
}

function safeAccountLabel(account) {
  const raw = typeof account === 'string'
    ? account
    : (account?.label ?? account?.name ?? account?.id);
  if (typeof raw !== 'string') return 'unknown';
  const value = raw.trim();
  if (!value || value.length > 64 || !/^[A-Za-z0-9._:-]+$/.test(value)) return 'unknown';
  return value;
}

function accountKey(account, label) {
  if (typeof account === 'string') return account;
  if (account != null && typeof account === 'object') return label;
  return label;
}

function concatBytes(chunks) {
  const valid = chunks.filter((chunk) => chunk?.byteLength);
  if (valid.length === 0) return new Uint8Array();
  if (valid.length === 1) return valid[0];
  const size = valid.reduce((total, chunk) => total + chunk.byteLength, 0);
  const out = new Uint8Array(size);
  let offset = 0;
  for (const chunk of valid) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function numericValue(value) {
  if (Number.isFinite(value)) return Number(value);
  if (typeof value === 'string' && /^\d{3}$/.test(value)) return Number(value);
  return null;
}

function stringValue(value) {
  return typeof value === 'string' ? value : '';
}

function lower(value) {
  return typeof value === 'string' ? value.toLowerCase() : '';
}

function containsAny(text, values) {
  for (const value of values) {
    if (text.includes(value)) return true;
  }
  return false;
}

function cancelBody(response) {
  response?.body?.cancel().catch(() => {});
}
