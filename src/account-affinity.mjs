import { createHash, randomUUID } from 'node:crypto';
import { readFile, writeFile, mkdir, rename, unlink, rmdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { hostname } from 'node:os';

const present = value => typeof value === 'string' && value.length > 0 ? value : undefined;
function identity({ sessionId, body = {}, headers, requestId }) {
  const metadata = body.client_metadata || {};
  let turn;
  try { turn = JSON.parse(metadata['x-codex-turn-metadata']); } catch {}
  const h = new Headers(headers);
  return present(sessionId) || present(metadata.session_id) || present(turn?.session_id) ||
    present(body.prompt_cache_key) || present(h.get('session_id')) || present(h.get('x-codex-session-id')) ||
    present(h.get('thread_id')) || present(requestId) || randomUUID();
}
function empty() { return { version: 1, next: 0, bindings: {} }; }
async function load(file) {
  let data;
  try { data = JSON.parse(await readFile(file, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return empty(); throw new Error('affinity state unreadable'); }
  if (data?.version !== 1 || !Number.isSafeInteger(data.next) || data.next < 0 ||
      !data.bindings || typeof data.bindings !== 'object' || Array.isArray(data.bindings) ||
      Object.entries(data.bindings).some(([key,value]) => !/^[a-f0-9]{64}$/.test(key) || typeof value !== 'string' || !value)) {
    throw new Error('affinity state invalid');
  }
  return data;
}
async function lock(file) {
  const path = file + '.lock';
  const end = Date.now() + 2000;
  while (true) {
    try {
      await mkdir(path, { mode: 0o700 });
      await writeFile(join(path, 'owner.json'), JSON.stringify({ pid: process.pid, host: hostname() }), { mode: 0o600 });
      return async () => { await unlink(join(path, 'owner.json')); await rmdir(path); };
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      // Serialize recovery too: two reclaimers must not delete a new owner's
      // lock after the first reclaimer removed the abandoned directory.
      const recovery = path + '.recovery';
      let recovering = false;
      try {
        await mkdir(recovery, { mode: 0o700 }); recovering = true;
        const owner = JSON.parse(await readFile(join(path, 'owner.json'), 'utf8'));
        if (owner.host === hostname() && Number.isInteger(owner.pid) && owner.pid > 0) {
          try { process.kill(owner.pid, 0); }
          catch (e) {
            if (e.code === 'ESRCH') {
              await unlink(join(path, 'owner.json')); await rmdir(path);
            }
          }
        }
      } catch {}
      finally { if (recovering) await rmdir(recovery).catch(() => {}); }
      if (Date.now() >= end) throw new Error('affinity lock timeout');
      await new Promise(resolve => setTimeout(resolve, 20));
    }
  }
}

export function createAccountAffinity({ file, providerID }) {
  const memory = empty();
  const keyOf = args => createHash('sha256').update(providerID + '\0' + identity(args)).digest('hex');
  async function transaction(change) {
      if (!file) return change(memory).result;
      await mkdir(dirname(file), { recursive: true, mode: 0o700 });
      const release = await lock(file);
      let temporary;
      try {
        const state = await load(file);
        const changed = change(state);
        if (changed.changed) {
          temporary = file + '.' + randomUUID() + '.tmp';
          await writeFile(temporary, JSON.stringify(state) + '\n', { mode: 0o600, flag: 'wx' });
          await rename(temporary, file); temporary = undefined;
        }
        return changed.result;
      } finally {
        if (temporary) await unlink(temporary).catch(() => {});
        await release();
      }
  }
  const api = {
    async resolve(args) {
      const { sources, hasHistory = true } = args;
      const key = keyOf(args);
      return transaction(state => {
        let account = state.bindings[key];
        let changed = false;
        if (!account && sources.length) {
          account = sources[hasHistory ? 0 : state.next % sources.length].id;
          state.bindings[key] = account;
          if (!hasHistory) state.next = (state.next + 1) % Number.MAX_SAFE_INTEGER;
          changed = true;
        }
        return { changed, result: { account: account || null, source: sources.find(s => s.id === account) || null } };
      });
    },
    async select(args) { return (await api.resolve(args)).source; },
    async rebind(args, { expectedAccount, account }) {
      if (!args.sources.some(source => source.id === account)) throw new Error('affinity target outside account pool');
      const key = keyOf(args);
      return transaction(state => {
        const previous = state.bindings[key];
        const changed = previous === expectedAccount && previous !== account;
        if (changed) state.bindings[key] = account;
        return { changed, result: { changed, account: state.bindings[key] || null } };
      });
    },
  };
  return api;
}
