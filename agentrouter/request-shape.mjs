import { createHash } from "node:crypto";

const digest = value => createHash("sha256").update(value).digest("hex");
const bytes = value => Buffer.byteLength(JSON.stringify(value));
const kinds = new Set(["message", "additional_tools", "reasoning", "compaction",
  "compaction_summary", "function_call", "function_call_output", "custom_tool_call",
  "custom_tool_call_output", "tool_search_call", "tool_search_output", "item_reference", "agent_message"]);

// Sorting object keys makes fingerprints independent of transport serialization.
// Arrays and tool call IDs retain their order and meaning.
function canonical(value) {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value && typeof value === "object") return "{" + Object.keys(value).sort()
    .map(key => JSON.stringify(key) + ":" + canonical(value[key])).join(",") + "}";
  return JSON.stringify(value);
}

export function requestAuditIdentity(body, headers, marker) {
  const candidates = [marker?.sessionId, headers?.get("session_id"), headers?.get("x-magpie-session"),
    body?.client_metadata?.session_id, body?.prompt_cache_key];
  return candidates.filter(x => typeof x === "string" && x.length > 0);
}

export function describeRequest(body, serializedBody, detailed = false) {
  const totalBytes = Buffer.byteLength(serializedBody);
  const sectionBytes = {};
  let valuesBytes = 0;
  // These are the UTF-8 JSON value sizes, excluding each key and delimiter.
  for (const [key, value] of Object.entries(body)) {
    const size = bytes(value);
    valuesBytes += size;
    const section = ["input", "messages", "tools", "instructions", "system"].includes(key) ? key : "otherValues";
    sectionBytes[section] = (sectionBytes[section] || 0) + size;
  }
  sectionBytes.jsonKeysAndDelimiters = totalBytes - valuesBytes;
  const input = Array.isArray(body.input) ? body.input : Array.isArray(body.messages) ? body.messages : [];
  const itemGroups = {}, fingerprints = [], repeatedItems = [], seen = new Map(), calls = new Map();
  let itemBytes = 0;
  for (let index = 0; index < input.length; index++) {
    const item = input[index];
    const type = kinds.has(item?.type) ? item.type : item?.type === undefined && item?.role ? "message" : "other";
    const role = ["user", "assistant", "developer", "system"].includes(item?.role) ? item.role : undefined;
    const kind = type === "message" && role ? type + "/" + role : type;
    const size = bytes(item);
    itemBytes += size;
    const group = itemGroups[kind] ||= { count: 0, bytes: 0 };
    group.count++;
    group.bytes += size;
    if (!detailed) continue;
    const hash = digest(canonical(item));
    if (seen.has(hash)) repeatedItems.push({ index, first: seen.get(hash), kind, bytes: size });
    else seen.set(hash, index);
    const fingerprint = { kind, bytes: size, hash };
    if (typeof item?.call_id === "string") {
      fingerprint.callRef = digest(item.call_id).slice(0, 24);
      const key = kind + ":" + fingerprint.callRef;
      const indexes = calls.get(key) || [];
      indexes.push(index);
      calls.set(key, indexes);
    }
    if (item?.output !== undefined) fingerprint.payloadHash = digest(canonical(item.output));
    fingerprints.push(fingerprint);
  }
  const result = { sectionBytes, inputItems: input.length, itemGroups,
    inputArrayDelimiters: input.length ? input.length + 1 : 2, inputItemBytes: itemBytes };
  if (detailed) {
    result.itemFingerprints = fingerprints;
    result.repeatedItems = repeatedItems;
    result.repeatedCallIds = [...calls.entries()].filter(([, indexes]) => indexes.length > 1)
      .map(([kindAndRef, indexes]) => ({ kindAndRef, indexes }));
  }
  return result;
}
