// JSON objects may arrive with different key ordering after gateway conversion.
// Only remove equal definitions. Changed schemas and historical declarations
// at later input positions remain distinct.
function equal(a, b) {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object") return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length &&
    keys.every(key => Object.prototype.hasOwnProperty.call(b, key) && equal(a[key], b[key]));
}

export function deduplicateToolDefinitions(body) {
  const index = Array.isArray(body?.input)
    ? body.input.findIndex(item => item?.type === "additional_tools") : -1;
  const item = index >= 0 ? body.input[index] : null;
  // Leave malformed declarations to the normal validation path.
  if (item && !Array.isArray(item.tools)) return { body, removed: 0 };
  const top = Array.isArray(body?.tools) ? body.tools : [];
  const original = item ? item.tools : top;
  const merged = [], buckets = new Map();
  let removed = 0;
  function add(tool) {
    const key = JSON.stringify([tool?.type, tool?.name, tool?.function?.name]);
    const bucket = buckets.get(key) || [];
    if (bucket.some(old => equal(old, tool))) { removed++; return; }
    bucket.push(tool);
    buckets.set(key, bucket);
    merged.push(tool);
  }
  original.forEach(add);
  if (item) top.forEach(add);
  if (!removed && !(item && top.length)) return { body, removed };
  if (!item) return { body: { ...body, tools: merged }, removed };
  const input = body.input.slice();
  input[index] = { ...item, tools: merged };
  const result = { ...body, input };
  // These definitions are now in the first Lite item, as codexLiteShape would
  // place them. Do not serialize another copy in the preserved request.
  delete result.tools;
  return { body: result, removed };
}
