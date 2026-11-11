// AnyRouter accepts tool-search declarations and emits native search calls,
// but its input validator rejects native client-search history items.
const complete = item => item && item.execution === 'client' &&
  typeof item.call_id === 'string' && item.call_id.length > 0 &&
  (item.status === undefined || item.status === 'completed');
const identity = tool => JSON.stringify([tool?.type ?? '', tool?.namespace ?? '', tool?.name ?? '']);

function activated(tool) {
  if (tool?.type === 'namespace' && Array.isArray(tool.tools)) {
    const tools = tool.tools.map(activated);
    return tools.some((entry, index) => entry !== tool.tools[index]) ? { ...tool, tools } : tool;
  }
  return tool?.defer_loading === true ? { ...tool, defer_loading: false } : tool;
}

function mergeTool(current, found, loaded = false) {
  if (current?.type !== 'namespace' || found?.type !== 'namespace' ||
      !Array.isArray(current.tools) || !Array.isArray(found.tools)) {
    return loaded && current?.defer_loading === true ? { ...current, defer_loading: false } : current;
  }
  let result = current.tools;
  for (const child of found.tools) {
    const index = result.findIndex(existing => identity(existing) === identity(child));
    if (index < 0) result = [...result, loaded ? activated(child) : child];
    else {
      const merged = mergeTool(result[index], child, loaded);
      if (merged !== result[index]) result = result.map((entry, n) => n === index ? merged : entry);
    }
  }
  return result === current.tools ? current : { ...current, tools: result };
}

export function normalizeToolSearchHistory(body) {
  if (!body || !Array.isArray(body.input)) return body;
  const calls = new Map(), outputs = new Map();
  for (const item of body.input) {
    if (!item || typeof item.call_id !== 'string' || !item.call_id) continue;
    const table = item.type === 'tool_search_call' ? calls : item.type === 'tool_search_output' ? outputs : null;
    if (table) table.set(item.call_id, [...(table.get(item.call_id) || []), item]);
  }
  const paired = new Set([...calls.keys()].filter(id => {
    const list = calls.get(id), results = outputs.get(id);
    if (list.length !== 1 || results?.length !== 1) return false;
    const call = list[0], result = results[0];
    return complete(call) && complete(result) && call.arguments &&
      typeof call.arguments === 'object' && !Array.isArray(call.arguments) && Array.isArray(result.tools);
  }));
  if (!paired.size) return body;

  // Current explicit declarations take priority over historical leaf schemas.
  // Updating all existing occurrences keeps the bridge's own deduplication valid.
  const declarations = new Map(), firstFound = new Map();
  function collect(tools, discoveryIndex) {
    for (const tool of Array.isArray(tools) ? tools : []) {
      if (!tool || typeof tool !== 'object') continue;
      const key = identity(tool), previous = declarations.get(key);
      if (previous) declarations.set(key, mergeTool(previous, tool, discoveryIndex !== undefined));
      else {
        declarations.set(key, discoveryIndex !== undefined ? activated(tool) : tool);
        if (discoveryIndex !== undefined) firstFound.set(key, discoveryIndex);
      }
    }
  }
  collect(body.tools);
  for (const item of body.input) if (item?.type === 'additional_tools') collect(item.tools);
  body.input.forEach((item, index) => {
    if (item?.type === 'tool_search_output' && paired.has(item.call_id) && outputs.get(item.call_id)[0] === item) collect(item.tools, index);
  });
  function updated(tools) {
    if (!Array.isArray(tools)) return tools;
    const result = tools.map(tool => declarations.get(identity(tool)) || tool);
    return result.some((entry, index) => entry !== tools[index]) ? result : tools;
  }
  const input = [];
  body.input.forEach((item, index) => {
    if (item?.type === 'tool_search_call' && paired.has(item.call_id) && calls.get(item.call_id)[0] === item) {
      const call = { type: 'function_call', name: 'tool_search', call_id: item.call_id, arguments: JSON.stringify(item.arguments) };
      if (item.status !== undefined) call.status = item.status;
      input.push(call);
    } else if (item?.type === 'tool_search_output' && paired.has(item.call_id) && outputs.get(item.call_id)[0] === item) {
      input.push({ type: 'function_call_output', call_id: item.call_id, output: JSON.stringify(item) });
    } else if (item?.type === 'additional_tools') {
      const tools = updated(item.tools);
      input.push(tools === item.tools ? item : { ...item, tools });
    } else input.push(item);
    const discovered = [...firstFound].filter(([, first]) => first === index).map(([key]) => declarations.get(key));
    if (discovered.length) input.push({ type: 'additional_tools', role: 'developer', tools: discovered });
  });
  const result = { ...body, input };
  if (Array.isArray(body.tools)) result.tools = updated(body.tools);
  return result;
}
