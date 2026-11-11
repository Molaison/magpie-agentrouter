// A complete item carries its own contents. Its server-minted id is not the
// tool correlation id and must not be resolved against a different Azure node.
// Opaque reasoning/compaction contents and reference-only items are preserved.
function complete(item) {
  if (!item || typeof item !== 'object') return false;
  // The public EasyInputMessage contract permits omitting type.
  if (item.type === undefined && ['user', 'assistant', 'developer', 'system'].includes(item.role)) {
    return typeof item.content === 'string' || Array.isArray(item.content);
  }
  switch (item.type) {
    case 'message':
      return typeof item.role === 'string' && (typeof item.content === 'string' || Array.isArray(item.content));
    case 'agent_message':
      return typeof item.author === 'string' && typeof item.recipient === 'string' && Array.isArray(item.content);
    case 'tool_search_call':
      return typeof item.execution === 'string' && item.arguments !== undefined;
    case 'tool_search_output':
      return typeof item.execution === 'string' && typeof item.status === 'string' && Array.isArray(item.tools);
    case 'function_call':
      return typeof item.call_id === 'string' && typeof item.name === 'string' && typeof item.arguments === 'string';
    case 'custom_tool_call':
      return typeof item.call_id === 'string' && typeof item.name === 'string' && typeof item.input === 'string';
    case 'function_call_output':
      // Native peer/delegation messages can carry a named tool output without
      // a preceding function call. Preserve that output without inventing a call_id.
      return (typeof item.call_id === 'string' || typeof item.name === 'string') && item.output !== undefined;
    case 'custom_tool_call_output':
      return typeof item.call_id === 'string' && item.output !== undefined;
    case 'reasoning':
      return typeof item.encrypted_content === 'string' && item.encrypted_content.length > 0;
    case 'compaction':
    case 'compaction_summary':
    case 'context_compaction':
      return typeof item.encrypted_content === 'string' && item.encrypted_content.length > 0;
    default:
      return false;
  }
}

export function portableInput(body) {
  if (!Array.isArray(body.input)) return { body, removed: 0 };
  let removed = 0;
  const input = body.input.map(item => {
    if (!complete(item) || typeof item.id !== 'string') return item;
    const copy = { ...item };
    delete copy.id;
    removed++;
    return copy;
  });
  return { body: removed ? { ...body, input } : body, removed };
}

export function hasResponseHistory(body) {
  if (body.previous_response_id || body.conversation) return true;
  return Array.isArray(body.input) && body.input.some(item => item && (
    item.role === 'assistant' || ['reasoning', 'compaction', 'item_reference',
      'function_call', 'custom_tool_call', 'function_call_output', 'custom_tool_call_output'].includes(item.type)
  ));
}

// Retaining an unresolved reference is essential for the original account,
// but it must not be mistaken for self-contained history when migrating.
export function migrationReady(body) {
  if (body.previous_response_id || body.conversation) return false;
  function referenced(value) {
    if (!value || typeof value !== 'object') return false;
    if (Array.isArray(value)) return value.some(referenced);
    if (value.type === 'item_reference' || typeof value.file_id === 'string') return true;
    if (typeof value.id === 'string' && value.type !== 'additional_tools' && !complete(value)) return true;
    return Object.entries(value).some(([key, item]) =>
      !['tools', 'parameters', 'arguments', 'input', 'output', 'encrypted_content'].includes(key) && referenced(item));
  }
  return !referenced(body.input);
}
