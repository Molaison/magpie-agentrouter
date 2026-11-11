// Standalone hot-loadable Magpie middleware (no Node/Bun dependency).
//
// Purpose: make the generic Lite-tool rewrite deterministic. The original
// provider generates `additional_tools.id = "at_" + randomUUID()` per request
// when the client sends top-level `tools` with no `additional_tools` item, so
// two otherwise identical requests differ on one wire field. This middleware
// pre-seeds the fixed-content `additional_tools` item inside the marker request
// captured by the provider's own preserve middleware, using a stable id
// derived from the cache key (a short string). The original provider then merges
// dedupes and deletes top-level tools exactly as before, but keeps our id.
//
// Scope: provider `agentrouter-retry`, `responses` protocol only. It only
// rewrites the private marker's captured request, never the outer body. When
// `additional_tools` already exists, the key is unavailable, the marker or its
// `request` is absent, or another provider/protocol is in play, the input is
// returned untouched.
//
// The SHA-256/canonical valueHash below is inlined byte-for-byte from
// `../hotload-test/light-diagnostic.middleware.js`: a hot-added standalone
// middleware may not import modules outside its own plugin directory (Magpie
// 0.1.1582 rejects such imports at load time).

const FIELD = "__magpie_anyrouter_retry";

const K = [0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2];
function utf8(s) {
  const encoded = encodeURIComponent(s), out = [];
  for (let i=0;i<encoded.length;i++) {
    if (encoded[i] === '%') { out.push(parseInt(encoded.slice(i+1,i+3),16)); i+=2; }
    else out.push(encoded.charCodeAt(i));
  }
  return out;
}
export function sha256(bytes) {
  const data=bytes.slice(), bitlen=data.length*8;
  data.push(128); while (data.length%64!==56) data.push(0);
  for(let i=7;i>=0;i--) data.push(Math.floor(bitlen/Math.pow(256,i))&255);
  let h=[0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19];
  const rotr=(x,n)=>(x>>>n)|(x<<(32-n));
  for(let off=0;off<data.length;off+=64) {
    const w=[];
    for(let i=0;i<16;i++) w[i]=((data[off+4*i]<<24)|(data[off+4*i+1]<<16)|(data[off+4*i+2]<<8)|data[off+4*i+3]);
    for(let i=16;i<64;i++) {
      const x=w[i-15],y=w[i-2];
      w[i]=(w[i-16]+(rotr(x,7)^rotr(x,18)^(x>>>3))+w[i-7]+(rotr(y,17)^rotr(y,19)^(y>>>10)))|0;
    }
    let [a,b,c,d,e,f,g,hh]=h;
    for(let i=0;i<64;i++) {
      const t1=(hh+(rotr(e,6)^rotr(e,11)^rotr(e,25))+((e&f)^(~e&g))+K[i]+w[i])|0;
      const t2=((rotr(a,2)^rotr(a,13)^rotr(a,22))+((a&b)^(a&c)^(b&c)))|0;
      hh=g;g=f;f=e;e=(d+t1)|0;d=c;c=b;b=a;a=(t1+t2)|0;
    }
    h=h.map((x,i)=>(x+[a,b,c,d,e,f,g,hh][i])|0);
  }
  return h.map(x=>('00000000'+(x>>>0).toString(16)).slice(-8)).join('');
}
export function canonical(value) {
  if (Array.isArray(value)) return '['+value.map(canonical).join(',')+']';
  if (value && typeof value==='object') return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+canonical(value[k])).join(',')+'}';
  return JSON.stringify(value === undefined ? null : value);
}
export function valueHash(value) { return sha256(utf8(canonical(value))); }

function nonEmptyString(value) {
  return typeof value === "string" && value.length > 0 ? value : null;
}

export function onProvider(body, ctx) {
  if (ctx?.provider?.id !== "agentrouter-retry" || ctx?.protocol !== "responses") return;
  if (!body || typeof body !== "object" || Array.isArray(body)) return;

  const marker = body[FIELD];
  if (!marker || typeof marker !== "object" || Array.isArray(marker)) return;
  const base = marker.request;
  if (!base || typeof base !== "object" || Array.isArray(base)) return;

  const tools = Array.isArray(base.tools) ? base.tools : null;
  if (!tools || tools.length === 0) return;
  if (!Array.isArray(base.input)) return;
  if (base.input.some((item) => item?.type === "additional_tools")) return;

  const clientMetadata = base.client_metadata && typeof base.client_metadata === "object"
    ? base.client_metadata
    : null;
  const key = nonEmptyString(base.prompt_cache_key)
    || nonEmptyString(marker.sessionId)
    || nonEmptyString(clientMetadata?.session_id);
  if (!key) return;

  // Hash only the short key string: hashing the full tool set would exceed
  // Goja's execution budget on real long tool definitions.
  const id = "at_" + valueHash(key).slice(0, 32);
  const item = { type: "additional_tools", id, role: "developer", tools };

  return {
    ...body,
    [FIELD]: { ...marker, request: { ...base, input: [item, ...base.input] } },
  };
}
