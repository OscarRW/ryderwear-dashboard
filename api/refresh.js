// Cron-triggered refresh: pulls every order from Shopify via the Bulk
// Operations API, aggregates into the same `gymData` shape the dashboard
// renders, and writes the result to Vercel KV. The dashboard reads from
// /api/data so page load drops to <1s once KV is populated.
//
// Two-phase design (so each invocation fits in Vercel Hobby's 60s cap):
//   Phase A — adopt: if a bulk op (started by us or anyone) is already
//             RUNNING, just store its id and return.
//   Phase B — pickup: if our stored bulk id has reached COMPLETED, download
//             the JSONL, parse, aggregate, write KV, clear state. If still
//             RUNNING, return. If FAILED, clear state so next tick retries.
//   Phase C — start: no pending or active op, kick a new bulk job off and
//             record its id. The next cron tick will pick it up.
//
// Auth: client_id + client_secret → 24h Admin API token via the
// client_credentials grant. Token cached in KV with a 5-min safety buffer.
//
// Env vars required:
//   SHOPIFY_STORE          e.g. "ryderwear-au" or "ryderwear-au.myshopify.com"
//   SHOPIFY_CLIENT_ID
//   SHOPIFY_CLIENT_SECRET
//   KV_REST_API_URL        (auto-set when Vercel KV is provisioned)
//   KV_REST_API_TOKEN      (auto-set when Vercel KV is provisioned)
//   CRON_SECRET            any random string — Vercel sends it on cron hits

const HISTORY_MONTHS = 6;
const KV_GYM_DATA = "gymData";
const KV_TOKEN    = "shopifyToken";
const KV_PENDING  = "shopifyBulkPending";
const STALE_RUN_MS = 2 * 60 * 60 * 1000; // bulk shouldn't run >2h; if it does, reset
const PERIODS = [7, 14, 30, 60, 90];
const SHOPIFY_API_VERSION = "2024-10";

// Apparel productTypes count toward the dashboard's "ex-gym" total.
// Everything else — supplements, drinks, food, memberships, gift cards,
// shipping insurance, lanyards, FOB, empty productType — is treated as
// "gym/excluded" revenue and netted out. Case-insensitive match on the
// trimmed productType.
const APPAREL_PRODUCT_TYPES = new Set(["clothing", "shoes", "socks", "accessories"]);
const isGP = li => {
  const pt = ((li.productType || "")).trim().toLowerCase();
  return !APPAREL_PRODUCT_TYPES.has(pt);
};

// Top-products category buckets shown on the dashboard. Anything whose
// productType doesn't match one of these is excluded from the top lists.
const TOP_CATEGORIES = ["clothing", "accessories", "supplements", "drinks", "food"];
const topCat = li => {
  const pt = ((li.productType || "")).trim().toLowerCase();
  return TOP_CATEGORIES.includes(pt) ? pt : null;
};

// Map: Shopify location name (matched by substring, case-insensitive) →
// the dashboard label that should appear. Online orders or in-store orders
// at locations not in this map are skipped.
const GYM_LOCATION_MAP = [
  ["flinders park", "Flinders Park, SA"],
  ["tranmere",      "Tranmere, SA"],
  ["midvale",       "Midvale, WA"],
  ["malaga",        "Malaga, WA"],
  ["alberton",      "Alberton, SA"],
  ["hackham",       "Hackham, SA"],
  ["wanneroo",      "Wanneroo, WA"],
  ["maddington",    "Maddington, WA"],
  ["munno para",    "Munno Para, SA"],
  ["rockingham",    "Rockingham, WA"],
  ["mandurah",      "Mandurah, WA"],
];
function mapLocation(name){
  const lower = (name || "").toLowerCase();
  for (const [needle, label] of GYM_LOCATION_MAP){
    if (lower.includes(needle)) return label;
  }
  return null;
}

function shopifyShop(){
  const s = process.env.SHOPIFY_STORE || "";
  return s.includes(".") ? s : `${s}.myshopify.com`;
}

// ── Vercel KV (REST, no SDK) ──────────────────────────────────────────────
async function kvGet(key){
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return null;
  const r = await fetch(`${url}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!r.ok) return null;
  const { result } = await r.json();
  if (!result) return null;
  return typeof result === "string" ? JSON.parse(result) : result;
}
async function kvSet(key, value){
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) throw new Error("KV not configured");
  const r = await fetch(`${url}/set/${encodeURIComponent(key)}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify(value)
  });
  if (!r.ok) throw new Error(`KV set ${r.status}: ${await r.text().catch(()=>"")}`);
}
async function kvDel(key){
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return;
  await fetch(`${url}/del/${encodeURIComponent(key)}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` }
  }).catch(()=>{});
}

// ── Shopify access token (24h via client_credentials, cached in KV) ───────
async function getAccessToken(){
  const cached = await kvGet(KV_TOKEN);
  if (cached && cached.token && cached.expiresAt > Date.now() + 60_000){
    return cached.token;
  }
  const shop = shopifyShop();
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: process.env.SHOPIFY_CLIENT_ID || "",
    client_secret: process.env.SHOPIFY_CLIENT_SECRET || ""
  }).toString();
  const r = await fetch(`https://${shop}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body
  });
  if (!r.ok) throw new Error(`Token fetch ${r.status}: ${await r.text().catch(()=>"")}`);
  const { access_token, expires_in } = await r.json();
  const ttl = Math.max(60, (expires_in || 86400) - 300);
  await kvSet(KV_TOKEN, { token: access_token, expiresAt: Date.now() + ttl * 1000 });
  return access_token;
}

// ── GraphQL helper ────────────────────────────────────────────────────────
async function gql(query, variables){
  const shop = shopifyShop();
  const token = await getAccessToken();
  const r = await fetch(`https://${shop}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": token
    },
    body: JSON.stringify({ query, variables })
  });
  const json = await r.json();
  if (json.errors && json.errors.length){
    throw new Error(`GraphQL: ${JSON.stringify(json.errors)}`);
  }
  return json.data;
}

// ── Bulk Operations ───────────────────────────────────────────────────────
const BULK_OP_FIELDS = `
  id status errorCode createdAt completedAt
  objectCount fileSize url partialDataUrl
`;

async function getCurrentBulkOp(){
  const data = await gql(`{ currentBulkOperation { ${BULK_OP_FIELDS} } }`);
  return data.currentBulkOperation;
}

async function getBulkOpById(id){
  const data = await gql(
    `query($id: ID!){ node(id: $id) { ... on BulkOperation { ${BULK_OP_FIELDS} } } }`,
    { id }
  );
  return data.node;
}

async function cancelBulkOp(id){
  return gql(`mutation { bulkOperationCancel(id: "${id}") { bulkOperation { status } userErrors { message } } }`);
}

async function startBulkOrders(createdSinceISO){
  // Bulk ops don't accept variables in the inner query string, so we inline
  // the date filter. Shopify's search syntax: created_at:>=YYYY-MM-DD.
  const dateFilter = createdSinceISO.slice(0, 10);
  const innerQuery = `
    {
      orders(query: "created_at:>=${dateFilter}", sortKey: CREATED_AT) {
        edges {
          node {
            id
            createdAt
            displayFinancialStatus
            currentTotalPriceSet { shopMoney { amount } }
            physicalLocation { id name }
            lineItems {
              edges {
                node {
                  id
                  title
                  quantity
                  originalUnitPriceSet { shopMoney { amount } }
                  variant { id title sku }
                  product { id productType }
                }
              }
            }
          }
        }
      }
    }
  `;
  const mutation = `
    mutation runBulk($q: String!) {
      bulkOperationRunQuery(query: $q) {
        bulkOperation { ${BULK_OP_FIELDS} }
        userErrors { field message }
      }
    }
  `;
  const data = await gql(mutation, { q: innerQuery });
  const errs = data.bulkOperationRunQuery.userErrors || [];
  if (errs.length) throw new Error(`Bulk start: ${JSON.stringify(errs)}`);
  return data.bulkOperationRunQuery.bulkOperation;
}

// ── Parse JSONL output ────────────────────────────────────────────────────
// Bulk export emits one JSON object per line. Top-level orders come first;
// nested line items follow with `__parentId` pointing back at their order.
async function downloadJsonl(url){
  const r = await fetch(url);
  if (!r.ok) throw new Error(`JSONL download ${r.status}`);
  return await r.text();
}
function parseJsonl(text){
  const orders = new Map();
  const lines = text.split("\n");
  for (const line of lines){
    if (!line) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    if (obj.id && obj.id.startsWith("gid://shopify/Order/")){
      orders.set(obj.id, { ...obj, lineItems: [] });
    } else if (obj.__parentId && obj.__parentId.startsWith("gid://shopify/Order/")){
      const parent = orders.get(obj.__parentId);
      if (parent) parent.lineItems.push(obj);
    }
  }
  return [...orders.values()];
}

// ── Aggregation ───────────────────────────────────────────────────────────
function prodKey(li){
  return (li.product && li.product.id) || li.title || "Unknown";
}
function prodName(li){
  return li.title || "Unknown";
}
function lineUnit(li){
  return parseFloat(((li.originalUnitPriceSet || {}).shopMoney || {}).amount || 0);
}
function lineQty(li){
  return parseFloat(li.quantity) || 0;
}

function buildGymData(orders){
  const seenOrders = new Set();
  const dedup = [];
  for (const o of orders){
    if (o && o.id){
      if (seenOrders.has(o.id)) continue;
      seenOrders.add(o.id);
    }
    dedup.push(o);
  }

  const daily = {};
  const now = Date.now();
  const periodCuts = {};
  PERIODS.forEach(d => { periodCuts[d] = now - d * 864e5; });
  const topBuckets = {};

  const addTop = (days, loc, li) => {
    const cat = topCat(li);
    if (!cat) return;
    const key = `${days}|${loc}`;
    if (!topBuckets[key]) topBuckets[key] = {};
    if (!topBuckets[key][cat]) topBuckets[key][cat] = {};
    const bucket = topBuckets[key][cat];
    const pKey = prodKey(li);
    if (!bucket[pKey]) bucket[pKey] = { n: prodName(li), t: 0, q: 0 };
    bucket[pKey].t += lineUnit(li) * lineQty(li);
    bucket[pKey].q += lineQty(li);
  };

  let processed = 0;
  for (const o of dedup){
    const locName = (o.physicalLocation && o.physicalLocation.name) || null;
    const branch = mapLocation(locName);
    if (!branch) continue;

    const orderTotal = parseFloat(((o.currentTotalPriceSet || {}).shopMoney || {}).amount || 0);
    if (!(orderTotal > 0)) continue;

    const lines = o.lineItems || [];
    let lineGym = 0, lineNonGym = 0;
    for (const li of lines){
      li.productType = (li.product && li.product.productType) || "";
      li.variantTitle = (li.variant && li.variant.title) || "";
      const r = lineUnit(li) * lineQty(li);
      if (isGP(li)) lineGym += r; else lineNonGym += r;
    }
    const lineSum = lineGym + lineNonGym;
    let ng, gym;
    if (lineSum > 0){
      const ratio = lineGym / lineSum;
      gym = orderTotal * ratio;
      ng  = orderTotal - gym;
    } else {
      ng = orderTotal; gym = 0;
    }

    const date = (o.createdAt || "").substring(0, 10);
    if (!date || date < "2020") continue;
    if (!daily[date]) daily[date] = {};
    if (!daily[date][branch]) daily[date][branch] = [0, 0, 0];
    daily[date][branch][0] += ng;
    daily[date][branch][1] += gym;
    daily[date][branch][2] += 1;

    const createdTs = new Date(o.createdAt || 0).getTime();
    if (lines.length && Number.isFinite(createdTs)){
      for (const days of PERIODS){
        if (createdTs < periodCuts[days]) continue;
        for (const li of lines){
          addTop(days, "all",  li);
          addTop(days, branch, li);
        }
      }
    }
    processed++;
  }

  const branches = [...new Set(Object.values(daily).flatMap(d => Object.keys(d)))].sort();
  const tops = {};
  for (const days of PERIODS){
    for (const loc of ["all", ...branches]){
      const key = `${days}|${loc}`;
      const b = topBuckets[key] || {};
      const out = {};
      for (const cat of TOP_CATEGORIES){
        out[cat] = Object.values(b[cat] || {})
          .filter(r => r.t > 0)
          .sort((a, b) => b.t - a.t)
          .slice(0, 15);
      }
      tops[key] = out;
    }
  }
  return { timestamp: Date.now(), daily, tops, branches, totalOrders: processed };
}

async function processCompleted(op, startTs){
  if (!op.url){
    const empty = { timestamp: Date.now(), daily: {}, tops: {}, branches: [], totalOrders: 0 };
    await kvSet(KV_GYM_DATA, empty);
    return { totalOrders: 0, objectCount: 0 };
  }
  const text = await downloadJsonl(op.url);
  const orders = parseJsonl(text);
  const gymData = buildGymData(orders);
  await kvSet(KV_GYM_DATA, gymData);
  return { totalOrders: gymData.totalOrders, objectCount: op.objectCount, durationSec: Math.round((Date.now()-startTs)/1000) };
}

// ── Handler ───────────────────────────────────────────────────────────────
module.exports = async function handler(req, res){
  const expected = process.env.CRON_SECRET;
  const auth = req.headers.authorization || "";
  if (!expected || auth !== `Bearer ${expected}`){
    return res.status(401).json({ error: "Unauthorized" });
  }

  const startTs = Date.now();

  try {
    // Phase: have a pending bulk we started earlier?
    const pending = await kvGet(KV_PENDING);
    if (pending && pending.bulkId){
      const op = await getBulkOpById(pending.bulkId);
      if (!op){
        await kvDel(KV_PENDING);
        return res.json({ ok: true, action: "stale-cleared" });
      }
      if (op.status === "RUNNING" || op.status === "CREATED"){
        if (Date.now() - (pending.startedAt || 0) > STALE_RUN_MS){
          await cancelBulkOp(op.id).catch(()=>{});
          await kvDel(KV_PENDING);
          return res.json({ ok: true, action: "stale-canceled", bulkId: op.id });
        }
        return res.json({ ok: true, action: "still-running", status: op.status, bulkId: op.id });
      }
      if (op.status === "COMPLETED"){
        const result = await processCompleted(op, startTs);
        await kvDel(KV_PENDING);
        return res.json({ ok: true, action: "completed", ...result });
      }
      // FAILED, CANCELED, EXPIRED — clear so next tick starts fresh.
      await kvDel(KV_PENDING);
      return res.status(500).json({ error: `Bulk op ${op.status}: ${op.errorCode || "no errorCode"}` });
    }

    // Phase: another bulk op already running on the shop? Adopt rather than fight.
    const current = await getCurrentBulkOp();
    if (current && (current.status === "RUNNING" || current.status === "CREATED")){
      await kvSet(KV_PENDING, { bulkId: current.id, startedAt: Date.now() });
      return res.json({ ok: true, action: "adopted", bulkId: current.id });
    }
    if (current && current.status === "COMPLETED" && current.url){
      // A previous run completed without us picking it up — use it if its
      // window matches what we'd request now (best-effort: just consume it).
      const result = await processCompleted(current, startTs);
      return res.json({ ok: true, action: "consumed-existing", ...result });
    }

    // Phase: no pending, nothing in flight — start a new bulk.
    const since = new Date();
    since.setMonth(since.getMonth() - HISTORY_MONTHS - 1);
    since.setHours(0, 0, 0, 0);
    const started = await startBulkOrders(since.toISOString());
    await kvSet(KV_PENDING, { bulkId: started.id, startedAt: Date.now() });
    return res.json({ ok: true, action: "started", bulkId: started.id });
  } catch(e){
    return res.status(500).json({ error: e.message });
  }
};

module.exports.config = { maxDuration: 60 };
