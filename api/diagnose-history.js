// Diagnostic: how far back does Shopify let us see orders?
// Tells us whether limited history is real (oldest order is recent) or
// whether the read_all_orders scope is blocking older orders.
//
//   GET /api/diagnose-history  (Authorization: Bearer $CRON_SECRET)

const SHOPIFY_API_VERSION = "2024-10";
const KV_TOKEN = "shopifyToken";

function shopifyShop(){
  const s = process.env.SHOPIFY_STORE || "";
  return s.includes(".") ? s : `${s}.myshopify.com`;
}

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
  await fetch(`${url}/set/${encodeURIComponent(key)}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify(value)
  });
}

async function getFreshAccessToken(){
  // Always do a fresh exchange so we get the `scope` field back —
  // the cache only stores the token string, not the granted scopes.
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
  const { access_token, expires_in, scope } = await r.json();
  const ttl = Math.max(60, (expires_in || 86400) - 300);
  await kvSet(KV_TOKEN, { token: access_token, expiresAt: Date.now() + ttl * 1000 });
  return { token: access_token, scope };
}

async function gql(token, query, variables){
  const shop = shopifyShop();
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

module.exports = async function handler(req, res){
  const expected = process.env.CRON_SECRET;
  const auth = req.headers.authorization || "";
  if (!expected || auth !== `Bearer ${expected}`){
    return res.status(401).json({ error: "Unauthorized" });
  }

  try {
    const { token, scope } = await getFreshAccessToken();

    const safe = async (label, fn) => {
      try { return await fn(); } catch (e) { return { error: `${label}: ${e.message}` }; }
    };

    // Probe 1: oldest order overall (no date filter, ascending)
    const oldest = await safe("oldest", () => gql(token,
      `{ orders(first: 5, sortKey: CREATED_AT, reverse: false) {
          edges { node { id createdAt name physicalLocation { name } } }
        } }`
    ));

    // Probe 2: orders >90 days old (well beyond the 60d default cap)
    const cutoff = new Date(Date.now() - 90 * 864e5).toISOString().slice(0, 10);
    const oldOrders = await safe("old", () => gql(token,
      `{ orders(first: 5, query: "created_at:<${cutoff}", sortKey: CREATED_AT, reverse: true) {
          edges { node { id createdAt name physicalLocation { name } } }
        } }`
    ));

    // Probe 3: total order count for the store
    const ordersCount = await safe("count", () => gql(token, `{ ordersCount { count } }`));

    res.status(200).json({
      ok: true,
      tokenScope: scope || "(scope not returned by token endpoint)",
      cutoffFor90DayProbe: cutoff,
      probe1_oldestOrderInStore: oldest.orders ? oldest.orders.edges.map(e => e.node) : oldest,
      probe2_ordersOlderThan90Days: oldOrders.orders ? oldOrders.orders.edges.map(e => e.node) : oldOrders,
      probe3_totalOrdersCount: ordersCount.ordersCount ? ordersCount.ordersCount.count : ordersCount,
      interpretation: {
        ifOldestIsRecent: "Shopify only has recent orders — gym launched recently, no API limit issue.",
        ifOldestIsOldButProbe2Empty: "App lacks read_all_orders scope — older orders exist but API is hiding them.",
        ifBothShowOldData: "Older data IS accessible. The issue is elsewhere (date filter, location mapping, etc.)"
      }
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};

module.exports.config = { maxDuration: 30 };
