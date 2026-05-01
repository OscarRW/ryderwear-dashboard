// Diagnostic: lists every distinct line-item productType in the recent orders
// window, with revenue and unit counts. Use to figure out what string the
// gym/nutrition classifier should match.
//
//   GET /api/types  (Authorization: Bearer $CRON_SECRET)

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

module.exports = async function handler(req, res){
  const expected = process.env.CRON_SECRET;
  const auth = req.headers.authorization || "";
  if (!expected || auth !== `Bearer ${expected}`){
    return res.status(401).json({ error: "Unauthorized" });
  }

  try {
    const since = new Date(Date.now() - 14 * 864e5).toISOString().slice(0, 10);
    const types = {};            // productType -> { revenue, units, sample[] }
    const products = {};         // productType -> productTitle counts (for samples)
    let cursor = null;
    let pages = 0;
    const MAX_PAGES = 10; // 10 × 50 = 500 orders; usually plenty

    while (pages < MAX_PAGES){
      pages++;
      const q = `
        query($cursor: String) {
          orders(first: 50, after: $cursor, query: "created_at:>=${since}", sortKey: CREATED_AT, reverse: true) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                id
                lineItems(first: 100) {
                  edges {
                    node {
                      title
                      quantity
                      originalUnitPriceSet { shopMoney { amount } }
                      product { productType }
                    }
                  }
                }
              }
            }
          }
        }
      `;
      const data = await gql(q, { cursor });
      const orders = data.orders.edges.map(e => e.node);
      for (const o of orders){
        for (const liEdge of (o.lineItems && o.lineItems.edges) || []){
          const li = liEdge.node;
          const pt = ((li.product && li.product.productType) || "").trim() || "(empty)";
          const unit = parseFloat(((li.originalUnitPriceSet || {}).shopMoney || {}).amount || 0);
          const qty = parseFloat(li.quantity) || 0;
          if (!types[pt]) types[pt] = { revenue: 0, units: 0, samples: {} };
          types[pt].revenue += unit * qty;
          types[pt].units += qty;
          types[pt].samples[li.title || "?"] = (types[pt].samples[li.title || "?"] || 0) + qty;
        }
      }
      if (!data.orders.pageInfo.hasNextPage) break;
      cursor = data.orders.pageInfo.endCursor;
    }

    const sorted = Object.entries(types)
      .map(([pt, v]) => ({
        productType: pt,
        revenue: Math.round(v.revenue * 100) / 100,
        units: v.units,
        topProducts: Object.entries(v.samples).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([n,q])=>({name:n,qty:q}))
      }))
      .sort((a, b) => b.revenue - a.revenue);

    res.status(200).json({
      ok: true,
      since,
      ordersScanned: pages * 50,
      types: sorted
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};

module.exports.config = { maxDuration: 60 };
