// Returns the latest aggregated `gymData` from Vercel KV (populated by the
// /api/refresh cron). The dashboard hits this endpoint instead of looping
// through 11 gyms × N pages of /api/cin7 — load drops from ~3 min to <1 sec.
//
// Returns 503 if KV isn't configured and 404 if the cron hasn't run yet,
// which lets the dashboard fall back to its existing direct-fetch path.

module.exports = async function handler(req, res){
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "public, s-maxage=300, stale-while-revalidate=86400");

  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) {
    return res.status(503).json({ error: "KV not configured" });
  }

  try {
    const r = await fetch(`${url}/get/gymData`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!r.ok) return res.status(502).json({ error: `KV ${r.status}` });
    const { result } = await r.json();
    if (!result) return res.status(404).json({ error: "Not yet refreshed" });
    const data = typeof result === "string" ? JSON.parse(result) : result;
    res.status(200).json(data);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
