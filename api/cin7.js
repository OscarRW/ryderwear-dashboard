const https = require("https");

module.exports = function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { endpoint, rows = 250, page = 1, where } = req.query;
  if (!endpoint) return res.status(400).json({ error: "Missing endpoint" });

  const auth = Buffer.from(
    (process.env.CIN7_USER || "") + ":" + (process.env.CIN7_KEY || "")
  ).toString("base64");

  const params = new URLSearchParams();
  params.set("rows", rows);
  params.set("page", page);
  if (where) params.set("where", where);

  const path = `/api/v1/${endpoint}?${params.toString()}`;

  const options = {
    hostname: "api.cin7.com",
    path,
    method: "GET",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/json",
    },
  };

  const request = https.request(options, (r) => {
    let data = "";
    r.on("data", (chunk) => (data += chunk));
    r.on("end", () => {
      try {
        res.status(r.statusCode).json(JSON.parse(data));
      } catch (e) {
        res.status(500).json({ error: "Parse error", raw: data.slice(0, 300) });
      }
    });
  });

  request.on("error", (e) => res.status(500).json({ error: e.message }));
  request.end();
};
