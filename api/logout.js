// POST /api/logout — clears the session cookie.

const { clearedCookie } = require("../lib/auth");

module.exports = async function handler(req, res){
  if (req.method !== "POST"){
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "method not allowed" });
  }
  res.setHeader("Set-Cookie", clearedCookie());
  res.status(200).json({ ok: true });
};
