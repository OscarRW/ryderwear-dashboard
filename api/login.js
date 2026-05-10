const crypto = require('crypto');

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'method not allowed' });
  }

  const expected = process.env.DASHBOARD_PASSWORD;
  const secret = process.env.AUTH_SECRET;

  if (!expected || !secret) {
    return res.status(500).json({ error: 'auth not configured' });
  }

  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch { body = {}; }
  }
  const password = body && body.password;

  if (typeof password !== 'string' || password.length === 0) {
    return res.status(400).json({ error: 'password required' });
  }

  const a = Buffer.from(password);
  const b = Buffer.from(expected);
  const equal = a.length === b.length && crypto.timingSafeEqual(a, b);

  if (!equal) {
    return res.status(401).json({ error: 'invalid password' });
  }

  const maxAge = 30 * 24 * 60 * 60;
  const expiry = Date.now() + maxAge * 1000;
  const sig = crypto
    .createHmac('sha256', secret)
    .update(String(expiry))
    .digest('base64url');
  const token = `${expiry}.${sig}`;

  res.setHeader(
    'Set-Cookie',
    `auth=${token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=${maxAge}`
  );
  res.status(200).json({ ok: true });
};
