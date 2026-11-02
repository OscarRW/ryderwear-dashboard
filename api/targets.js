// Per-gym sales targets, stored in KV under `targets`.
//
//   GET /api/targets   admin only — the full target sheet plus the roster
//   PUT /api/targets   admin only — replace the sheet
//
// Targets are the one place dollar figures live, and they are admin-only on
// purpose: gym accounts read /api/progress, which returns percentages only,
// with no field a revenue number could be reconstructed from.
//
// Shape:
//   { updatedAt, updatedBy, basis, gyms: { "<label>": { daily, weekly, monthly } } }

const { requireSession, kvSet, parseBody } = require("./_auth");
const { GYM_LABELS } = require("./_gyms");
const { KV_TARGETS, FIELDS, BASES, DEFAULT_BASIS, loadTargets } = require("./_targets");

const MAX_TARGET = 100_000_000;

module.exports = async function handler(req, res){
  res.setHeader("Cache-Control", "no-store");

  const session = requireSession(req, res, { adminOnly: true });
  if (!session) return;

  if (req.method === "GET"){
    try {
      const targets = await loadTargets();
      return res.status(200).json({ ...targets, roster: GYM_LABELS });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (req.method === "PUT" || req.method === "POST"){
    const body = parseBody(req);
    const incoming = body.gyms;
    if (!incoming || typeof incoming !== "object"){
      return res.status(400).json({ error: "Expected a `gyms` object" });
    }

    // Gyms the caller didn't mention keep what they had. Clearing a target
    // means sending a 0 for it, which is what the admin page does — this way
    // a partial write can't silently wipe the rest of the sheet.
    const current = await loadTargets().catch(() => ({ gyms: {} }));

    const gyms = {};
    for (const label of GYM_LABELS){
      const t = incoming[label] || current.gyms[label] || {};
      const row = {};
      for (const field of FIELDS){
        const raw = t[field];
        const value = raw === "" || raw == null ? 0 : Number(raw);
        if (!Number.isFinite(value) || value < 0 || value > MAX_TARGET){
          return res.status(400).json({ error: `Invalid ${field} target for ${label}` });
        }
        row[field] = Math.round(value * 100) / 100;
      }
      gyms[label] = row;
    }

    const basis = BASES.includes(body.basis) ? body.basis : DEFAULT_BASIS;
    const payload = { gyms, basis, updatedAt: Date.now(), updatedBy: session.username };
    try {
      await kvSet(KV_TARGETS, payload);
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
    return res.status(200).json({ ok: true, ...payload, roster: GYM_LABELS });
  }

  res.setHeader("Allow", "GET, PUT");
  res.status(405).json({ error: "method not allowed" });
};
