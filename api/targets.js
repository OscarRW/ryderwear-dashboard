// Per-gym weekly sales targets, stored in KV under `targets`.
//
//   GET /api/targets?month=YYYY-MM   admin only — that sales month's weeks,
//                                    the roster, and the months worth offering
//   PUT /api/targets                 admin only — save one month's weeks
//
// Targets are the one place dollar figures live, and they are admin-only on
// purpose: gym accounts read /api/progress, which returns percentages only,
// with no field a revenue number could be reconstructed from.

const { requireSession, kvGet, kvSet, parseBody } = require("../lib/auth");
const { GYM_LABELS } = require("../lib/gyms");
const {
  KV_TARGETS, BASES, DEFAULT_BASIS, loadTargets, weekTarget, monthTarget, validAmount
} = require("../lib/targets");
const {
  salesMonthOf, salesMonthRange, weeksInSalesMonth, shiftMonth, monthLabel, addDays
} = require("../lib/dates");

// Which months the dropdown offers: a few back for reference, and far enough
// forward to set next month — and the one after — well before they start.
const MONTHS_BACK = 3;
const MONTHS_FORWARD = 3;

const ADELAIDE_TODAY = () => new Intl.DateTimeFormat("en-CA", {
  timeZone: "Australia/Adelaide", year: "numeric", month: "2-digit", day: "2-digit"
}).format(new Date());

function monthOptions(currentYm){
  const out = [];
  for (let i = -MONTHS_BACK; i <= MONTHS_FORWARD; i++){
    const ym = shiftMonth(currentYm, i);
    const range = salesMonthRange(ym);
    out.push({
      ym,
      label: monthLabel(ym),
      weeks: range.weeks.length,
      start: range.start,
      end: range.end,
      isCurrent: i === 0
    });
  }
  return out;
}

function monthPayload(sheet, ym){
  const range = salesMonthRange(ym);
  const weeks = range.weeks.map((monday, i) => ({
    monday,
    sunday: addDays(monday, 6),
    label: `Week ${i + 1}`
  }));

  const gyms = {};
  for (const gym of GYM_LABELS){
    const row = { weeks: {}, sources: {} };
    for (const { monday } of weeks){
      const w = weekTarget(sheet, gym, monday);
      row.weeks[monday] = w.amount;
      row.sources[monday] = w.source;
    }
    row.month = monthTarget(sheet, gym, ym).amount;
    gyms[gym] = row;
  }

  return {
    month: ym,
    monthLabel: monthLabel(ym),
    range: { start: range.start, end: range.end, days: range.days },
    weeks,
    gyms
  };
}

module.exports = async function handler(req, res){
  res.setHeader("Cache-Control", "no-store");

  const session = requireSession(req, res, { adminOnly: true });
  if (!session) return;

  const currentYm = salesMonthOf(ADELAIDE_TODAY());

  let sheet;
  try { sheet = await loadTargets(); }
  catch (e) { return res.status(500).json({ error: e.message }); }

  if (req.method === "GET"){
    const asked = String((req.query && req.query.month) || "");
    const ym = /^\d{4}-\d{2}$/.test(asked) ? asked : currentYm;
    try {
      return res.status(200).json({
        ...monthPayload(sheet, ym),
        basis: sheet.basis,
        roster: GYM_LABELS,
        currentMonth: currentYm,
        months: monthOptions(currentYm),
        updatedAt: sheet.updatedAt,
        updatedBy: sheet.updatedBy
      });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (req.method === "PUT" || req.method === "POST"){
    const body = parseBody(req);
    const ym = String(body.month || "");
    if (!/^\d{4}-\d{2}$/.test(ym)){
      return res.status(400).json({ error: "Expected a `month` of the form YYYY-MM" });
    }
    const incoming = body.gyms;
    if (!incoming || typeof incoming !== "object"){
      return res.status(400).json({ error: "Expected a `gyms` object" });
    }

    const monthWeeks = weeksInSalesMonth(ym);
    const monthWeekSet = new Set(monthWeeks);

    // Only this month's weeks are touched; every other week already stored is
    // left exactly as it was, so saving November can't disturb December.
    const weeks = {};
    for (const [monday, row] of Object.entries(sheet.weeks)){
      if (!monthWeekSet.has(monday)) weeks[monday] = { ...row };
    }

    for (const monday of monthWeeks){
      const row = {};
      for (const gym of GYM_LABELS){
        const raw = ((incoming[gym] || {}).weeks || {})[monday];
        const value = validAmount(raw);
        if (value === null){
          return res.status(400).json({ error: `Invalid target for ${gym}, week of ${monday}` });
        }
        if (value > 0) row[gym] = value;
      }
      if (Object.keys(row).length) weeks[monday] = row;
    }

    const basis = BASES.includes(body.basis) ? body.basis : (sheet.basis || DEFAULT_BASIS);
    const payload = { weeks, basis, updatedAt: Date.now(), updatedBy: session.username };

    try { await kvSet(KV_TARGETS, payload); }
    catch (e) { return res.status(500).json({ error: e.message }); }

    const saved = await loadTargets();
    return res.status(200).json({
      ok: true,
      ...monthPayload(saved, ym),
      basis: saved.basis,
      roster: GYM_LABELS,
      currentMonth: currentYm,
      months: monthOptions(currentYm),
      updatedAt: saved.updatedAt,
      updatedBy: saved.updatedBy
    });
  }

  res.setHeader("Allow", "GET, PUT");
  res.status(405).json({ error: "method not allowed" });
};
