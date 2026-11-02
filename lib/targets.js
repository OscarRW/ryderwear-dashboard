// The target sheet: shared by /api/targets (admin read + write) and
// /api/progress (which turns targets into percentages for everyone else).
//
// Targets are entered per week, keyed by the Monday that starts the week, so
// a month can be filled in ahead of time and each week can differ. Months are
// a grouping of those weeks (see lib/dates.js), not a separate figure: the
// month's target is the sum of its four or five weeks, which is why the two
// can never disagree.
//
// The daily target is derived too — the week's figure split across Mon–Sun by
// each gym's own trading shape, which api/progress.js works out. Nothing below
// the week is ever entered or stored.
//
// Shape in KV:
//   {
//     basis: "total",
//     weeks: { "2026-11-02": { "Malaga, WA": 7500, … }, "2026-11-09": { … } },
//     updatedAt, updatedBy
//   }

const { kvGet } = require("./auth");
const { GYM_LABELS } = require("./gyms");
const { weeksInSalesMonth } = require("./dates");

const KV_TARGETS = "targets";

// Which revenue counts toward target. All three are retail/POS takings at the
// gym; they differ only in which product categories count.
//
//   total           — every category, including supplements, drinks, food
//                     and memberships. The default: gym targets are normally
//                     set against the whole till.
//   exgym_giftcards — Clothing, Shoes, Socks, Accessories and Gift Cards.
//   exgym           — the same without gift cards, matching the sales
//                     dashboard's headline figure exactly.
//
// There is no "Gym Products" category in Shopify; "ex gym products" means the
// apparel allowlist in api/refresh.js, which is where the definition lives.
const BASES = ["total", "exgym_giftcards", "exgym"];
const DEFAULT_BASIS = "total";
const LEGACY_BASIS_ALIASES = { apparel: "exgym" };

const MAX_TARGET = 100_000_000;

async function loadTargets(){
  const stored = (await kvGet(KV_TARGETS)) || {};

  const weeks = {};
  for (const [monday, row] of Object.entries(stored.weeks || {})){
    if (!/^\d{4}-\d{2}-\d{2}$/.test(monday)) continue;
    const clean = {};
    for (const gym of GYM_LABELS){
      const v = Number(row[gym]);
      if (Number.isFinite(v) && v > 0) clean[gym] = v;
    }
    if (Object.keys(clean).length) weeks[monday] = clean;
  }

  // Sheets written before targets were per-week carried one recurring weekly
  // figure per gym. Keep it as the value for any week never filled in, so a
  // month nobody has got to yet still tracks instead of going blank.
  const carried = {};
  for (const gym of GYM_LABELS){
    const v = Number(((stored.gyms || {})[gym] || {}).weekly);
    if (Number.isFinite(v) && v > 0) carried[gym] = v;
  }

  const storedBasis = LEGACY_BASIS_ALIASES[stored.basis] || stored.basis;
  return {
    weeks,
    carried,
    sortedWeeks: Object.keys(weeks).sort(),
    basis: BASES.includes(storedBasis) ? storedBasis : DEFAULT_BASIS,
    updatedAt: stored.updatedAt || null,
    updatedBy: stored.updatedBy || null
  };
}

// What a gym is working to in the week beginning `mondayISO`.
//   explicit  — entered for that week
//   carried   — nothing entered yet, so the most recent earlier week stands in
//               (or the pre-per-week figure, if there is no earlier week)
//   unset     — nothing to go on
function weekTarget(sheet, gym, mondayISO){
  const direct = (sheet.weeks[mondayISO] || {})[gym];
  if (direct > 0) return { amount: direct, source: "explicit" };

  for (let i = sheet.sortedWeeks.length - 1; i >= 0; i--){
    const wk = sheet.sortedWeeks[i];
    if (wk >= mondayISO) continue;
    const prior = sheet.weeks[wk][gym];
    if (prior > 0) return { amount: prior, source: "carried", from: wk };
  }

  const legacy = sheet.carried[gym];
  if (legacy > 0) return { amount: legacy, source: "carried" };
  return { amount: 0, source: "unset" };
}

// A sales month's target is its weeks added up — never entered, so the month
// and its weeks cannot drift apart.
function monthTarget(sheet, gym, ym){
  let total = 0, anyExplicit = false, anySet = false;
  for (const monday of weeksInSalesMonth(ym)){
    const w = weekTarget(sheet, gym, monday);
    total += w.amount;
    if (w.source === "explicit") anyExplicit = true;
    if (w.amount > 0) anySet = true;
  }
  return { amount: total, anyExplicit, anySet };
}

function validAmount(raw){
  const v = raw === "" || raw == null ? 0 : Number(raw);
  return Number.isFinite(v) && v >= 0 && v <= MAX_TARGET ? Math.round(v * 100) / 100 : null;
}

module.exports = {
  KV_TARGETS, BASES, DEFAULT_BASIS, LEGACY_BASIS_ALIASES, MAX_TARGET,
  loadTargets, weekTarget, monthTarget, validAmount
};
