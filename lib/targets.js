// The target sheet: shared by /api/targets (admin read + write) and
// /api/progress (which turns targets into percentages for everyone else).
//
// Only two targets are entered: weekly and monthly. The daily target is
// derived as weekly ÷ 7 rather than typed, so it can never drift out of step
// with the weekly figure it should add up to. The sales week is Mon–Sun, and
// every day carries the same share of it.

const { kvGet } = require("./auth");
const { GYM_LABELS } = require("./gyms");

const KV_TARGETS = "targets";
const FIELDS = ["weekly", "monthly"];
const DAYS_IN_WEEK = 7;

// Which revenue counts toward target. All three are retail/POS takings at the
// gym; they differ only in which product categories count.
//
//   exgym_giftcards — Clothing, Shoes, Socks, Accessories and Gift Cards.
//                     The sales dashboard's "ex gym products" plus gift card
//                     sales. This is the default.
//   exgym           — the same without gift cards, matching the sales
//                     dashboard's headline figure exactly.
//   total           — every category, including supplements, drinks, food
//                     and memberships.
//
// There is no "Gym Products" category in Shopify; "ex gym products" means the
// apparel allowlist in api/refresh.js, which is where the definition lives.
const BASES = ["exgym_giftcards", "exgym", "total"];
const DEFAULT_BASIS = "exgym_giftcards";

// Snapshots written before gift cards were split out have no fourth element
// in their daily cells, so they report $0 of gift card sales until the next
// refresh cron runs. That understates the default basis rather than breaking it.
const LEGACY_BASIS_ALIASES = { apparel: "exgym" };

const dailyFromWeekly = weekly => (weekly > 0 ? weekly / DAYS_IN_WEEK : 0);

async function loadTargets(){
  const stored = await kvGet(KV_TARGETS);
  const gyms = (stored && stored.gyms) || {};
  const out = {};
  for (const label of GYM_LABELS){
    const t = gyms[label] || {};
    const weekly = Number(t.weekly) || 0;
    out[label] = {
      weekly,
      monthly: Number(t.monthly) || 0,
      // Derived, never stored. A `daily` left over from an older sheet is ignored.
      daily: dailyFromWeekly(weekly)
    };
  }
  const storedBasis = LEGACY_BASIS_ALIASES[stored && stored.basis] || (stored && stored.basis);
  const basis = BASES.includes(storedBasis) ? storedBasis : DEFAULT_BASIS;
  return {
    gyms: out,
    basis,
    updatedAt: (stored && stored.updatedAt) || null,
    updatedBy: (stored && stored.updatedBy) || null
  };
}

module.exports = {
  KV_TARGETS, FIELDS, DAYS_IN_WEEK, BASES, DEFAULT_BASIS,
  LEGACY_BASIS_ALIASES, dailyFromWeekly, loadTargets
};
