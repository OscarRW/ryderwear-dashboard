// The target sheet: shared by /api/targets (admin read + write) and
// /api/progress (which turns targets into percentages for everyone else).

const { kvGet } = require("./_auth");
const { GYM_LABELS } = require("./_gyms");

const KV_TARGETS = "targets";
const FIELDS = ["daily", "weekly", "monthly"];

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

async function loadTargets(){
  const stored = await kvGet(KV_TARGETS);
  const gyms = (stored && stored.gyms) || {};
  const out = {};
  for (const label of GYM_LABELS){
    const t = gyms[label] || {};
    out[label] = {
      daily:   Number(t.daily)   || 0,
      weekly:  Number(t.weekly)  || 0,
      monthly: Number(t.monthly) || 0
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

module.exports = { KV_TARGETS, FIELDS, BASES, DEFAULT_BASIS, LEGACY_BASIS_ALIASES, loadTargets };
