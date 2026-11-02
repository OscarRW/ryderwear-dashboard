// The target sheet: shared by /api/targets (admin read + write) and
// /api/progress (which turns targets into percentages for everyone else).

const { kvGet } = require("./_auth");
const { GYM_LABELS } = require("./_gyms");

const KV_TARGETS = "targets";
const FIELDS = ["daily", "weekly", "monthly"];

// Which revenue counts toward target:
//   total   — every order taken at the gym: apparel, supplements, drinks, food
//   apparel — apparel only, matching the sales dashboard's "ex gym products"
const BASES = ["total", "apparel"];
const DEFAULT_BASIS = "total";

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
  const basis = BASES.includes(stored && stored.basis) ? stored.basis : DEFAULT_BASIS;
  return {
    gyms: out,
    basis,
    updatedAt: (stored && stored.updatedAt) || null,
    updatedBy: (stored && stored.updatedBy) || null
  };
}

module.exports = { KV_TARGETS, FIELDS, BASES, DEFAULT_BASIS, loadTargets };
