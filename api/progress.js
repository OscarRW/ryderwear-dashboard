// GET /api/progress — per-gym tracking against target.
//
// Every figure is a percentage of target, a count of days or a ratio, with one
// deliberate exception: `requiredPerDay`, the dollars a gym needs to take per
// remaining day to land the week or the month. That is the number a manager
// acts on, so it is quoted in dollars even for gym accounts; revenue to date
// and the targets themselves still never leave this endpoint.
//
// A gym account sees only the gyms on its own login.
//
// Dates follow Australia/Adelaide, matching how /api/refresh buckets orders.
// The 1.5h offset to the WA gyms only misattributes orders placed after
// 22:30 AWST, which is past closing.

const { requireSession, kvGet, scopeGyms } = require("../lib/auth");
const { GYM_LABELS } = require("../lib/gyms");
const { loadTargets } = require("../lib/targets");

const KV_GYM_DATA = "gymData";
const TREND_DAYS = 14;

// Rough trading window, used only to work out how much of *today* has gone
// when judging pace. Without it every gym looks catastrophically behind at 9am.
const TRADING_START_HOUR = 6;
const TRADING_END_HOUR = 22;

// Projecting a whole week off a partial Monday is noise, not a forecast: a
// morning's takings divided by a third of a day extrapolates to a run rate
// several times reality. Wait for one full day in the period before quoting a
// run rate or a projection; until then the UI shows "too early" and managers
// go off today's own percentage and the required-per-day figure, which are
// both sound from the first hour.
const MIN_ELAPSED_DAYS_TO_PROJECT = 1;

const ADELAIDE = "Australia/Adelaide";
const DATE_FMT = new Intl.DateTimeFormat("en-CA", {
  timeZone: ADELAIDE, year: "numeric", month: "2-digit", day: "2-digit"
});
const TIME_FMT = new Intl.DateTimeFormat("en-GB", {
  timeZone: ADELAIDE, hour: "2-digit", minute: "2-digit", hour12: false
});

function adelaideNow(){
  const now = new Date();
  const date = DATE_FMT.format(now);
  const [hRaw, m] = TIME_FMT.format(now).split(":").map(Number);
  const hour = (hRaw === 24 ? 0 : hRaw) + (m || 0) / 60;
  return { date, hour };
}

// How much of today's trading has elapsed, 0–1.
function tradingFraction(hour){
  const span = TRADING_END_HOUR - TRADING_START_HOUR;
  return Math.min(1, Math.max(0, (hour - TRADING_START_HOUR) / span));
}

// ISO date helpers. Anchored at UTC noon so a DST shift can't roll the date.
function toDate(iso){
  const [y, m, d] = iso.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d, 12));
}
function toISO(dt){ return dt.toISOString().slice(0, 10); }
function addDays(iso, n){
  const d = toDate(iso);
  d.setUTCDate(d.getUTCDate() + n);
  return toISO(d);
}

// daily[date][gym] = [exGymRevenue, excludedRevenue, orderCount, giftCardRevenue]
// where giftCardRevenue is a slice of excludedRevenue, not a fourth bucket.
// Snapshots written before the gift card split have no index 3 and read as 0.
function cellValue(cell, basis){
  if (!cell) return 0;
  const exGym = cell[0] || 0;
  if (basis === "exgym") return exGym;
  if (basis === "total") return exGym + (cell[1] || 0);
  return exGym + (cell[3] || 0); // exgym_giftcards
}

function sumRange(daily, gym, fromISO, toISOInclusive, basis){
  let total = 0;
  for (let d = fromISO; d <= toISOInclusive; d = addDays(d, 1)){
    const row = daily[d];
    if (row) total += cellValue(row[gym], basis);
  }
  return total;
}

const pct = (actual, target) => (target > 0 ? (actual / target) * 100 : null);
const round1 = v => (v == null ? null : Math.round(v * 10) / 10);

// One period (week or month) reduced to percentages.
//   daysTotal        length of the period in days
//   daysElapsedWhole 1-based index of today within the period
//   todayFraction    how much of today has traded, 0–1
function periodStats({ actual, target, dayTarget, daysTotal, daysElapsedWhole, todayFraction }){
  const elapsed = (daysElapsedWhole - 1) + todayFraction;
  const daysRemainingWhole = daysTotal - daysElapsedWhole;

  const achievedPct = pct(actual, target);
  const expectedPct = daysTotal > 0 ? (elapsed / daysTotal) * 100 : null;
  const pacePct = achievedPct != null && expectedPct > 0 ? (achievedPct / expectedPct) * 100 : null;

  const canProject = elapsed >= MIN_ELAPSED_DAYS_TO_PROJECT;
  const runRateDaily = canProject ? actual / elapsed : null;
  const projectedPct = canProject ? pct(runRateDaily * daysTotal, target) : null;

  // The bar a gym has to clear from here on. This is the one figure quoted in
  // dollars — it's what a manager acts on, and a percentage of a target day
  // is a worse instruction than a number on the till. The percentage version
  // is kept alongside it to drive the colour coding.
  //
  // Spread over whole days left *including today*, not the fractional remainder,
  // so the figure reconciles with the day count shown beside it: a manager who
  // multiplies "$86 a day × 7 days" gets back the shortfall. Using the fraction
  // of today still to run would be marginally more precise and would not
  // multiply back, which is a bad trade for a number people read at a glance.
  const effectiveDayTarget = dayTarget > 0 ? dayTarget : (target > 0 ? target / daysTotal : 0);
  const shortfall = Math.max(0, target - actual);
  const daysLeftInclToday = Math.max(1, daysTotal - daysElapsedWhole + 1);
  const requiredDaily = shortfall / daysLeftInclToday;

  return {
    targetSet: target > 0,
    achievedPct: round1(achievedPct),
    expectedPct: round1(expectedPct),
    pacePct: round1(pacePct),
    projectedPct: round1(projectedPct),
    runRatePctOfDaily: round1(runRateDaily != null ? pct(runRateDaily, effectiveDayTarget) : null),
    requiredPerDay: Math.round(requiredDaily),
    requiredPctOfDaily: round1(pct(requiredDaily, effectiveDayTarget)),
    metAlready: target > 0 && actual >= target,
    daysTotal,
    daysElapsed: daysElapsedWhole,
    daysRemaining: Math.max(0, daysRemainingWhole),
    daysRemainingIncludingToday: Math.max(0, daysRemainingWhole + 1)
  };
}

function buildGymRow({ gym, daily, targets, basis, cal }){
  const t = targets[gym] || { daily: 0, weekly: 0, monthly: 0 };

  const todayActual = sumRange(daily, gym, cal.today, cal.today, basis);
  const weekActual  = sumRange(daily, gym, cal.weekStart, cal.today, basis);
  const monthActual = sumRange(daily, gym, cal.monthStart, cal.today, basis);
  const yesterdayActual = sumRange(daily, gym, cal.yesterday, cal.yesterday, basis);

  const trend = [];
  for (let i = TREND_DAYS - 1; i >= 0; i--){
    const date = addDays(cal.today, -i);
    const actual = sumRange(daily, gym, date, date, basis);
    trend.push({
      date,
      pct: round1(pct(actual, t.daily)),
      isToday: date === cal.today
    });
  }

  return {
    gym,
    hasAnyTarget: t.daily > 0 || t.weekly > 0 || t.monthly > 0,
    today: {
      targetSet: t.daily > 0,
      achievedPct: round1(pct(todayActual, t.daily)),
      expectedPct: round1(cal.todayFraction * 100),
      metAlready: t.daily > 0 && todayActual >= t.daily
    },
    yesterday: {
      targetSet: t.daily > 0,
      achievedPct: round1(pct(yesterdayActual, t.daily))
    },
    week: periodStats({
      actual: weekActual, target: t.weekly, dayTarget: t.daily,
      daysTotal: 7, daysElapsedWhole: cal.weekDayIndex, todayFraction: cal.todayFraction
    }),
    month: periodStats({
      actual: monthActual, target: t.monthly, dayTarget: t.daily,
      daysTotal: cal.daysInMonth, daysElapsedWhole: cal.monthDayIndex, todayFraction: cal.todayFraction
    }),
    trend
  };
}

module.exports = async function handler(req, res){
  res.setHeader("Cache-Control", "no-store");

  const session = requireSession(req, res);
  if (!session) return;

  let snapshot, targetSheet;
  try {
    [snapshot, targetSheet] = await Promise.all([kvGet(KV_GYM_DATA), loadTargets()]);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }

  if (!process.env.KV_REST_API_URL || !process.env.KV_REST_API_TOKEN){
    return res.status(503).json({ error: "KV not configured" });
  }
  if (!snapshot || !snapshot.daily){
    return res.status(404).json({ error: "No sales snapshot yet — the refresh cron hasn't run" });
  }

  const { date: today, hour } = adelaideNow();
  const todayFraction = tradingFraction(hour);

  const dow = toDate(today).getUTCDay();          // 0 = Sunday
  const weekDayIndex = dow === 0 ? 7 : dow;       // 1 = Monday … 7 = Sunday
  const [year, month] = today.split("-").map(Number);

  const cal = {
    today,
    yesterday: addDays(today, -1),
    todayFraction,
    weekStart: addDays(today, -(weekDayIndex - 1)),
    weekDayIndex,
    monthStart: `${year}-${String(month).padStart(2, "0")}-01`,
    monthDayIndex: Number(today.slice(8, 10)),
    daysInMonth: new Date(Date.UTC(year, month, 0)).getUTCDate()
  };

  const basis = targetSheet.basis;
  const daily = snapshot.daily;
  const gyms = scopeGyms(session, GYM_LABELS);

  const rows = gyms.map(gym => buildGymRow({ gym, daily, targets: targetSheet.gyms, basis, cal }));

  // Roll-up: sum the actuals and the targets, then take the percentage, so a
  // big gym carries more weight than a small one.
  const totals = { today: 0, week: 0, month: 0, yesterday: 0 };
  const targetTotals = { daily: 0, weekly: 0, monthly: 0 };
  for (const gym of gyms){
    totals.today     += sumRange(daily, gym, cal.today, cal.today, basis);
    totals.yesterday += sumRange(daily, gym, cal.yesterday, cal.yesterday, basis);
    totals.week      += sumRange(daily, gym, cal.weekStart, cal.today, basis);
    totals.month     += sumRange(daily, gym, cal.monthStart, cal.today, basis);
    const t = targetSheet.gyms[gym] || {};
    targetTotals.daily   += t.daily   || 0;
    targetTotals.weekly  += t.weekly  || 0;
    targetTotals.monthly += t.monthly || 0;
  }

  const group = {
    gymCount: gyms.length,
    gymsWithTargets: rows.filter(r => r.hasAnyTarget).length,
    today: {
      targetSet: targetTotals.daily > 0,
      achievedPct: round1(pct(totals.today, targetTotals.daily)),
      expectedPct: round1(todayFraction * 100),
      metAlready: targetTotals.daily > 0 && totals.today >= targetTotals.daily
    },
    yesterday: {
      targetSet: targetTotals.daily > 0,
      achievedPct: round1(pct(totals.yesterday, targetTotals.daily))
    },
    week: periodStats({
      actual: totals.week, target: targetTotals.weekly, dayTarget: targetTotals.daily,
      daysTotal: 7, daysElapsedWhole: cal.weekDayIndex, todayFraction
    }),
    month: periodStats({
      actual: totals.month, target: targetTotals.monthly, dayTarget: targetTotals.daily,
      daysTotal: cal.daysInMonth, daysElapsedWhole: cal.monthDayIndex, todayFraction
    })
  };

  res.status(200).json({
    role: session.role,
    username: session.username,
    basis,
    calendar: {
      today: cal.today,
      weekStart: cal.weekStart,
      weekDayIndex: cal.weekDayIndex,
      monthStart: cal.monthStart,
      monthDayIndex: cal.monthDayIndex,
      daysInMonth: cal.daysInMonth,
      todayElapsedPct: round1(todayFraction * 100),
      timezone: ADELAIDE
    },
    dataUpdatedAt: snapshot.timestamp || null,
    targetsUpdatedAt: targetSheet.updatedAt,
    group,
    gyms: rows
  });
};
