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
const round2 = v => (v == null ? null : Math.round(v * 100) / 100);

// ── Weekday shape ─────────────────────────────────────────────────────────
// A gym doesn't trade evenly across the week — weekends run well below a
// weekday. Splitting the weekly target into seven equal days therefore marks
// every Saturday and Sunday short even in a week that finishes ahead, which
// trains people to ignore the number.
//
// So the split follows each gym's own history instead: the share of a week's
// takings that has landed on each weekday, measured over the trailing window
// and applied to the weekly target. Nobody types anything, and the seven days
// still add up to exactly the weekly target.
const DOW_WINDOW_DAYS = 84;   // 12 weeks — enough samples to see a shape
const DOW_MIN_SAMPLES = 4;    // per weekday, before trusting it over an even split

// 0 = Monday … 6 = Sunday
function dowIndex(iso){
  const d = toDate(iso).getUTCDay();
  return d === 0 ? 6 : d - 1;
}

// Median, not mean: one freak trading day or a one-off closure shouldn't
// redefine what a normal Tuesday looks like.
function median(values){
  if (!values.length) return 0;
  const s = values.slice().sort((a, b) => a - b);
  const mid = s.length >> 1;
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

// Seven fractions summing to 1, or null when there isn't enough settled
// history to see a pattern — in which case the caller falls back to sevenths.
// Today is excluded throughout: it's half-traded and would drag its weekday down.
function weekdayShares(daily, gym, basis, today){
  const buckets = [[], [], [], [], [], [], []];
  for (let i = 1; i <= DOW_WINDOW_DAYS; i++){
    const date = addDays(today, -i);
    if (!daily[date]) continue;                  // no snapshot for that day at all
    buckets[dowIndex(date)].push(cellValue(daily[date][gym], basis));
  }
  if (buckets.some(b => b.length < DOW_MIN_SAMPLES)) return null;

  const medians = buckets.map(median);
  const total = medians.reduce((a, b) => a + b, 0);
  if (!(total > 0)) return null;
  return medians.map(m => m / total);
}

// The dollar target for one gym on one date, and how that compares with an
// average day (1.0 = average, 0.7 = a typical quiet Sunday).
function makeDayTarget(weekly, shares){
  const even = weekly > 0 ? weekly / 7 : 0;
  return {
    weighted: !!shares,
    average: even,
    on: date => (shares ? weekly * shares[dowIndex(date)] : even),
    factorOn: date => (shares ? shares[dowIndex(date)] * 7 : 1)
  };
}

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

function buildGymRow({ gym, daily, targets, basis, cal, dayTarget }){
  // t.daily is weekly ÷ 7 — the average day. `dayTarget.on(date)` is that
  // week split by the gym's own weekday shape, which is what a single day
  // is judged against. Both sum to the same week.
  const t = targets[gym] || { daily: 0, weekly: 0, monthly: 0 };

  const todayActual = sumRange(daily, gym, cal.today, cal.today, basis);
  const weekActual  = sumRange(daily, gym, cal.weekStart, cal.today, basis);
  const monthActual = sumRange(daily, gym, cal.monthStart, cal.today, basis);
  const yesterdayActual = sumRange(daily, gym, cal.yesterday, cal.yesterday, basis);

  const todayTarget = dayTarget.on(cal.today);
  const yesterdayTarget = dayTarget.on(cal.yesterday);

  const trend = [];
  for (let i = TREND_DAYS - 1; i >= 0; i--){
    const date = addDays(cal.today, -i);
    const actual = sumRange(daily, gym, date, date, basis);
    trend.push({
      date,
      pct: round1(pct(actual, dayTarget.on(date))),
      isToday: date === cal.today
    });
  }

  return {
    gym,
    hasAnyTarget: t.weekly > 0 || t.monthly > 0,
    today: {
      targetSet: todayTarget > 0,
      achievedPct: round1(pct(todayActual, todayTarget)),
      expectedPct: round1(cal.todayFraction * 100),
      metAlready: todayTarget > 0 && todayActual >= todayTarget,
      // 1.0 = an average day for this gym; 0.7 = a typically quiet one
      dayFactor: round2(dayTarget.factorOn(cal.today)),
      weighted: dayTarget.weighted
    },
    yesterday: {
      targetSet: yesterdayTarget > 0,
      achievedPct: round1(pct(yesterdayActual, yesterdayTarget))
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

  // Each gym's weekday shape, measured once and reused for its own row and
  // for the roll-up below.
  const dayTargets = {};
  for (const gym of gyms){
    const weekly = (targetSheet.gyms[gym] || {}).weekly || 0;
    const shares = weekly > 0 ? weekdayShares(daily, gym, basis, cal.today) : null;
    dayTargets[gym] = makeDayTarget(weekly, shares);
  }
  const groupDayTarget = date => gyms.reduce((sum, gym) => sum + dayTargets[gym].on(date), 0);

  const rows = gyms.map(gym => buildGymRow({
    gym, daily, targets: targetSheet.gyms, basis, cal, dayTarget: dayTargets[gym]
  }));

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

  // The roll-up's own 14-day trend. It has to be summed here rather than
  // averaged on the client: the per-gym rows carry percentages only, so a
  // client-side mean would weight a small gym the same as a big one.
  const groupTrend = [];
  for (let i = TREND_DAYS - 1; i >= 0; i--){
    const date = addDays(cal.today, -i);
    let actual = 0;
    for (const gym of gyms) actual += sumRange(daily, gym, date, date, basis);
    groupTrend.push({
      date,
      pct: round1(pct(actual, groupDayTarget(date))),
      isToday: date === cal.today
    });
  }

  const groupTodayTarget = groupDayTarget(cal.today);
  const groupYesterdayTarget = groupDayTarget(cal.yesterday);

  const group = {
    gymCount: gyms.length,
    gymsWithTargets: rows.filter(r => r.hasAnyTarget).length,
    trend: groupTrend,
    today: {
      targetSet: groupTodayTarget > 0,
      achievedPct: round1(pct(totals.today, groupTodayTarget)),
      expectedPct: round1(todayFraction * 100),
      metAlready: groupTodayTarget > 0 && totals.today >= groupTodayTarget,
      dayFactor: round2(targetTotals.daily > 0 ? groupTodayTarget / targetTotals.daily : 1),
      weighted: gyms.some(g => dayTargets[g].weighted)
    },
    yesterday: {
      targetSet: groupYesterdayTarget > 0,
      achievedPct: round1(pct(totals.yesterday, groupYesterdayTarget))
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
