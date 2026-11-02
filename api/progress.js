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
const { loadTargets, weekTarget, monthTarget } = require("../lib/targets");
const {
  addDays, toDate, dowIndex, mondayOf, salesMonthOf, salesMonthRange, monthLabel
} = require("../lib/dates");

const KV_GYM_DATA = "gymData";
const TREND_DAYS = 14;

// Rough trading window, used only to work out how much of *today* has gone
// when judging pace. Without it every gym looks catastrophically behind at 9am.
const TRADING_START_HOUR = 6;
const TRADING_END_HOUR = 22;

// How much of a period's target must have come due before a projection is
// worth quoting. A share of target, not a count of days: five percent of a
// week is reached partway through Monday for most gyms, and sooner for one
// whose week front-loads onto it. Below that the sample is too thin and the
// UI says "too early" — today's own percentage and the required-per-day
// figure are both sound from the first hour and carry the gap.
const MIN_EXPECTED_SHARE = 0.05;

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
  // Oldest first, so the window can be trimmed to when this gym opened.
  const samples = [];
  for (let i = DOW_WINDOW_DAYS; i >= 1; i--){
    const date = addDays(today, -i);
    if (!daily[date]) continue;                  // no snapshot for that day at all
    samples.push({ date, value: cellValue(daily[date][gym], basis) });
  }

  // A gym added part-way through the window has structural zeros from before
  // it opened, and counting those drags its medians toward zero — a site added
  // five weeks ago reads as "shut on Mondays". Start at its first day of trade.
  const opened = samples.findIndex(s => s.value > 0);
  if (opened === -1) return null;

  const buckets = [[], [], [], [], [], [], []];
  for (const s of samples.slice(opened)) buckets[dowIndex(s.date)].push(s.value);
  if (buckets.some(b => b.length < DOW_MIN_SAMPLES)) return null;

  const medians = buckets.map(median);
  const total = medians.reduce((a, b) => a + b, 0);
  // A zero median on any weekday would set that day's target to $0 and leave
  // the day untrackable — no percentage, no bar. Far likelier to be a gap in
  // the data than a gym that genuinely never trades on a Tuesday, so hand the
  // whole gym back to the even split rather than publish an unusable target.
  if (!(total > 0) || medians.some(m => m <= 0)) return null;
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

// What share of a period's target has come due by now, given the gym's
// weekday shape: the elapsed days' weight over the whole period's weight,
// counting today by how much of it has traded. Works for a week and a month
// alike, because it is a ratio of weights rather than of dollars — the month's
// target is its own figure, not seven-day arithmetic.
function elapsedTargetShare(dayTarget, fromISO, toISOInclusive, cal){
  let elapsed = 0, total = 0;
  for (let d = fromISO; d <= toISOInclusive; d = addDays(d, 1)){
    const w = dayTarget.factorOn(d);
    total += w;
    if (d < cal.today) elapsed += w;
    else if (d === cal.today) elapsed += w * cal.todayFraction;
  }
  return total > 0 ? elapsed / total : 0;
}

// One period (week or month) reduced to percentages.
//   daysTotal        length of the period in days
//   daysElapsedWhole 1-based index of today within the period
//   todayFraction    how much of today has traded, 0–1
// `expectedSoFar` is how much of the period's target has come due by now —
// the completed days' targets plus today's, pro-rated by how much of today has
// traded. Because the daily targets follow the gym's weekday shape, this is
// what makes a Monday-only week comparable: a gym that front-loads onto
// Monday has more of its week due by Monday night than one that trades at
// the weekend, and gets judged accordingly.
function periodStats({ actual, target, expectedSoFar, daysTotal, daysElapsedWhole }){
  const daysRemainingWhole = daysTotal - daysElapsedWhole;

  const achievedPct = pct(actual, target);
  const expectedPct = pct(expectedSoFar, target);
  const pacePct = expectedSoFar > 0 ? (actual / expectedSoFar) * 100 : null;

  // Where the period lands if this level of performance holds. Projecting
  // against the shape rather than against elapsed days is what lets this work
  // on a Monday: the old flat version divided by a fraction of a day and
  // produced figures in the hundreds of percent, so it had to be suppressed
  // until a full day had passed — which left the week chart empty all Monday.
  //
  // The gate is now a share of the period's target rather than a count of
  // days, so it clears as soon as enough of the week is genuinely due.
  const expectedShare = target > 0 ? expectedSoFar / target : 0;
  const canProject = expectedShare >= MIN_EXPECTED_SHARE;
  const projectedPct = canProject ? pacePct : null;

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
  const effectiveDayTarget = target > 0 ? target / daysTotal : 0;
  const shortfall = Math.max(0, target - actual);
  const daysLeftInclToday = Math.max(1, daysTotal - daysElapsedWhole + 1);
  const requiredDaily = shortfall / daysLeftInclToday;

  return {
    targetSet: target > 0,
    achievedPct: round1(achievedPct),
    expectedPct: round1(expectedPct),
    pacePct: round1(pacePct),
    projectedPct: round1(projectedPct),
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
    // "explicit" when this week was entered, "carried" when it is standing in
    // from an earlier week, "unset" when there is nothing to go on.
    weekTargetSource: t.weeklySource || "unset",
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
      actual: weekActual, target: t.weekly,
      expectedSoFar: t.weekly * elapsedTargetShare(dayTarget, cal.weekStart, cal.weekEnd, cal),
      daysTotal: 7, daysElapsedWhole: cal.weekDayIndex
    }),
    month: periodStats({
      actual: monthActual, target: t.monthly,
      expectedSoFar: t.monthly * elapsedTargetShare(dayTarget, cal.monthStart, cal.monthEnd, cal),
      daysTotal: cal.daysInMonth, daysElapsedWhole: cal.monthDayIndex
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

  // The month here is the sales month — whole Mon–Sun weeks, the month taking
  // each week that has the majority of its days (lib/dates.js). So it runs
  // Mon 2 Nov → Sun 29 Nov rather than 1–30, and its target is simply its
  // weeks added up.
  const weekStart = mondayOf(today);
  const weekDayIndex = dowIndex(today) + 1;       // 1 = Monday … 7 = Sunday
  const salesMonth = salesMonthOf(today);
  const monthRange = salesMonthRange(salesMonth);

  const cal = {
    today,
    yesterday: addDays(today, -1),
    todayFraction,
    weekStart,
    weekEnd: addDays(weekStart, 6),
    weekDayIndex,
    salesMonth,
    salesMonthLabel: monthLabel(salesMonth),
    monthStart: monthRange.start,
    monthEnd: monthRange.end,
    weeksInMonth: monthRange.weeks.length,
    // Which day of the sales month today is, and how long that month runs.
    monthDayIndex: Math.round((toDate(today) - toDate(monthRange.start)) / 864e5) + 1,
    daysInMonth: monthRange.days
  };

  const basis = targetSheet.basis;
  const daily = snapshot.daily;
  const gyms = scopeGyms(session, GYM_LABELS);

  // Each gym's weekday shape, measured once and reused for its own row and
  // for the roll-up below.
  // This week's and this sales month's target per gym, pulled from the week
  // store. `source` says whether the week was entered or carried forward from
  // an earlier one, so the dashboard can be honest about which it is.
  const targets = {};
  for (const gym of gyms){
    const wk = weekTarget(targetSheet, gym, cal.weekStart);
    const mo = monthTarget(targetSheet, gym, cal.salesMonth);
    targets[gym] = {
      weekly: wk.amount,
      weeklySource: wk.source,
      monthly: mo.amount,
      daily: wk.amount > 0 ? wk.amount / 7 : 0
    };
  }

  const dayTargets = {};
  for (const gym of gyms){
    const weekly = targets[gym].weekly;
    const shares = weekly > 0 ? weekdayShares(daily, gym, basis, cal.today) : null;
    dayTargets[gym] = makeDayTarget(weekly, shares);
  }
  const groupDayTarget = date => gyms.reduce((sum, gym) => sum + dayTargets[gym].on(date), 0);

  const rows = gyms.map(gym => buildGymRow({
    gym, daily, targets, basis, cal, dayTarget: dayTargets[gym]
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
    targetTotals.daily   += targets[gym].daily;
    targetTotals.weekly  += targets[gym].weekly;
    targetTotals.monthly += targets[gym].monthly;
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
    // Each gym's own share of its own target, summed — not an average of
    // shares, so a big gym's week counts for more than a small one's.
    week: periodStats({
      actual: totals.week, target: targetTotals.weekly,
      expectedSoFar: gyms.reduce((s, gym) => s +
        targets[gym].weekly *
        elapsedTargetShare(dayTargets[gym], cal.weekStart, cal.weekEnd, cal), 0),
      daysTotal: 7, daysElapsedWhole: cal.weekDayIndex
    }),
    month: periodStats({
      actual: totals.month, target: targetTotals.monthly,
      expectedSoFar: gyms.reduce((s, gym) => s +
        targets[gym].monthly *
        elapsedTargetShare(dayTargets[gym], cal.monthStart, cal.monthEnd, cal), 0),
      daysTotal: cal.daysInMonth, daysElapsedWhole: cal.monthDayIndex
    })
  };

  res.status(200).json({
    role: session.role,
    username: session.username,
    basis,
    calendar: {
      today: cal.today,
      weekStart: cal.weekStart,
      weekEnd: cal.weekEnd,
      weekDayIndex: cal.weekDayIndex,
      salesMonth: cal.salesMonth,
      salesMonthLabel: cal.salesMonthLabel,
      weeksInMonth: cal.weeksInMonth,
      weekOfMonth: Math.floor(Math.round((toDate(cal.weekStart) - toDate(cal.monthStart)) / 864e5) / 7) + 1,
      monthStart: cal.monthStart,
      monthEnd: cal.monthEnd,
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
