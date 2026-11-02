// Sales calendar: Mon–Sun weeks, and months made of whole weeks.
//
// A week belongs to whichever month holds the majority of its days. For a
// Mon–Sun week that is always the month containing its Thursday — Thursday is
// the fourth of seven days, so wherever it falls has at least four of them.
// That is the ISO rule, arrived at from the same place.
//
// So the sales month runs Monday to Sunday, not 1st to last. November 2026 is
// Mon 2 Nov → Sun 29 Nov (four weeks); October 2026 is Mon 28 Sep → Sun 1 Nov
// (five). Every day belongs to exactly one sales month, and no week is split.

// ISO dates are anchored at UTC noon so a DST shift can't roll the date.
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

// 0 = Monday … 6 = Sunday
function dowIndex(iso){
  const d = toDate(iso).getUTCDay();
  return d === 0 ? 6 : d - 1;
}

// The Monday that starts the week containing this date.
function mondayOf(iso){
  return addDays(iso, -dowIndex(iso));
}

// "YYYY-MM" of the sales month a week belongs to: the month holding its
// Thursday, which is the month holding most of its days.
function salesMonthOfWeek(mondayISO){
  return addDays(mondayISO, 3).slice(0, 7);
}

// The sales month a given date falls in — via its week, so a date near a month
// boundary follows its week rather than the calendar.
function salesMonthOf(iso){
  return salesMonthOfWeek(mondayOf(iso));
}

// Every week (by its Monday) in a sales month, in order. Four or five.
function weeksInSalesMonth(ym){
  const [y, m] = ym.split("-").map(Number);
  // Walk from the week holding the 1st; it may belong to the previous month.
  let monday = mondayOf(`${ym}-01`);
  if (salesMonthOfWeek(monday) < ym) monday = addDays(monday, 7);
  const weeks = [];
  while (salesMonthOfWeek(monday) === ym){
    weeks.push(monday);
    monday = addDays(monday, 7);
  }
  return weeks;
}

// First and last day of a sales month, and how many days it spans (28 or 35).
function salesMonthRange(ym){
  const weeks = weeksInSalesMonth(ym);
  if (!weeks.length) return null;
  const start = weeks[0];
  const end = addDays(weeks[weeks.length - 1], 6);
  return { start, end, weeks, days: weeks.length * 7 };
}

// Shift a sales month by n months, e.g. "2026-11" + 1 → "2026-12".
function shiftMonth(ym, n){
  const [y, m] = ym.split("-").map(Number);
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
}

function monthLabel(ym){
  const [y, m] = ym.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, 1)).toLocaleDateString("en-AU", {
    month: "long", year: "numeric", timeZone: "UTC"
  });
}

module.exports = {
  toDate, toISO, addDays, dowIndex, mondayOf,
  salesMonthOfWeek, salesMonthOf, weeksInSalesMonth, salesMonthRange,
  shiftMonth, monthLabel
};
