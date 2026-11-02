// Single source of truth for the gym roster. Both the Shopify refresh cron
// (which maps Shopify location names onto dashboard labels) and the targets /
// progress endpoints read from here, so adding a gym is a one-line change.
//
// Map: Shopify location name (matched by substring, case-insensitive) →
// the dashboard label that should appear. Online orders or in-store orders
// at locations not in this map are skipped.
const GYM_LOCATION_MAP = [
  ["flinders park", "Flinders Park, SA"],
  ["tranmere",      "Tranmere, SA"],
  ["midvale",       "Midvale, WA"],
  ["malaga",        "Malaga, WA"],
  ["alberton",      "Alberton, SA"],
  ["hackham",       "Hackham, SA"],
  ["wanneroo",      "Wanneroo, WA"],
  ["maddington",    "Maddington, WA"],
  ["munno para",    "Munno Para, SA"],
  ["rockingham",    "Rockingham, WA"],
  ["mandurah",      "Mandurah, WA"],
  ["beckenham",     "Beckenham, WA"],
  // Trades as "The Lodge"; Cin7/Shopify still carry the site as Brahma
  // Lodge, so match on "lodge" to cover the name under either spelling.
  ["lodge",         "The Lodge, SA"],
];

const GYM_LABELS = GYM_LOCATION_MAP.map(([, label]) => label).sort();

function mapLocation(name){
  const lower = (name || "").toLowerCase();
  for (const [needle, label] of GYM_LOCATION_MAP){
    if (lower.includes(needle)) return label;
  }
  return null;
}

module.exports = { GYM_LOCATION_MAP, GYM_LABELS, mapLocation };
