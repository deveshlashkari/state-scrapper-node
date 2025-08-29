/**
 * scraper_production.js
 *
 * Production-ready scraper:
 * - Primary source: YellowPages (structured)
 * - Fallback: Serper Places API (requires SERPER_API_KEY env secret)
 * - Visits websites to extract emails (cheerio + validator)
 * - Streaming CSV append, persistent dedupe (dedupe.json)
 * - Retries, exponential backoff, timeouts
 * - ETA and progress logging
 *
 * Usage:
 *   export SERPER_API_KEY=xxxxx
 *   node scraper_production.js
 *
 * Notes:
 * - Running on GitHub Actions: set repository secret SERPER_API_KEY
 * - Large runs may take long; this is safe to run in scheduled CI
 */

const axios = require("axios");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const cheerio = require("cheerio");
const validator = require("validator");

// ---------------- CONFIG ----------------
const API_KEY = process.env.SERPER_API_KEY || ""; // optional but recommended for fallback
const OUTPUT_FILE = process.env.OUTPUT_FILE || "businesses.csv";
const DEDUPE_FILE = process.env.DEDUPE_FILE || "dedupe.json";
const MAX_PAGES = Number(process.env.MAX_PAGES || 2); // pages per query - YP uses page param
const SITE_CONCURRENCY = Number(process.env.SITE_CONCURRENCY || 8);
const API_CONCURRENCY = Number(process.env.API_CONCURRENCY || 4);
const REQUEST_TIMEOUT = Number(process.env.REQUEST_TIMEOUT_MS || 15000);
const PER_REQUEST_DELAY_MS = Number(process.env.PER_REQUEST_DELAY_MS || 150);
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 3);

// ---------------- User-agent rotation ----------------
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.5938.92 Safari/537.36",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
];
let uaIndex = 0;
function nextUA() {
  uaIndex = (uaIndex + 1) % USER_AGENTS.length;
  return USER_AGENTS[uaIndex];
}

// ---------------- States & cities (50 states, each with some major cities) ----------------
// This list targets major population centers, not every town.
const STATE_CITY_MAP = {
  Alabama: ["Birmingham", "Montgomery", "Mobile"],
  Alaska: ["Anchorage", "Fairbanks"],
  Arizona: ["Phoenix", "Tucson", "Mesa"],
  Arkansas: ["Little Rock", "Fayetteville"],
  California: [
    "Los Angeles",
    "San Francisco",
    "San Diego",
    "Sacramento",
    "San Jose",
  ],
  Colorado: ["Denver", "Colorado Springs", "Aurora"],
  Connecticut: ["Bridgeport", "New Haven", "Hartford"],
  Delaware: ["Wilmington", "Dover"],
  Florida: ["Miami", "Orlando", "Tampa", "Jacksonville"],
  Georgia: ["Atlanta", "Savannah", "Augusta"],
  Hawaii: ["Honolulu"],
  Idaho: ["Boise", "Idaho Falls"],
  Illinois: ["Chicago", "Aurora", "Naperville"],
  Indiana: ["Indianapolis", "Fort Wayne"],
  Iowa: ["Des Moines", "Cedar Rapids"],
  Kansas: ["Wichita", "Overland Park"],
  Kentucky: ["Louisville", "Lexington"],
  Louisiana: ["New Orleans", "Baton Rouge"],
  Maine: ["Portland"],
  Maryland: ["Baltimore", "Annapolis"],
  Massachusetts: ["Boston", "Worcester"],
  Michigan: ["Detroit", "Grand Rapids", "Ann Arbor"],
  Minnesota: ["Minneapolis", "Saint Paul"],
  Mississippi: ["Jackson"],
  Missouri: ["St. Louis", "Kansas City"],
  Montana: ["Billings"],
  Nebraska: ["Omaha", "Lincoln"],
  Nevada: ["Las Vegas", "Reno"],
  NewHampshire: ["Manchester"],
  NewJersey: ["Newark", "Jersey City"],
  NewMexico: ["Albuquerque", "Santa Fe"],
  NewYork: ["New York City", "Buffalo", "Rochester"],
  NorthCarolina: ["Charlotte", "Raleigh", "Durham"],
  NorthDakota: ["Fargo"],
  Ohio: ["Columbus", "Cleveland", "Cincinnati"],
  Oklahoma: ["Oklahoma City", "Tulsa"],
  Oregon: ["Portland", "Eugene"],
  Pennsylvania: ["Philadelphia", "Pittsburgh", "Harrisburg"],
  RhodeIsland: ["Providence"],
  SouthCarolina: ["Charleston", "Columbia"],
  SouthDakota: ["Sioux Falls"],
  Tennessee: ["Nashville", "Memphis", "Knoxville"],
  Texas: ["Houston", "Dallas", "Austin", "San Antonio"],
  Utah: ["Salt Lake City", "Provo"],
  Vermont: ["Burlington"],
  Virginia: ["Richmond", "Virginia Beach"],
  Washington: ["Seattle", "Spokane"],
  WestVirginia: ["Charleston"],
  Wisconsin: ["Milwaukee", "Madison"],
  Wyoming: ["Cheyenne"],
};

// ---------------- Business categories (expanded) ----------------
const BUSINESS_TYPES = [
  "restaurants",
  "coffee shops",
  "bakeries",
  "hair salons",
  "barbershops",
  "dentist offices",
  "doctors clinics",
  "pharmacies",
  "veterinary clinics",
  "pet shops",
  "shoe stores",
  "auto repair shops",
  "plumbers",
  "electricians",
  "dry cleaners",
  "florists",
  "bookstores",
  "jewelry stores",
  "lawyers",
  "accountants",
];

// ---------------- Utilities ----------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function md5(s) {
  return crypto.createHash("md5").update(s).digest("hex");
}
function csvEscape(v) {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (s.includes('"') || s.includes(",") || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

// ---------------- HTTP clients ----------------
const apiClient = axios.create({ timeout: REQUEST_TIMEOUT });
const siteClient = axios.create({ timeout: REQUEST_TIMEOUT, maxRedirects: 5 });

// ---------------- CSV streaming & dedupe ----------------
function ensureCsvHeader() {
  if (!fs.existsSync(OUTPUT_FILE) || fs.statSync(OUTPUT_FILE).size === 0) {
    const headers = [
      "name",
      "phone",
      "website",
      "email",
      "businessType",
      "city",
      "state",
    ];
    fs.writeFileSync(
      OUTPUT_FILE,
      headers.map(csvEscape).join(",") + "\n",
      "utf8"
    );
  }
}
function appendRows(rows) {
  if (!rows || rows.length === 0) return;
  const lines =
    rows
      .map((r) =>
        [
          csvEscape(r.name || ""),
          csvEscape(r.phone || ""),
          csvEscape(r.website || ""),
          csvEscape(r.email || ""),
          csvEscape(r.businessType || ""),
          csvEscape(r.city || ""),
          csvEscape(r.state || ""),
        ].join(",")
      )
      .join("\n") + "\n";
  fs.appendFileSync(OUTPUT_FILE, lines, "utf8");
}

let dedupeSet = new Set();
function loadDedupe() {
  try {
    if (fs.existsSync(DEDUPE_FILE)) {
      const data = fs.readFileSync(DEDUPE_FILE, "utf8");
      const arr = JSON.parse(data);
      arr.forEach((x) => dedupeSet.add(x));
      console.log(`Loaded ${dedupeSet.size} dedupe keys.`);
    }
  } catch (e) {
    console.warn("Could not load dedupe file:", e.message);
  }
}
function persistDedupe() {
  try {
    fs.writeFileSync(DEDUPE_FILE, JSON.stringify([...dedupeSet]), "utf8");
  } catch (e) {
    console.warn("Could not persist dedupe file:", e.message);
  }
}
setInterval(persistDedupe, 60_000);

// ---------------- Networking helpers (retries/backoff) ----------------
async function requestWithRetries(config, retries = MAX_RETRIES, delay = 1000) {
  try {
    const res = await axios(config);
    return res;
  } catch (err) {
    if (retries > 0) {
      await sleep(delay);
      return requestWithRetries(config, retries - 1, delay * 2);
    }
    throw err;
  }
}

// ---------------- YellowPages scraping (structured) ----------------
/**
 * Scrape YellowPages search results for query & location.
 * Returns array of { name, phone, website }.
 */
async function fetchYellowPages(query, city, state, page = 1) {
  const geo = `${city}, ${state}`;
  const url = `https://www.yellowpages.com/search?search_terms=${encodeURIComponent(
    query
  )}&geo_location_terms=${encodeURIComponent(geo)}&page=${page}`;
  // Use Serper proxy? If SERPER not provided, directly fetch (may get blocked)
  const useProxy = Boolean(API_KEY);
  const finalUrl = useProxy
    ? `https://api.allorigins.win/raw?url=${encodeURIComponent(url)}`
    : url;
  // NOTE: allorigins is a free CORS proxy — not guaranteed; if you have ScraperAPI/other proxy, replace here.
  try {
    const res = await requestWithRetries({
      method: "get",
      url: finalUrl,
      timeout: REQUEST_TIMEOUT,
      headers: { "User-Agent": nextUA() },
    });
    const html = res.data;
    const $ = cheerio.load(html);
    const results = [];
    $(".result").each((i, el) => {
      const name =
        $(el).find(".business-name span").text().trim() ||
        $(el).find(".business-name").text().trim();
      const phone = $(el).find(".phones").first().text().trim();
      // website link sometimes is direct, sometimes via track-visit-website
      let website = $(el).find("a.track-visit-website").attr("href") || "";
      if (website && website.startsWith("/biz_redirect?")) {
        // YellowPages sometimes uses redirect links; try to pick data-visit
        website =
          $(el).find("a.track-visit-website").attr("data-visit") || website;
      }
      website = website ? website.trim() : "";
      if (name) {
        results.push({ name, phone, website });
      }
    });
    const hasNext = $(".pagination a.next").length > 0;
    return { results, hasNext };
  } catch (err) {
    // graceful fallback
    // console.warn(`YP fetch failed for ${city}, ${state}: ${err.message}`);
    return { results: [], hasNext: false };
  }
}

// ---------------- Serper Places fallback ----------------
async function fetchSerperPlaces(businessType, city, state, page = 1) {
  if (!API_KEY) return [];
  const payload = JSON.stringify([
    {
      q: `${businessType} in ${city}`,
      location: `${city}, ${state}, United States`,
      num: 30,
      page,
    },
  ]);
  try {
    const res = await requestWithRetries(
      {
        method: "post",
        url: "https://google.serper.dev/places",
        data: payload,
        headers: { "Content-Type": "application/json", "X-API-KEY": API_KEY },
      },
      MAX_RETRIES,
      1000
    );
    return res.data?.[0]?.places || [];
  } catch (err) {
    return [];
  }
}

// ---------------- Website scraping & email extraction ----------------
function extractEmailsFromHtml(html) {
  const $ = cheerio.load(html || "");
  const found = new Set();
  $('a[href^="mailto:"]').each((i, el) => {
    const m = $(el)
      .attr("href")
      .replace(/^mailto:/i, "")
      .split("?")[0]
      .trim();
    if (validator.isEmail(m)) found.add(m.toLowerCase());
  });
  const text = $("body").text() || "";
  const re = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const matches = text.match(re) || [];
  matches.forEach((m) => {
    if (validator.isEmail(m)) found.add(m.toLowerCase());
  });
  return Array.from(found);
}
async function fetchHtmlUrl(url) {
  const normalized = normalizeUrl(url);
  if (!normalized) return null;
  try {
    const res = await requestWithRetries(
      {
        method: "get",
        url: normalized,
        timeout: REQUEST_TIMEOUT,
        headers: { "User-Agent": nextUA() },
      },
      MAX_RETRIES,
      800
    );
    return res.data;
  } catch {
    return null;
  }
}
function normalizeUrl(u) {
  if (!u) return null;
  const t = u.trim();
  if (!/^https?:\/\//i.test(t)) return "http://" + t;
  return t;
}
async function scrapeSiteForEmails(website) {
  if (!website) return [];
  const tried = new Set();
  const paths = [
    "/",
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/company",
    "/info",
  ];
  for (const p of paths) {
    try {
      const url = new URL(p, normalizeUrl(website)).toString();
      if (tried.has(url)) continue;
      tried.add(url);
      const html = await fetchHtmlUrl(url);
      if (!html) continue;
      const emails = extractEmailsFromHtml(html);
      if (emails.length > 0) return emails;
      await sleep(120);
    } catch {
      continue;
    }
  }
  return [];
}

// ---------------- Async pool (concurrency limiter) ----------------
async function asyncPool(limit, items, iteratorFn) {
  const results = [];
  const executing = new Set();
  for (const item of items) {
    const p = Promise.resolve().then(() => iteratorFn(item));
    results.push(p);
    executing.add(p);
    const clean = () => executing.delete(p);
    p.then(clean).catch(clean);
    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  return Promise.all(results);
}

// ---------------- Progress / Stats ----------------
let stats = {
  tasksTotal: 0,
  tasksCompleted: 0,
  websitesFetched: 0,
  emailsFound: 0,
  rowsAppended: 0,
};

function printProgress() {
  const elapsed = (Date.now() - startTime) / 1000;
  const tps = stats.tasksCompleted / Math.max(1, elapsed);
  const remaining = Math.max(0, stats.tasksTotal - stats.tasksCompleted);
  const eta =
    tps > 0
      ? new Date(Math.round((remaining / tps) * 1000))
          .toISOString()
          .substr(11, 8)
      : "Calculating";
  console.log(
    `\n=== PROGRESS: ${stats.tasksCompleted}/${stats.tasksTotal} tasks — ETA ${eta} ===`
  );
  console.log(
    `websites fetched: ${stats.websitesFetched} | emails found: ${stats.emailsFound} | rows appended: ${stats.rowsAppended}`
  );
}

// ---------------- Graceful shutdown ----------------
let shuttingDown = false;
process.on("SIGINT", () => {
  console.log("SIGINT received — finishing current work then exit");
  shuttingDown = true;
});
process.on("SIGTERM", () => {
  console.log("SIGTERM received — finishing current work then exit");
  shuttingDown = true;
});

// ---------------- Main pipeline ----------------
ensureCsvHeader();
loadDedupe();

const cityTuples = [];
for (const [state, cities] of Object.entries(STATE_CITY_MAP)) {
  for (const city of cities) cityTuples.push({ city, state });
}
stats.tasksTotal = cityTuples.length * BUSINESS_TYPES.length;
const startTime = Date.now();

(async () => {
  try {
    for (const { city, state } of cityTuples) {
      if (shuttingDown) break;
      console.log(`\n--- Processing ${city}, ${state} ---`);
      for (const businessType of BUSINESS_TYPES) {
        if (shuttingDown) break;
        console.log(`\n[Task] ${businessType} in ${city}, ${state}`);
        // small throttling between tasks
        await sleep(PER_REQUEST_DELAY_MS);

        // 1) Primary: YellowPages structured search across pages
        let places = [];
        for (let page = 1; page <= MAX_PAGES; page++) {
          const { results, hasNext } = await fetchYellowPages(
            businessType,
            city,
            state,
            page
          );
          if (results && results.length) {
            places.push(...results);
          }
          if (!hasNext) break;
          await sleep(120);
        }

        // 2) Fallback: Serper Places (if no YP results)
        if (places.length === 0 && API_KEY) {
          const serperPlaces = await fetchSerperPlaces(
            businessType,
            city,
            state,
            1
          );
          // serper places have different shape; normalize
          for (const p of serperPlaces) {
            places.push({
              name: p.title || p.name || "",
              phone: p.phone || "",
              website: p.website || p.url || "",
              _serper_raw: p,
            });
          }
        }

        console.log(`  Candidate places: ${places.length}`);

        // Build per-place tasks that visit website & extract emails
        const placeTasks = places.map((pl) => async () => {
          if (shuttingDown) return null;
          // Normalize fields
          const name = (pl.name || pl.title || "").trim() || "Unknown";
          const phone = pl.phone || "";
          const website = (pl.website || pl.url || "").trim();
          const key = md5(`${name}|${city}|${state}`);
          if (dedupeSet.has(key)) return null;

          // Try to get emails: first from Serper if present, else scrape site
          let emails = [];
          try {
            // if pl has _serper_raw and email inside
            if (
              pl._serper_raw &&
              pl._serper_raw.email &&
              validator.isEmail(pl._serper_raw.email)
            ) {
              emails = [pl._serper_raw.email.toLowerCase()];
            } else if (website) {
              const found = await scrapeSiteForEmails(website);
              if (found && found.length) emails = found;
              stats.websitesFetched++;
            }
          } catch (e) {
            // ignore site-level errors
          }

          if (!emails || emails.length === 0) emails = [""];

          // produce rows
          const rows = emails.map((em) => ({
            name,
            phone,
            website,
            email: em || "",
            businessType,
            city,
            state,
          }));

          // mark dedupe
          dedupeSet.add(key);
          return rows;
        });

        // Run place tasks with concurrency
        const results = await asyncPool(SITE_CONCURRENCY, placeTasks, (t) =>
          t()
        );
        const newRows = [];
        for (const r of results) {
          if (Array.isArray(r) && r.length) {
            newRows.push(...r);
          }
        }

        if (newRows.length > 0) {
          appendRows(newRows);
          stats.rowsAppended += newRows.length;
          const foundEmails = newRows.filter(
            (r) => r.email && r.email.length > 0
          ).length;
          stats.emailsFound += foundEmails;
          console.log(
            `  Appended ${newRows.length} rows (emails found: ${foundEmails})`
          );
        } else {
          console.log("  No new rows for this batch.");
        }

        stats.tasksCompleted++;
        printProgress();

        // small pause to reduce burstiness
        await sleep(120);
      } // end business types
      // persist dedupe after each city
      persistDedupe();
    } // end cities
  } catch (err) {
    console.error("Fatal pipeline error:", err);
  } finally {
    persistDedupe();
    console.log("\n✅ Scrape finished. Final stats:", stats);
  }
})();
