// scraper_production.js
/**
 * Production-ready scraper for major states/cities using Serper places API.
 * - Save this as scraper_production.js
 * - npm install axios cheerio validator
 * - Set SERPER_API_KEY env var (required)
 *
 * Features:
 *  - concurrency control
 *  - retries with exponential backoff
 *  - UA rotation
 *  - homepage/contact/about fallback scraping
 *  - streaming CSV append with header
 *  - persistent dedupe (stored to disk)
 *  - graceful shutdown
 */

const axios = require("axios");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const cheerio = require("cheerio");
const validator = require("validator");

// ======= CONFIG (via env) =======
const API_KEY = process.env.SERPER_API_KEY || "";
if (!API_KEY) {
  console.error("Error: SERPER_API_KEY environment variable is required.");
  process.exit(1);
}

const OUTPUT_FILE = process.env.OUTPUT_FILE || "businesses.csv";
const DEKUPE_FILE = process.env.DEDUPE_FILE || "dedupe.json"; // persistent dedupe store
const MAX_PAGES = Number(process.env.MAX_PAGES || 2);
const API_CONCURRENCY = Number(process.env.API_CONCURRENCY || 6); // parallel place queries
const SITE_CONCURRENCY = Number(process.env.SITE_CONCURRENCY || 12); // parallel website scrapes
const PER_REQUEST_DELAY_MS = Number(process.env.PER_REQUEST_DELAY_MS || 150);
const REQUEST_TIMEOUT = Number(process.env.REQUEST_TIMEOUT_MS || 15_000);
const SKIP_SCRAPING_IF_NO_WEBSITE =
  process.env.SKIP_SCRAPING_IF_NO_WEBSITE === "true";

// Default states + cities (major ones). You may override by setting STATE_LIST env to JSON string.
const DEFAULT_STATES = {
  California: [
    "Los Angeles",
    "San Francisco",
    "San Diego",
    "San Jose",
    "Sacramento",
  ],
  Texas: ["Houston", "Dallas", "Austin", "San Antonio", "Fort Worth"],
  Florida: ["Miami", "Tampa", "Orlando", "Jacksonville", "Tallahassee"],
  NewYork: ["New York City", "Buffalo", "Rochester", "Albany", "Syracuse"],
  Illinois: ["Chicago", "Aurora", "Naperville", "Joliet", "Rockford"],
  Arizona: ["Phoenix", "Tucson", "Mesa", "Scottsdale", "Chandler"],
};

let STATES;
try {
  STATES = process.env.STATE_LIST
    ? JSON.parse(process.env.STATE_LIST)
    : DEFAULT_STATES;
} catch (err) {
  console.warn("STATE_LIST env invalid JSON — using defaults.");
  STATES = DEFAULT_STATES;
}

// Business types to search (tweak if desired)
const BUSINESS_TYPES = [
  "restaurants",
  "coffee shops",
  "hair salons",
  "dentist offices",
  "auto repair shops",
  "bakeries",
  "pharmacies",
  "bookstores",
  "florists",
  "veterinary clinics",
  "shoe stores",
];

// ======= Helpers =======
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}
function md5(s) {
  return crypto.createHash("md5").update(s).digest("hex");
}
function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const s = String(value);
  if (s.includes('"') || s.includes(",") || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

// ======= User Agents (rotate) =======
const UAS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.5938.92 Safari/537.36",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
];
let uaIndex = 0;
function nextUA() {
  uaIndex = (uaIndex + 1) % UAS.length;
  return UAS[uaIndex];
}

// ======= HTTP clients =======
const apiClient = axios.create({
  timeout: REQUEST_TIMEOUT,
  headers: { "Content-Type": "application/json" },
});
const siteClient = axios.create({ timeout: REQUEST_TIMEOUT, maxRedirects: 5 });

// ======= CSV streaming utilities =======
function ensureCsvHeader() {
  const headers = ["name", "website", "email", "businessType", "city", "state"];
  if (!fs.existsSync(OUTPUT_FILE) || fs.statSync(OUTPUT_FILE).size === 0) {
    fs.writeFileSync(
      OUTPUT_FILE,
      headers.map(csvEscape).join(",") + "\n",
      "utf8"
    );
  }
}
function appendRows(rows) {
  if (!rows || rows.length === 0) return;
  const lines = rows
    .map((r) =>
      [
        csvEscape(r.name),
        csvEscape(r.website || ""),
        csvEscape(r.email || ""),
        csvEscape(r.businessType || ""),
        csvEscape(r.city || ""),
        csvEscape(r.state || ""),
      ].join(",")
    )
    .join("\n");
  fs.appendFileSync(OUTPUT_FILE, lines + "\n", "utf8");
}

// ======= Persistent dedupe (simple disk-backed JSON of MD5 keys) =======
let dedupeSet = new Set();
function loadDedupe() {
  try {
    if (fs.existsSync(DEKUPE_FILE)) {
      const raw = fs.readFileSync(DEKUPE_FILE, "utf8");
      const arr = JSON.parse(raw);
      arr.forEach((k) => dedupeSet.add(k));
      console.log(`Loaded ${dedupeSet.size} keys from ${DEKUPE_FILE}`);
    }
  } catch (err) {
    console.warn("Failed loading dedupe file:", err.message);
  }
}
function persistDedupe() {
  try {
    fs.writeFileSync(DEKUPE_FILE, JSON.stringify([...dedupeSet]), "utf8");
  } catch (err) {
    console.warn("Failed persisting dedupe file:", err.message);
  }
}

// Persist dedupe periodically
setInterval(() => {
  persistDedupe();
}, 60_000);

// ======= Serper Places call with retries/backoff =======
async function fetchPlaces(
  city,
  state,
  businessType,
  page = 1,
  retries = 3,
  delay = 1000
) {
  const payload = JSON.stringify([
    {
      q: `${businessType} in ${city}`,
      location: `${city}, ${state}, United States`,
      num: 30,
      page,
    },
  ]);
  try {
    const res = await apiClient.post(
      "https://google.serper.dev/places",
      payload,
      {
        headers: { "X-API-KEY": API_KEY, "Content-Type": "application/json" },
      }
    );
    return res.data?.[0]?.places || [];
  } catch (err) {
    if (retries > 0) {
      console.warn(
        `fetchPlaces error for ${city}/${businessType} page ${page}: ${err.message}. Retrying in ${delay}ms (${retries} left).`
      );
      await sleep(delay);
      return fetchPlaces(
        city,
        state,
        businessType,
        page,
        retries - 1,
        delay * 2
      );
    }
    console.error(
      `fetchPlaces permanently failed for ${city}/${businessType} page ${page}: ${err.message}`
    );
    return [];
  }
}

// ======= Email extraction from HTML =======
function extractEmailsFromHtml(html) {
  const $ = cheerio.load(html || "");
  const emails = new Set();
  $('a[href^="mailto:"]').each((i, el) => {
    const href = $(el).attr("href");
    if (!href) return;
    const em = href
      .replace(/^mailto:/i, "")
      .split("?")[0]
      .trim();
    if (validator.isEmail(em)) emails.add(em.toLowerCase());
  });
  const text = $("body").text() || "";
  const regex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const found = text.match(regex) || [];
  for (const e of found) {
    if (validator.isEmail(e)) emails.add(e.toLowerCase());
  }
  return Array.from(emails);
}

// ======= Normalize URL & fetch with UA =======
function normalizeUrl(u) {
  if (!u) return null;
  const t = u.trim();
  if (!/^https?:\/\//i.test(t)) return "http://" + t;
  return t;
}
async function fetchHtmlWithUA(url) {
  try {
    const ua = nextUA();
    const res = await siteClient.get(url, {
      headers: {
        "User-Agent": ua,
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
      validateStatus: (s) => s >= 200 && s < 400,
    });
    return res.data;
  } catch {
    return null;
  }
}

// ======= Scrape site + fallback contact/about pages =======
const SITE_PATHS = [
  "/",
  "/contact",
  "/contact-us",
  "/about",
  "/about-us",
  "/company",
  "/info",
];
async function scrapeSiteForEmails(website) {
  if (!website) return [];
  const root = normalizeUrl(website);
  const tried = new Set();
  for (const p of SITE_PATHS) {
    try {
      const url = new URL(p, root).toString();
      if (tried.has(url)) continue;
      tried.add(url);
      const html = await fetchHtmlWithUA(url);
      if (!html) continue;
      const emails = extractEmailsFromHtml(html);
      if (emails.length > 0) return emails;
      await sleep(120); // small delay between site fetches
    } catch {
      // skip invalid URL or network error
      continue;
    }
  }
  return [];
}

// ======= Async pool (concurrency) =======
async function asyncPool(limit, tasks = [], handler) {
  const results = [];
  const executing = new Set();
  for (const t of tasks) {
    const p = Promise.resolve().then(() => handler(t));
    results.push(p);
    executing.add(p);
    const onDone = () => executing.delete(p);
    p.then(onDone).catch(onDone);
    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  return Promise.all(results);
}

// ======= Graceful shutdown =======
let shuttingDown = false;
process.on("SIGINT", () => {
  console.log(
    "SIGINT received — will shutdown gracefully after current tasks finish..."
  );
  shuttingDown = true;
});
process.on("SIGTERM", () => {
  console.log(
    "SIGTERM received — will shutdown gracefully after current tasks finish..."
  );
  shuttingDown = true;
});

// ======= Main pipeline =======
async function main() {
  ensureCsvHeader();
  loadDedupe();

  // build task list
  const cityTuples = [];
  for (const [state, cityList] of Object.entries(STATES)) {
    for (const city of cityList) cityTuples.push({ city, state });
  }
  const totalTasks = cityTuples.length * BUSINESS_TYPES.length;
  let completedTasks = 0;
  const start = Date.now();

  for (const { city, state } of cityTuples) {
    if (shuttingDown) break;
    console.log(`\n=== Processing city: ${city}, ${state} ===`);
    for (const businessType of BUSINESS_TYPES) {
      if (shuttingDown) break;
      completedTasks++;
      const elapsed = (Date.now() - start) / 1000;
      const tps = completedTasks / Math.max(1, elapsed);
      const remaining = totalTasks - completedTasks;
      const eta = isFinite(tps)
        ? new Date(Math.round((remaining / tps) * 1000))
            .toISOString()
            .substr(11, 8)
        : "Calculating";
      console.log(
        `[${completedTasks}/${totalTasks}] ${businessType} in ${city}, ${state} — ETA ${eta}`
      );

      // small spacing between API calls
      await sleep(PER_REQUEST_DELAY_MS);

      // fetch pages
      let places = [];
      for (let page = 1; page <= MAX_PAGES; page++) {
        if (shuttingDown) break;
        console.log(`  fetching page ${page}...`);
        const pagePlaces = await fetchPlaces(city, state, businessType, page);
        if (!pagePlaces || pagePlaces.length === 0) {
          if (page === 1) console.log("  no results or error for first page.");
          break;
        }
        places.push(...pagePlaces);
        await sleep(120);
      }

      console.log(`  fetched ${places.length} candidate places.`);

      // create tasks for each place
      const tasks = places.map((place) => async () => {
        if (shuttingDown) return null;
        // normalize name
        const name = (place.title || place.name || "").trim() || "Unknown";
        const website = place.website || place.url || "";
        const key = md5(`${name}|${city}|${state}`);
        if (dedupeSet.has(key)) {
          // skip
          return null;
        }

        // primary email if present in API
        let emails = [];
        try {
          if (place.email && validator.isEmail(place.email)) {
            emails = [place.email.toLowerCase()];
          } else if (website && !SKIP_SCRAPING_IF_NO_WEBSITE) {
            emails = await scrapeSiteForEmails(website);
          } else {
            // no website -> keep emails empty
          }
        } catch (err) {
          emails = [];
        }
        if (emails.length === 0) emails = [""]; // maintain shape

        const rows = emails.map((em) => ({
          name,
          website: website || "",
          email: em || "",
          businessType,
          city,
          state,
        }));

        dedupeSet.add(key);
        return rows;
      });

      // run place tasks with concurrency (site scrapes)
      const results = await asyncPool(SITE_CONCURRENCY, tasks, (t) => t());
      // flatten results and append
      const newRows = [];
      for (const r of results) {
        if (Array.isArray(r) && r.length) newRows.push(...r);
      }

      if (newRows.length > 0) {
        appendRows(newRows);
        console.log(
          `  appended ${newRows.length} rows (dedupe total ≈ ${dedupeSet.size})`
        );
      } else {
        console.log("  no new rows for this batch.");
      }

      // small pause between business types to reduce burstiness
      await sleep(100);
    } // end businessTypes
  } // end cities

  // final persist dedupe
  persistDedupe();
  console.log("\n✅ Scraping run complete.");
  console.log(`Total deduped entries: ${dedupeSet.size}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  persistDedupe();
  process.exit(1);
});
