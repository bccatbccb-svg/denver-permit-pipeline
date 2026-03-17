/**
 * Denver Metro eTRAKiT Permit Scraper
 * ------------------------------------
 * Crawls an eTRAKiT permit portal, filters for custom home builds
 * and remodels, and pushes results to the Apify dataset in the
 * unified permit pipeline schema.
 *
 * Works against any eTRAKiT instance — pass a different baseUrl
 * in the input to run against Lakewood, Westminster, etc.
 *
 * eTRAKiT search flow:
 *   1. GET  /Search/permit               → search form
 *   2. POST /Search/permit  (date range + permit type filter)
 *   3. GET  /Search/permit?page=N        → paginate results table
 *   4. GET  /Permit/permit/{id}          → detail page (contractor etc.)
 */

import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset, log } from 'crawlee';
import { chromium } from 'playwright';

await Actor.init();

// ── Input ─────────────────────────────────────────────────────────────────────

const input = await Actor.getInput() ?? {};

const {
  baseUrl            = 'https://arvadapermits.org/etrakit3',
  cityName           = 'Arvada',
  daysBack           = 30,
  minValuation       = 50000,
  permitTypeKeywords = ['NEW SINGLE FAMILY', 'NEW SFR', 'CUSTOM HOME',
                        'SINGLE FAMILY DETACHED', 'ADDITION', 'REMODEL',
                        'ADU', 'ACCESSORY DWELLING'],
  maxPages           = 0,
  proxyConfiguration = { useApifyProxy: true },
} = input;

// Derived values
const searchUrl  = `${baseUrl}/Search/permit`;
const detailBase = `${baseUrl}/Permit/permit`;
const fromDate   = daysAgo(daysBack);
const toDate     = today();

log.info(`Starting eTRAKiT scrape`, { city: cityName, baseUrl, fromDate, toDate, minValuation });

// ── Proxy ─────────────────────────────────────────────────────────────────────

const proxy = await Actor.createProxyConfiguration(proxyConfiguration);

// ── State — track pages and seen permit IDs ───────────────────────────────────

const seen    = new Set();
let pageCount = 0;
let hitCount  = 0;

// ── Crawler ───────────────────────────────────────────────────────────────────

const crawler = new PlaywrightCrawler({

  // Use a consistent browser context so session/cookies persist across requests
  launchContext: {
    launcher: chromium,
    launchOptions: { headless: true },
  },

  proxyConfiguration: proxy,

  // eTRAKiT pages are slow — give them time
  requestHandlerTimeoutSecs: 60,
  navigationTimeoutSecs:     45,

  // Don't hammer the portal — be polite
  minConcurrency: 1,
  maxConcurrency: 2,
  requestDelay:   800,

  async requestHandler({ page, request, enqueueLinks, log }) {
    const { label } = request.userData ?? {};

    if (label === 'DETAIL') {
      await handleDetailPage({ page, request, log });
    } else {
      await handleSearchPage({ page, request, enqueueLinks, log });
    }
  },

  failedRequestHandler({ request, log }) {
    log.error(`Request failed after retries`, { url: request.url });
  },
});

// ── Search / results page handler ─────────────────────────────────────────────

async function handleSearchPage({ page, request, enqueueLinks, log }) {
  const isFirstPage = !request.userData?.page;

  if (isFirstPage) {
    // Fill the search form on first visit
    log.info('Filling search form...');
    await fillSearchForm(page, fromDate, toDate, permitTypeKeywords);
    await page.waitForSelector('table.k-grid-table, .t-grid-content table, #permitSearchResults', {
      timeout: 15000,
    }).catch(() => log.warning('Results table selector not found — page structure may differ'));
  }

  // Extract all permit rows from the results table
  const rows = await extractResultRows(page);
  log.info(`Found ${rows.length} rows on page`, { url: request.url });

  pageCount++;

  for (const row of rows) {
    if (seen.has(row.permitNumber)) continue;
    seen.add(row.permitNumber);

    // Apply keyword filter if eTRAKiT didn't filter server-side
    if (!isTargetPermit(row, permitTypeKeywords)) continue;

    // Apply valuation filter
    if (minValuation > 0 && row.valuation < minValuation) continue;

    hitCount++;

    // Enqueue the detail page to get contractor info
    if (row.detailUrl) {
      await enqueueLinks({
        urls: [row.detailUrl],
        userData: { label: 'DETAIL', partialRecord: row },
        label: 'DETAIL',
      });
    } else {
      // No detail URL — push what we have
      await Dataset.pushData(toUnifiedSchema(row, cityName, null));
    }
  }

  // Paginate — eTRAKiT uses a "next page" link or query param
  if (maxPages === 0 || pageCount < maxPages) {
    const nextUrl = await getNextPageUrl(page, baseUrl, pageCount);
    if (nextUrl) {
      await enqueueLinks({
        urls: [nextUrl],
        userData: { label: 'SEARCH', page: pageCount + 1 },
      });
    }
  }
}

// ── Detail page handler ───────────────────────────────────────────────────────

async function handleDetailPage({ page, request, log }) {
  const { partialRecord } = request.userData;
  log.debug('Scraping detail page', { permitNumber: partialRecord?.permitNumber });

  const detail = await extractDetailFields(page);
  const record = toUnifiedSchema(partialRecord, cityName, detail);

  await Dataset.pushData(record);
}

// ── eTRAKiT form interaction ──────────────────────────────────────────────────

/**
 * Fill the eTRAKiT search form.
 * eTRAKiT uses a Kendo UI grid with date pickers and a permit type dropdown.
 * Field selectors vary slightly between hosted instances — this covers the
 * most common patterns. Run with headless:false locally to inspect if selectors fail.
 */
async function fillSearchForm(page, fromDate, toDate, keywords) {
  await page.goto(`${baseUrl}/Search/permit`, { waitUntil: 'networkidle' });

  // ── Date range ──
  // Try common date field patterns across eTRAKiT versions
  const fromSelectors = [
    'input[name="IssuedDateFrom"]',
    '#IssuedDateFrom',
    'input[placeholder*="From"]',
    '.date-from input',
  ];
  const toSelectors = [
    'input[name="IssuedDateTo"]',
    '#IssuedDateTo',
    'input[placeholder*="To"]',
    '.date-to input',
  ];

  await fillField(page, fromSelectors, fromDate);
  await fillField(page, toSelectors, toDate);

  // ── Permit type — leave blank to get all, then filter client-side ──
  // eTRAKiT's type dropdown varies too much between instances.
  // Safer to pull all permits and filter on our side.
  // If you want server-side filtering, uncomment and adjust:
  //
  // await page.selectOption('select[name="PermitType"]', { label: 'Residential' })
  //   .catch(() => {});

  // ── Submit ──
  const submitSelectors = [
    'button[type="submit"]',
    'input[type="submit"]',
    '#searchButton',
    '.search-btn',
    'button:has-text("Search")',
  ];
  for (const sel of submitSelectors) {
    const btn = await page.$(sel);
    if (btn) {
      await btn.click();
      break;
    }
  }

  await page.waitForLoadState('networkidle').catch(() => {});
}

async function fillField(page, selectors, value) {
  for (const sel of selectors) {
    try {
      const el = await page.$(sel);
      if (el) {
        await el.click({ clickCount: 3 }); // select all
        await el.type(value, { delay: 30 });
        return;
      }
    } catch { /* try next selector */ }
  }
}

// ── Result row extraction ─────────────────────────────────────────────────────

/**
 * Extract permit rows from the eTRAKiT results table.
 * eTRAKiT renders results as a Kendo UI grid — rows are <tr> elements
 * inside .k-grid-content or .t-grid-content.
 */
async function extractResultRows(page) {
  return page.evaluate(() => {
    const rows = [];

    // Try multiple table container selectors
    const tableSelectors = [
      '.k-grid-content table tbody tr',
      '.t-grid-content table tbody tr',
      '#permitSearchResults tbody tr',
      'table.permit-results tbody tr',
    ];

    let trEls = [];
    for (const sel of tableSelectors) {
      trEls = Array.from(document.querySelectorAll(sel));
      if (trEls.length > 0) break;
    }

    for (const tr of trEls) {
      const cells = Array.from(tr.querySelectorAll('td'));
      if (cells.length < 3) continue; // skip header/empty rows

      // eTRAKiT column order (most instances):
      // 0: Permit Number  1: Type  2: Status  3: Address  4: Issued Date  5: Valuation
      // Column order can vary — adjust indices below if your output is wrong.
      const getText  = (i) => cells[i]?.textContent?.trim() ?? '';
      const permitLink = tr.querySelector('a[href*="/Permit/"]') ??
                         tr.querySelector('a[href*="permit"]');

      rows.push({
        permitNumber: getText(0) || permitLink?.textContent?.trim() || '',
        permitType:   getText(1),
        status:       getText(2),
        address:      getText(3),
        issuedDate:   parseDate(getText(4)),
        valuation:    parseCurrency(getText(5)),
        detailUrl:    permitLink ? new URL(permitLink.href, window.location.origin).href : null,
      });
    }

    return rows;

    function parseDate(str) {
      if (!str) return '';
      // Common formats: MM/DD/YYYY, YYYY-MM-DD
      const d = new Date(str);
      return isNaN(d) ? str : d.toISOString().split('T')[0];
    }

    function parseCurrency(str) {
      if (!str) return 0;
      return parseInt(str.replace(/[^0-9]/g, ''), 10) || 0;
    }
  });
}

// ── Detail page extraction ────────────────────────────────────────────────────

async function extractDetailFields(page) {
  return page.evaluate(() => {
    const get = (selectors) => {
      for (const sel of selectors) {
        const el = document.querySelector(sel);
        if (el?.textContent?.trim()) return el.textContent.trim();
      }
      return '';
    };

    // eTRAKiT detail pages use labeled field/value pairs.
    // Helper: find a label by text content, return the adjacent value cell.
    const getLabelValue = (labelText) => {
      const labels = Array.from(document.querySelectorAll('.field-label, th, td, label'));
      for (const lbl of labels) {
        if (lbl.textContent.trim().toLowerCase().includes(labelText.toLowerCase())) {
          // Value is typically the next sibling td or a nearby .field-value
          const next = lbl.nextElementSibling ??
                       lbl.closest('tr')?.querySelector('td:last-child');
          if (next) return next.textContent.trim();
        }
      }
      return '';
    };

    return {
      contractorName:    getLabelValue('contractor') || getLabelValue('applicant'),
      contractorLicense: getLabelValue('license'),
      contractorPhone:   getLabelValue('phone'),
      ownerName:         getLabelValue('owner'),
      workDescription:   getLabelValue('description') || getLabelValue('scope'),
      sqft:              parseInt(getLabelValue('sq ft') || getLabelValue('square'), 10) || 0,
    };
  });
}

// ── Pagination ────────────────────────────────────────────────────────────────

async function getNextPageUrl(page, baseUrl, currentPage) {
  return page.evaluate((currentPage) => {
    // eTRAKiT "next page" patterns
    const nextLink = document.querySelector(
      '.k-pager-nav.k-pager-next:not(.k-state-disabled) a, ' +
      'a[title="Go to the next page"], ' +
      '.t-pager-next:not(.t-state-disabled) a'
    );
    if (nextLink?.href) return nextLink.href;

    // Some instances use query string pagination
    const url = new URL(window.location.href);
    const page = parseInt(url.searchParams.get('page') || '1', 10);
    // Only return next-page URL if there are actually more pages
    const pagerInfo = document.querySelector('.k-pager-info, .t-status-text');
    if (pagerInfo) {
      const match = pagerInfo.textContent.match(/(\d+)\s*-\s*(\d+)\s*of\s*(\d+)/);
      if (match) {
        const [, , end, total] = match.map(Number);
        if (end < total) {
          url.searchParams.set('page', page + 1);
          return url.href;
        }
      }
    }
    return null;
  }, currentPage);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function isTargetPermit(row, keywords) {
  const type = (row.permitType || '').toUpperCase();
  const desc = (row.workDescription || '').toLowerCase();
  return keywords.some(kw => type.includes(kw.toUpperCase()) || desc.includes(kw.toLowerCase()));
}

/**
 * Map scraped fields to the unified permit pipeline schema.
 * Matches the same field names as denver_arcgis.py output.
 */
function toUnifiedSchema(row, cityName, detail) {
  return {
    source_city:         cityName,
    source_system:       'eTRAKiT',
    permit_number:       row?.permitNumber ?? '',
    permit_type:         row?.permitType ?? '',
    status:              row?.status ?? '',
    issued_date:         row?.issuedDate ?? '',
    applied_date:        '',  // not on results page — available on detail
    address:             row?.address ?? '',
    city:                cityName,
    state:               'CO',
    zip:                 extractZip(row?.address ?? ''),
    contractor_name:     detail?.contractorName ?? '',
    contractor_license:  detail?.contractorLicense ?? '',
    contractor_phone:    detail?.contractorPhone ?? '',
    owner_name:          detail?.ownerName ?? '',
    work_description:    detail?.workDescription ?? '',
    valuation:           row?.valuation ?? 0,
    sqft:                detail?.sqft ?? 0,
    lat:                 '',  // eTRAKiT doesn't expose coords — geocode separately if needed
    lng:                 '',
    portal_url:          row?.detailUrl ?? '',
    scraped_at:          new Date().toISOString(),
  };
}

function extractZip(address) {
  const match = address.match(/\b(\d{5})\b/);
  return match ? match[1] : '';
}

function today() {
  return new Date().toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });
}

// ── Run ───────────────────────────────────────────────────────────────────────

await crawler.run([{ url: searchUrl, userData: { label: 'SEARCH' } }]);

Log.info(`Scrape complete`, {
  city:          cityName,
  pagesScraped:  pageCount,
  leadsFound:    hitCount,
});

await Actor.exit();
