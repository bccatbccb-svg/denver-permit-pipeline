/**
 * Denver Metro eTRAKiT Permit Scraper
 * ------------------------------------
 * Scrapes eTRAKiT permit portals (CentralSquare/Telerik RadGrid)
 * for custom home and remodel leads.
 *
 * How this portal actually works (confirmed from Arvada HTML):
 *   1. Load /Search/permit.aspx
 *   2. Set dropdown "Search By" to "Issued Date" (field: Permit_Main.ISSUED)
 *   3. Set operator to "AT LEAST" and value to the from-date
 *   4. Click Search → page does an AJAX postback
 *   5. Wait for the Telerik RadGrid (#ctl00_cplMain_rgSearchRslts) to populate
 *   6. Extract rows from the grid table
 *   7. Paginate via the grid next-page button
 *   8. For each matching row, click it to load the detail panel (AJAX)
 *   9. Extract contractor and description from the detail panel
 *
 * Works against any eTRAKiT/CentralSquare instance — pass baseUrl as input.
 */

import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset, log } from 'crawlee';

await Actor.init();

// ── Input ─────────────────────────────────────────────────────────────────────

const input = await Actor.getInput() ?? {};

const {
  baseUrl            = 'https://arvadapermits.org/etrakit3',
  cityName           = 'Arvada',
  daysBack           = 30,
  minValuation       = 50000,
  permitTypeKeywords = [
    'NEW SINGLE FAMILY', 'NEW SFR', 'CUSTOM HOME',
    'SINGLE FAMILY DETACHED', 'ADDITION', 'REMODEL',
    'ADU', 'ACCESSORY DWELLING', 'RESIDENTIAL NEW',
  ],
  maxPages           = 0,
  proxyConfiguration = { useApifyProxy: true },
} = input;

const searchUrl = `${baseUrl}/Search/permit.aspx`;
const fromDate  = daysAgo(daysBack);

log.info('Starting eTRAKiT scrape', { city: cityName, baseUrl, fromDate, minValuation });

// ── Proxy ─────────────────────────────────────────────────────────────────────

const proxy = await Actor.createProxyConfiguration(proxyConfiguration);

// ── State ─────────────────────────────────────────────────────────────────────

const seen    = new Set();
let pageCount = 0;
let hitCount  = 0;

// ── Crawler ───────────────────────────────────────────────────────────────────

const crawler = new PlaywrightCrawler({

  proxyConfiguration: proxy,
  requestHandlerTimeoutSecs: 120,
  navigationTimeoutSecs: 60,
  minConcurrency: 1,
  maxConcurrency: 1,

  async requestHandler({ page, request }) {

    log.info('Loading search page', { url: request.url });

    // Step 1: Navigate
    await page.goto(searchUrl, { waitUntil: 'networkidle' });

    // Step 2: Set Search By to Issued Date
    await page.selectOption('#cplMain_ddSearchBy', 'Permit_Main.ISSUED');
    await page.waitForLoadState('networkidle');

    // Step 3: Set operator to AT LEAST
    await page.selectOption('#cplMain_ddSearchOper', 'AT LEAST');

    // Step 4: Enter the from-date
    await page.fill('#cplMain_txtSearchString', fromDate);

    // Step 5: Click Search and wait for AJAX grid
    log.info('Submitting search form...', { fromDate });
    await page.click('#ctl00_cplMain_btnSearch');

    try {
      await page.waitForSelector(
        '#ctl00_cplMain_rgSearchRslts table tbody tr',
        { timeout: 30000 }
      );
    } catch {
      const noResults = await page.$('#cplMain_lblNoSearchRslts');
      if (noResults) {
        log.info('Portal returned no results for this date range.');
      } else {
        log.warning('Grid did not load — page structure may differ.');
      }
      return;
    }

    // Step 6: Page through all results
    let hasNextPage = true;

    while (hasNextPage) {
      pageCount++;
      log.info(`Processing page ${pageCount}...`);

      const rows = await extractGridRows(page);
      log.info(`Found ${rows.length} rows on page ${pageCount}`);

      for (const row of rows) {
        if (seen.has(row.permitNumber)) continue;
        seen.add(row.permitNumber);

        if (!isTargetPermit(row, permitTypeKeywords)) continue;
        if (minValuation > 0 && row.valuation > 0 && row.valuation < minValuation) continue;

        hitCount++;

        const detail = await clickRowAndGetDetail(page, row.rowIndex);
        const record = toUnifiedSchema(row, cityName, detail, baseUrl);
        await Dataset.pushData(record);
        log.debug('Saved permit', { permitNumber: row.permitNumber, type: row.permitType });
      }

      if (maxPages > 0 && pageCount >= maxPages) {
        log.info(`Reached maxPages limit (${maxPages}), stopping.`);
        break;
      }

      hasNextPage = await clickNextPage(page);
    }
  },

  failedRequestHandler({ request }) {
    log.error('Request failed after retries', { url: request.url });
  },
});

// ── Grid row extraction ───────────────────────────────────────────────────────
// Column order confirmed from Arvada viewstate:
// 0:expand  1:Address  2:Issued  3:PermitNo  4:Subdivision  5:Applied
// 6:Approved  7:Finaled  8:Expired  9:PermitType  10:Subtype  11:Status
// 12:Description  13:JobValue  14:CO Issued  15:ContractorName

async function extractGridRows(page) {
  return page.evaluate(() => {
    const rows = [];
    const trEls = document.querySelectorAll(
      '#ctl00_cplMain_rgSearchRslts table tbody tr'
    );

    trEls.forEach((tr, rowIndex) => {
      const cells = Array.from(tr.querySelectorAll('td'));
      if (cells.length < 4) return;

      const get = (i) => cells[i]?.textContent?.trim() ?? '';
      const link = tr.querySelector('a[href*="permit"], a[href*="Permit"]');

      rows.push({
        rowIndex,
        permitNumber:  get(3),
        address:       get(1),
        issuedDate:    get(2),
        appliedDate:   get(5),
        permitType:    get(9),
        permitSubtype: get(10),
        status:        get(11),
        description:   get(12),
        valuation:     parseCurrency(get(13)),
        contractorName: get(15),
        detailHref:    link?.href ?? '',
      });
    });

    return rows;

    function parseCurrency(str) {
      if (!str) return 0;
      return parseInt(str.replace(/[^0-9]/g, ''), 10) || 0;
    }
  });
}

// ── Detail panel extraction ───────────────────────────────────────────────────

async function clickRowAndGetDetail(page, rowIndex) {
  try {
    const rows = await page.$$('#ctl00_cplMain_rgSearchRslts table tbody tr');
    if (!rows[rowIndex]) return null;

    await rows[rowIndex].click();

    await page.waitForSelector(
      '#cplMain_RadPageViewPermitInfo, #cplMain_UpdatePanelDetail',
      { timeout: 10000 }
    ).catch(() => {});

    await page.waitForTimeout(1000);

    return page.evaluate(() => {
      const getLabelValue = (labelText) => {
        const allEls = document.querySelectorAll('span, td, label, div');
        for (const el of allEls) {
          if (el.textContent.trim().toLowerCase().includes(labelText.toLowerCase())) {
            const next = el.nextElementSibling;
            if (next?.textContent?.trim()) return next.textContent.trim();
            const parentCell = el.closest('td');
            if (parentCell?.nextElementSibling?.textContent?.trim()) {
              return parentCell.nextElementSibling.textContent.trim();
            }
          }
        }
        return '';
      };

      return {
        contractorName:    getLabelValue('contractor'),
        contractorLicense: getLabelValue('license'),
        contractorPhone:   getLabelValue('phone'),
        ownerName:         getLabelValue('owner'),
        workDescription:   getLabelValue('description') || getLabelValue('scope of work'),
        sqft:              parseInt(getLabelValue('sq ft') || getLabelValue('square feet'), 10) || 0,
      };
    });
  } catch (err) {
    log.debug('Could not load detail panel', { rowIndex, err: err.message });
    return null;
  }
}

// ── Pagination ────────────────────────────────────────────────────────────────

async function clickNextPage(page) {
  try {
    const nextBtn = await page.$(
      '.rgPageNext:not(.rgDisabled), ' +
      'a[title="Next Page"]:not(.rgDisabled), ' +
      '.t-arrow-next:not(.t-state-disabled)'
    );
    if (!nextBtn) return false;

    const isDisabled = await nextBtn.evaluate(
      el => el.classList.contains('rgDisabled') ||
            el.classList.contains('t-state-disabled') ||
            el.getAttribute('disabled') !== null
    );
    if (isDisabled) return false;

    await nextBtn.click();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);
    return true;
  } catch {
    return false;
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function isTargetPermit(row, keywords) {
  const type = (row.permitType    || '').toUpperCase();
  const sub  = (row.permitSubtype || '').toUpperCase();
  const desc = (row.description   || '').toLowerCase();
  return keywords.some(kw => {
    const k = kw.toUpperCase();
    return type.includes(k) || sub.includes(k) || desc.includes(kw.toLowerCase());
  });
}

function toUnifiedSchema(row, cityName, detail, baseUrl) {
  return {
    source_city:        cityName,
    source_system:      'eTRAKiT',
    permit_number:      row.permitNumber       ?? '',
    permit_type:        row.permitType         ?? '',
    status:             row.status             ?? '',
    issued_date:        row.issuedDate         ?? '',
    applied_date:       row.appliedDate        ?? '',
    address:            row.address            ?? '',
    city:               cityName,
    state:              'CO',
    zip:                extractZip(row.address ?? ''),
    contractor_name:    detail?.contractorName    || row.contractorName || '',
    contractor_license: detail?.contractorLicense ?? '',
    contractor_phone:   detail?.contractorPhone   ?? '',
    owner_name:         detail?.ownerName         ?? '',
    work_description:   detail?.workDescription   || row.description   || '',
    valuation:          row.valuation             ?? 0,
    sqft:               detail?.sqft              ?? 0,
    lat:                '',
    lng:                '',
    portal_url:         row.detailHref || `${baseUrl}/Search/permit.aspx`,
    scraped_at:         new Date().toISOString(),
  };
}

function extractZip(address) {
  const match = address.match(/\b(\d{5})\b/);
  return match ? match[1] : '';
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toLocaleDateString('en-US', {
    month: '2-digit', day: '2-digit', year: 'numeric',
  });
}

// ── Run ───────────────────────────────────────────────────────────────────────

await crawler.run([{ url: searchUrl }]);

log.info('Scrape complete', {
  city:         cityName,
  pagesScraped: pageCount,
  leadsFound:   hitCount,
});

await Actor.exit();
