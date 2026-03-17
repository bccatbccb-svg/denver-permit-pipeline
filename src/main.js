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
  filterKeyword      = '',
  maxPages           = 0,
  proxyConfiguration = { useApifyProxy: true },
} = input;

const searchUrl = `${baseUrl}/Search/permit.aspx`;
const fromDate  = daysAgo(daysBack);

log.info('Starting eTRAKiT scrape', { city: cityName, baseUrl, fromDate, minValuation, filterKeyword });

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

  async requestHandler({ page }) {

    log.info('Loading search page', { url: searchUrl });

    await page.goto(searchUrl, { waitUntil: 'networkidle' });

    // Set Search By to Issued Date
    await page.selectOption('#cplMain_ddSearchBy', 'Permit_Main.ISSUED');
    await page.waitForLoadState('networkidle');

    // Set operator to AT LEAST
    await page.selectOption('#cplMain_ddSearchOper', 'AT LEAST');

    // Enter from-date
    await page.fill('#cplMain_txtSearchString', fromDate);

    // Submit
    log.info('Submitting search...', { fromDate });
    await page.click('#ctl00_cplMain_btnSearch');

    // Wait for AJAX grid to load
    let gridLoaded = false;
    try {
      await page.waitForSelector(
        '.rgMasterTable tbody tr, #ctl00_cplMain_rgSearchRslts table tr',
        { timeout: 30000 }
      );
      gridLoaded = true;
    } catch (e) {
      log.warning('Grid did not load after 30s', { error: e.message });
    }

    // Always save screenshot + HTML so you can inspect what the browser saw
    // View them in Apify: Storage tab → Key-value store → search-result / page-html
    const shot = await page.screenshot({ fullPage: true });
    await Actor.setValue('search-result', shot, { contentType: 'image/png' });
    const pageHtml = await page.content();
    await Actor.setValue('page-html', pageHtml, { contentType: 'text/html' });
    log.info('Saved screenshot and HTML to key-value store (Storage tab).');

    if (!gridLoaded) {
      log.info('Grid did not load — check search-result screenshot in Storage tab.');
      return;
    }

    // Page through all results
    let hasNextPage = true;

    while (hasNextPage) {
      pageCount++;
      log.info(`Processing page ${pageCount}...`);

      const rows = await extractGridRows(page);
      log.info(`Found ${rows.length} rows on page ${pageCount}`);
      if (rows.length > 0) log.info('First row sample', { r: JSON.stringify(rows[0]) });

      for (const row of rows) {
        if (seen.has(row.permitNumber)) continue;
        seen.add(row.permitNumber);

        // Keyword filter — checks every field in the row
        if (!matchesKeyword(row, filterKeyword)) continue;

        // Valuation filter
        if (minValuation > 0 && row.valuation > 0 && row.valuation < minValuation) continue;

        hitCount++;

        const detail = await clickRowAndGetDetail(page, row.rowIndex);
        const record = toUnifiedSchema(row, cityName, detail, baseUrl);
        await Dataset.pushData(record);
        log.info('Saved permit', { permitNumber: row.permitNumber, type: row.permitType });
      }

      if (maxPages > 0 && pageCount >= maxPages) {
        log.info(`Reached maxPages limit (${maxPages}), stopping.`);
        break;
      }

      hasNextPage = await clickNextPage(page);
    }
  },

  failedRequestHandler({ request }) {
    log.error('Request failed', { url: request.url });
  },
});

// ── Grid row extraction ───────────────────────────────────────────────────────
// Columns from screenshot: PermitNumber | Applied | Approved | IssuedDate |
// Finaled | Expired | PermitType | PermitSubtype | Address | Status | ...

async function extractGridRows(page) {
  return page.evaluate(() => {
    const rows = [];

    const trEls = document.querySelectorAll(
      '.rgMasterTable tbody tr, #ctl00_cplMain_rgSearchRslts table tbody tr'
    );

    const unique = Array.from(new Set(Array.from(trEls)));

    unique.forEach(function(tr, rowIndex) {
      const cells = Array.from(tr.querySelectorAll('td'));
      if (cells.length < 4) return;

      const get = function(i) { return cells[i] ? cells[i].textContent.trim() : ''; };

      function parseCurrency(str) {
        if (!str) return 0;
        return parseInt(str.replace(/[^0-9]/g, ''), 10) || 0;
      }

      // Capture every cell as raw text — used by keyword filter
      const allText = cells.map(function(td) { return td.textContent.trim(); }).join(' | ');

      // Column order confirmed from Arvada screenshot:
      // 0:PermitNo 1:Applied 2:Approved 3:IssuedDate 4:Finaled 5:Expired
      // 6:PermitType 7:PermitSubtype — remaining cols vary, capture via allText
      rows.push({
        rowIndex:       rowIndex,
        permitNumber:   get(0),
        appliedDate:    get(1),
        approvedDate:   get(2),
        issuedDate:     get(3),
        permitType:     get(6),
        permitSubtype:  get(7),
        status:         get(8),
        description:    get(9),
        valuation:      parseCurrency(get(10)),
        contractorName: get(11),
        address:        get(12),
        _allText:       allText,
      });
    });

    return rows;
  });
}

// ── Detail panel ─────────────────────────────────────────────────────────────

async function clickRowAndGetDetail(page, rowIndex) {
  try {
    const rows = await page.$$(
      '.rgMasterTable tbody tr, #ctl00_cplMain_rgSearchRslts table tbody tr'
    );
    if (!rows[rowIndex]) return null;

    await rows[rowIndex].click();

    try {
      await page.waitForSelector(
        '#cplMain_RadPageViewPermitInfo, #cplMain_UpdatePanelDetail',
        { timeout: 10000 }
      );
    } catch (e) {
      log.debug('Detail panel timeout', { rowIndex });
    }

    await page.waitForTimeout(800);

    return page.evaluate(() => {
      function getLabelValue(labelText) {
        const els = document.querySelectorAll('span, td, label, div');
        for (const el of els) {
          if (el.textContent.trim().toLowerCase().includes(labelText.toLowerCase())) {
            const next = el.nextElementSibling;
            if (next && next.textContent.trim()) return next.textContent.trim();
            const parentCell = el.closest('td');
            if (parentCell && parentCell.nextElementSibling && parentCell.nextElementSibling.textContent.trim()) {
              return parentCell.nextElementSibling.textContent.trim();
            }
          }
        }
        return '';
      }

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
    log.debug('Detail panel error', { rowIndex, error: err.message });
    return null;
  }
}

// ── Pagination ────────────────────────────────────────────────────────────────

async function clickNextPage(page) {
  try {
    const nextBtn = await page.$(
      '.rgPageNext:not(.rgDisabled), a[title="Next Page"]:not(.rgDisabled)'
    );
    if (!nextBtn) return false;

    const isDisabled = await nextBtn.evaluate(function(el) {
      return el.classList.contains('rgDisabled') || el.getAttribute('disabled') !== null;
    });
    if (isDisabled) return false;

    await nextBtn.click();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(800);
    return true;
  } catch (err) {
    log.debug('Pagination error', { error: err.message });
    return false;
  }
}

// ── Keyword filter ────────────────────────────────────────────────────────────
// Scans every field in the row — doesn't matter which column the keyword is in

function matchesKeyword(row, keyword) {
  if (!keyword || keyword.trim() === '') return true;
  const kw = keyword.trim().toUpperCase();
  // _allText contains every cell joined — catches any column regardless of index
  if (row._allText && row._allText.toUpperCase().includes(kw)) return true;
  return Object.values(row).some(function(val) {
    return typeof val === 'string' && val.toUpperCase().includes(kw);
  });
}

// ── Schema ────────────────────────────────────────────────────────────────────

function toUnifiedSchema(row, cityName, detail, baseUrl) {
  return {
    source_city:        cityName,
    source_system:      'eTRAKiT',
    permit_number:      row.permitNumber              || '',
    permit_type:        row.permitType                || '',
    permit_subtype:     row.permitSubtype             || '',
    status:             row.status                    || '',
    issued_date:        row.issuedDate                || '',
    applied_date:       row.appliedDate               || '',
    address:            row.address                   || '',
    city:               cityName,
    state:              'CO',
    zip:                extractZip(row.address        || ''),
    contractor_name:    (detail && detail.contractorName)    || row.contractorName || '',
    contractor_license: (detail && detail.contractorLicense) || '',
    contractor_phone:   (detail && detail.contractorPhone)   || '',
    owner_name:         (detail && detail.ownerName)         || '',
    work_description:   (detail && detail.workDescription)   || row.description || '',
    valuation:          row.valuation                 || 0,
    sqft:               (detail && detail.sqft)       || 0,
    lat:                '',
    lng:                '',
    portal_url:         baseUrl + '/Search/permit.aspx',
    scraped_at:         new Date().toISOString(),
  };
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function extractZip(address) {
  const match = address.match(/\b(\d{5})\b/);
  return match ? match[1] : '';
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });
}

// ── Run ───────────────────────────────────────────────────────────────────────

await crawler.run([{ url: searchUrl }]);

log.info('Scrape complete', { city: cityName, pagesScraped: pageCount, leadsFound: hitCount });

await Actor.exit();
