// ============================================================
// SCHEDULER ENGINE - Puppeteer-based per-job scheduler
// Uses a real headless Chrome browser for all HTTP requests,
// giving us authentic TLS fingerprints, headers, and cookies.
// ============================================================

const db = require('./database');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const BASE_URL = 'https://ais.usvisa-info.com';

// ── Fingerprint pool ──
// Each entry is a realistic, internally-consistent browser identity.
// UA, sec-ch-ua, platform, and viewport all match so nothing looks off.
const os = require('os');
const IS_LINUX = os.platform() === 'linux';

const FINGERPRINTS = [
  // ── Windows / Chrome 133 ──
  {
    ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="133", "Google Chrome";v="133", "Not?A_Brand";v="99"',
    platform: '"Windows"',
    viewport: { width: 1920, height: 1080 },
    acceptLang: 'en-US,en;q=0.9'
  },
  {
    ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="133", "Google Chrome";v="133", "Not?A_Brand";v="99"',
    platform: '"Windows"',
    viewport: { width: 1536, height: 864 },
    acceptLang: 'en-US,en;q=0.9'
  },
  {
    ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="132", "Google Chrome";v="132", "Not?A_Brand";v="99"',
    platform: '"Windows"',
    viewport: { width: 1366, height: 768 },
    acceptLang: 'en-US,en;q=0.9'
  },
  {
    ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="131", "Google Chrome";v="131", "Not?A_Brand";v="99"',
    platform: '"Windows"',
    viewport: { width: 1440, height: 900 },
    acceptLang: 'en-US,en;q=0.9,es;q=0.8'
  },
  // ── macOS / Chrome 133 (only used when actually on macOS) ──
  {
    ua: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="133", "Google Chrome";v="133", "Not?A_Brand";v="99"',
    platform: '"macOS"',
    viewport: { width: 1440, height: 900 },
    acceptLang: 'en-US,en;q=0.9',
    macOnly: true
  },
  {
    ua: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="132", "Google Chrome";v="132", "Not?A_Brand";v="99"',
    platform: '"macOS"',
    viewport: { width: 1680, height: 1050 },
    acceptLang: 'en-CA,en;q=0.9,fr;q=0.8',
    macOnly: true
  },
  // ── Linux / Chrome (used when running on Linux VPS) ──
  {
    ua: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="133", "Google Chrome";v="133", "Not?A_Brand";v="99"',
    platform: '"Linux"',
    viewport: { width: 1920, height: 1080 },
    acceptLang: 'en-US,en;q=0.9',
    linuxOnly: true
  },
  {
    ua: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="132", "Google Chrome";v="132", "Not?A_Brand";v="99"',
    platform: '"Linux"',
    viewport: { width: 1366, height: 768 },
    acceptLang: 'en-US,en;q=0.9',
    linuxOnly: true
  },
  {
    ua: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="131", "Google Chrome";v="131", "Not?A_Brand";v="99"',
    platform: '"Linux"',
    viewport: { width: 1536, height: 864 },
    acceptLang: 'en-CA,en;q=0.9',
    linuxOnly: true
  }
];

function pickFingerprint() {
  // Filter to platform-appropriate fingerprints
  const candidates = FINGERPRINTS.filter(fp => {
    if (IS_LINUX && fp.macOnly) return false;   // don't claim macOS on Linux
    if (!IS_LINUX && fp.linuxOnly) return false; // don't claim Linux on macOS
    return true;
  });
  return candidates[Math.floor(Math.random() * candidates.length)];
}

// Shared browser instance (lazy-launched, reused across jobs)
let sharedBrowser = null;
let browserLaunchPromise = null;

async function launchBrowser() {
  if (sharedBrowser && sharedBrowser.connected) return sharedBrowser;
  if (browserLaunchPromise) return browserLaunchPromise;

  browserLaunchPromise = (async () => {
    const b = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--no-first-run',
        '--disable-background-networking',
        '--disable-default-apps',
        '--disable-sync',
        '--disable-translate',
        '--metrics-recording-only',
        '--mute-audio',
        '--no-default-browser-check',
        '--disable-blink-features=AutomationControlled'
      ],
      defaultViewport: null  // each page sets its own viewport via fingerprint
    });
    b.on('disconnected', () => { sharedBrowser = null; });
    sharedBrowser = b;
    browserLaunchPromise = null;
    return b;
  })();

  return browserLaunchPromise;
}

// loadModules kept for backward compat with agent.js
async function loadModules() {
  // No-op — puppeteer is loaded via require()
}

class SchedulerInstance {
  constructor(jobId) {
    this.jobId = jobId;
    this.running = false;
    this.stopping = false;
    this.page = null;
    this.csrfToken = null;
    this.config = null;
    this.loopPromise = null;

    // Pick a unique fingerprint for this instance (stays constant for its lifetime)
    this.fingerprint = pickFingerprint();

    // Health stats (in-memory, synced to DB periodically)
    this.health = {
      totalChecks: 0,
      successfulChecks: 0,
      failedChecks: 0,
      consecutiveFailures: 0,
      reloginCount: 0,
      lastError: null,
      lastCheckAt: null,
      startedAt: null
    };
  }

  // ── Logging ──
  log(level, msg) {
    const ts = new Date().toISOString();
    const prefixes = { debug: '🔍', info: 'ℹ️', success: '✅', warn: '⚠️', error: '❌' };
    if (level !== 'debug') {
      console.log('[' + ts + '] [Job:' + this.jobId + '] ' + (prefixes[level] || '') + ' ' + msg);
    }
    try {
      db.addLog(this.jobId, level, msg);
    } catch (e) { /* ignore DB errors in logging */ }
  }

  // ── Sync health to DB ──
  syncHealth() {
    try {
      db.updateJob(this.jobId, {
        totalChecks: this.health.totalChecks,
        successfulChecks: this.health.successfulChecks,
        failedChecks: this.health.failedChecks,
        consecutiveFailures: this.health.consecutiveFailures,
        reloginCount: this.health.reloginCount,
        lastError: this.health.lastError,
        lastCheckAt: this.health.lastCheckAt
      });
    } catch (e) { /* ignore */ }
  }

  sleep(ms) {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, ms);
      this._sleepTimer = timer;
    });
  }

  cancelSleep() {
    if (this._sleepTimer) {
      clearTimeout(this._sleepTimer);
      this._sleepTimer = null;
    }
  }

  // ============================================================
  // BROWSER PAGE MANAGEMENT
  // ============================================================
  async initPage() {
    const browser = await launchBrowser();
    if (this.page && !this.page.isClosed()) {
      try { await this.page.close(); } catch (e) { /* ignore */ }
    }
    this.page = await browser.newPage();

    const fp = this.fingerprint;
    this.log('info', '🪪 Fingerprint: ' + fp.platform + ' / Chrome ' + fp.secChUa.match(/Chrome";v="(\d+)/)?.[1] + ' / ' + fp.viewport.width + 'x' + fp.viewport.height);

    // Set the viewport to match the fingerprint
    await this.page.setViewport(fp.viewport);

    // Set User-Agent to match the fingerprint
    await this.page.setUserAgent(fp.ua);

    // Set matching HTTP headers
    await this.page.setExtraHTTPHeaders({
      'Accept-Language': fp.acceptLang,
      'sec-ch-ua': fp.secChUa,
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': fp.platform
    });

    // Hide webdriver flag and override navigator.platform to match fingerprint
    const navPlatform = fp.platform.includes('Windows') ? 'Win32' :
                        fp.platform.includes('macOS') ? 'MacIntel' : 'Linux x86_64';
    await this.page.evaluateOnNewDocument((plat) => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
      Object.defineProperty(navigator, 'platform', { get: () => plat });
    }, navPlatform);

    // Block images, fonts, media to save bandwidth (keep stylesheets & scripts)
    await this.page.setRequestInterception(true);
    this.page.on('request', (req) => {
      const type = req.resourceType();
      if (['image', 'font', 'media'].includes(type)) {
        req.abort();
      } else {
        req.continue();
      }
    });

    this.csrfToken = null;
  }

  async closePage() {
    if (this.page && !this.page.isClosed()) {
      try { await this.page.close(); } catch (e) { /* ignore */ }
    }
    this.page = null;
  }

  // ── Extract CSRF token from the current page ──
  async extractCsrf() {
    try {
      const token = await this.page.evaluate(() => {
        const meta = document.querySelector('meta[name="csrf-token"]');
        if (meta) return meta.getAttribute('content');
        const input = document.querySelector('input[name="authenticity_token"]');
        if (input) return input.value;
        return null;
      });
      if (token) this.csrfToken = token;
      return token;
    } catch (e) {
      return null;
    }
  }

  // ============================================================
  // IN-BROWSER FETCH WITH RETRY
  // Executes fetch() inside Chrome — real TLS, real cookies
  // ============================================================
  async browserFetch(url, options, maxRetries) {
    maxRetries = maxRetries || (this.config && this.config.maxRetries) || 3;
    let attempt = 0;
    let lastError;
    const method = (options && options.method) || 'GET';
    const timeoutMs = (this.config && this.config.requestTimeoutMs) || 20000;

    while (attempt < maxRetries) {
      attempt++;
      this.log('debug', 'fetch ' + method + ' ' + url + ' [attempt ' + attempt + '/' + maxRetries + ']');
      try {
        const result = await this.page.evaluate(async (fetchUrl, fetchMethod, fetchHeaders, fetchBody, fetchTimeout) => {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort(), fetchTimeout);
          try {
            const opts = {
              method: fetchMethod,
              headers: fetchHeaders || {},
              signal: controller.signal,
              credentials: 'include'
            };
            if (fetchBody) opts.body = fetchBody;
            const resp = await fetch(fetchUrl, opts);
            clearTimeout(timer);
            const text = await resp.text();
            return {
              ok: resp.ok,
              status: resp.status,
              url: resp.url,
              text: text,
              redirected: resp.redirected
            };
          } catch (err) {
            clearTimeout(timer);
            return { error: err.message || 'fetch failed' };
          }
        }, url, method, options.headers || {}, options.body || null, timeoutMs);

        if (result.error) {
          throw new Error(result.error);
        }
        return result;
      } catch (err) {
        lastError = err;
        this.log('debug', 'fetch ' + method + ' FAILED: ' + err.message);

        const isSocketError = !!(err.message && (
          err.message.includes('socket hang up') ||
          err.message.includes('net::ERR_') ||
          err.message.includes('ECONNRESET') ||
          err.message.includes('ECONNREFUSED') ||
          err.message.includes('ETIMEDOUT') ||
          err.message.includes('aborted') ||
          err.message.includes('TimeoutError') ||
          err.message.includes('Navigation timeout') ||
          err.message.includes('Execution context was destroyed') ||
          err.message.includes('Protocol error') ||
          err.message.includes('Failed to fetch')
        ));
        if (isSocketError) lastError._isSocketError = true;

        if (attempt < maxRetries) {
          const baseDelay = isSocketError
            ? Math.min(5000 * Math.pow(2, attempt - 1), 60000)
            : Math.min(1000 * Math.pow(2, attempt - 1), 10000);
          const delay = baseDelay + Math.random() * 2000;
          this.log('warn', 'Request failed (' + err.message + '). Retry ' + attempt + '/' + maxRetries + ' in ' + Math.round(delay / 1000) + 's...');
          await this.sleep(delay);
        }
      }
    }
    if (lastError) lastError._retriesExhausted = true;
    throw lastError;
  }

  // ============================================================
  // LOGIN — real browser navigation (authentic Chrome TLS)
  // ============================================================
  async login() {
    this.log('info', 'Logging in as ' + this.config.email + '...');
    await this.initPage();

    const loginUrl = BASE_URL + '/' + this.config.country + '/niv/users/sign_in';

    // Step 1: Navigate to login page
    try {
      await this.page.goto(loginUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
      // Give the page a moment to settle (scripts may inject elements)
      await this.sleep(2000);
    } catch (err) {
      throw new Error('Could not load login page: ' + err.message);
    }
    this.log('info', 'Login page loaded: ' + this.page.url());

    // Step 2: Extract CSRF — try immediately, then wait for the element
    let csrf = await this.extractCsrf();
    if (!csrf) {
      // The meta tag might not be in the DOM yet — wait up to 10s
      try {
        await this.page.waitForSelector('meta[name="csrf-token"], input[name="authenticity_token"]', { timeout: 10000 });
        csrf = await this.extractCsrf();
      } catch (e) { /* timeout — will handle below */ }
    }
    if (!csrf) {
      const html = await this.page.content();
      const snippet = html.substring(0, 500);
      this.log('error', 'CSRF not found. URL: ' + this.page.url() + ' | HTML snippet: ' + snippet);
      if (html.includes('try again later') || html.includes('Too many')) {
        throw new Error('Login rate limited. Try again later.');
      }
      throw new Error('Could not extract CSRF token from login page');
    }
    this.log('info', 'Got CSRF token.');

    // Step 3: Fill and submit login form
    // NOTE: The form uses data-remote="true" (Rails UJS), so it submits via AJAX.
    // The server responds with JS that sets window.location for redirect.
    try {
      // Clear fields first in case of previous values
      await this.page.evaluate(() => {
        const emailEl = document.querySelector('input[name="user[email]"]');
        const passEl = document.querySelector('input[name="user[password]"]');
        if (emailEl) emailEl.value = '';
        if (passEl) passEl.value = '';
      });

      await this.page.type('input[name="user[email]"]', this.config.email, { delay: 30 });
      await this.page.type('input[name="user[password]"]', this.config.password, { delay: 30 });

      // Check policy_confirmed checkbox if present
      const policyCheckbox = await this.page.$('input[name="policy_confirmed"]');
      if (policyCheckbox) {
        const isChecked = await this.page.evaluate(el => el.checked, policyCheckbox);
        if (!isChecked) await policyCheckbox.click();
      }

      // Click submit and wait for either navigation or AJAX response
      this.log('info', 'Submitting login form...');
      await this.page.click('input[type="submit"], button[type="submit"]');

      // Wait for navigation (the AJAX response triggers window.location = ...)
      // Use a race between navigation and a timeout
      try {
        await this.page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 15000 });
      } catch (navErr) {
        // Navigation may not happen if AJAX login failed (wrong creds, rate limit, etc.)
        this.log('debug', 'No navigation after submit: ' + navErr.message);
      }

      // Give extra time for JS redirects to fire
      await this.sleep(3000);
    } catch (err) {
      throw new Error('Login form submission failed: ' + err.message);
    }

    // Step 4: Check if login succeeded
    const currentUrl = this.page.url();
    this.log('info', 'Post-login URL: ' + currentUrl);
    const pageContent = await this.page.content();

    if (pageContent.includes('Invalid Email or password') || pageContent.includes('invalid email or password')) {
      throw new Error('Invalid email or password.');
    }
    if (pageContent.includes('try again later') || pageContent.includes('Too many')) {
      throw new Error('Login rate limited. Try again later.');
    }

    if (currentUrl.includes('sign_in')) {
      // Still on login page — try to navigate to the group page to verify
      this.log('info', 'Still on login page, attempting redirect...');
      try {
        const groupUrl = BASE_URL + '/' + this.config.country + '/niv/groups/' + this.config.scheduleId;
        await this.page.goto(groupUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
        const afterUrl = this.page.url();
        if (afterUrl.includes('sign_in')) {
          throw new Error('Login failed — redirected back to sign in');
        }
      } catch (navErr) {
        if (navErr.message.includes('Login failed')) throw navErr;
        throw new Error('Login failed: ' + navErr.message);
      }
    }

    // Refresh CSRF from the authenticated page
    await this.extractCsrf();

    this.log('success', 'Login successful! (Puppeteer browser)');
    this.health.reloginCount++;
    return true;
  }

  // ============================================================
  // FETCH LOCATIONS — parse the appointment page
  // ============================================================
  async fetchLocations() {
    this.log('info', 'Fetching available locations...');
    const url = BASE_URL + '/' + this.config.country + '/niv/schedule/' + this.config.scheduleId + '/appointment';

    try {
      await this.page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await this.sleep(2000);
    } catch (err) {
      throw new Error('Could not load appointment page: ' + err.message);
    }

    const currentUrl = this.page.url();
    if (currentUrl.includes('sign_in')) {
      throw new Error('SESSION_EXPIRED');
    }

    // Refresh CSRF from appointment page
    await this.extractCsrf();

    // Extract locations from select dropdown
    const locations = await this.page.evaluate(() => {
      const select = document.querySelector('select[name="appointments[consulate_appointment][facility_id]"]');
      if (!select) return [];
      const options = [];
      select.querySelectorAll('option').forEach(opt => {
        const val = opt.value ? opt.value.trim() : '';
        const name = opt.textContent ? opt.textContent.trim() : '';
        if (val) options.push({ id: val, name: name });
      });
      return options;
    });

    if (locations.length === 0) {
      const html = await this.page.content();
      if (html.includes('sign_in') || html.includes('Sign In')) {
        throw new Error('SESSION_EXPIRED');
      }
    }

    if (locations.length > 0) {
      db.cacheLocations(this.jobId, locations);
    }

    return locations;
  }

  // ============================================================
  // NAVIGATE TO APPOINTMENT PAGE (shared helper)
  // Ensures we're on the appointment page with a fresh CSRF.
  // Reuses the page if already there, avoiding extra navigations.
  // ============================================================
  async ensureOnAppointmentPage() {
    const apptUrl = BASE_URL + '/' + this.config.country + '/niv/schedule/' + this.config.scheduleId + '/appointment';

    // Check if we're already on the appointment page
    const currentUrl = this.page.url();
    const isOnApptPage = currentUrl.includes('/appointment') && !currentUrl.includes('/days/') && !currentUrl.includes('/times/');

    if (isOnApptPage) {
      // Quick DOM check: is the facility dropdown still present?
      const hasFacilitySelect = await this.page.evaluate(() => !!document.querySelector('#appointments_consulate_appointment_facility_id'));
      if (hasFacilitySelect) {
        this.log('debug', 'Already on appointment page, reusing.');
        return;
      }
    }

    this.log('debug', 'Navigating to appointment page...');
    try {
      await this.page.goto(apptUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await this.sleep(1500 + Math.random() * 1000);
    } catch (err) {
      throw new Error('Could not load appointment page: ' + err.message);
    }

    const afterUrl = this.page.url();
    if (afterUrl.includes('sign_in')) {
      throw new Error('SESSION_EXPIRED');
    }

    await this.extractCsrf();

    // Wait for the facility dropdown to be present
    try {
      await this.page.waitForSelector('#appointments_consulate_appointment_facility_id', { timeout: 10000 });
    } catch (e) {
      throw new Error('Appointment page did not load properly (no facility dropdown)');
    }
  }

  // ============================================================
  // CHECK DATES — DOM-based (simulates user selecting a facility)
  // Instead of hitting the JSON endpoint directly, we:
  // 1. Navigate to the appointment page
  // 2. Select the facility from the dropdown (triggers AJAX naturally)
  // 3. Wait for the calendar to load
  // 4. Extract available dates from the rendered datepicker
  // ============================================================
  async checkDates(facilityId) {
    await this.ensureOnAppointmentPage();

    // Set up a promise to intercept the AJAX response for days.json
    // This fires naturally when the dropdown selection changes
    const daysJsonPromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.page.off('response', handler);
        resolve(null); // timeout — no response, we'll extract from DOM
      }, 15000);

      const handler = async (response) => {
        const url = response.url();
        if (url.includes('/appointment/days/') && url.includes('.json')) {
          clearTimeout(timeout);
          this.page.off('response', handler);
          try {
            const status = response.status();
            if (status === 401 || status === 403) {
              resolve({ _error: 'SESSION_EXPIRED' });
              return;
            }
            if (status === 429) {
              resolve({ _error: 'RATE_LIMITED' });
              return;
            }
            if (status === 422) {
              resolve({ _error: 'CSRF_EXPIRED' });
              return;
            }
            const json = await response.json();
            resolve(json);
          } catch (e) {
            resolve(null);
          }
        }
      };
      this.page.on('response', handler);
    });

    // Select the facility from the dropdown (like a real user)
    this.log('debug', 'Selecting facility ' + facilityId + ' from dropdown...');
    const selectResult = await this.page.evaluate((facId) => {
      const sel = document.querySelector('#appointments_consulate_appointment_facility_id');
      if (!sel) return { error: 'no_select' };
      // Check if the option exists
      const optExists = Array.from(sel.options).some(o => o.value === String(facId));
      if (!optExists) return { error: 'option_not_found' };
      sel.value = String(facId);
      sel.dispatchEvent(new Event('change', { bubbles: true }));
      return { ok: true };
    }, facilityId);

    if (selectResult.error === 'no_select') throw new Error('Facility dropdown not found on page');
    if (selectResult.error === 'option_not_found') throw new Error('Facility ' + facilityId + ' not in dropdown');

    // Wait a human-like beat
    await this.sleep(50 + Math.random() * 10);

    // Wait for the AJAX response that the dropdown selection triggered
    const daysJson = await daysJsonPromise;

    if (daysJson !== null) {
      // Check for error signals from the intercepted response
      if (daysJson._error) {
        throw new Error(daysJson._error);
      }

      // We intercepted the AJAX response — use it directly
      if (Array.isArray(daysJson)) {
        this.log('debug', 'Got ' + daysJson.length + ' dates from intercepted AJAX response.');
        return daysJson;
      }
      // If the response was a redirect to sign_in (returned as HTML)
      if (typeof daysJson === 'string' && daysJson.includes && daysJson.includes('sign_in')) {
        throw new Error('SESSION_EXPIRED');
      }
      return [];
    }

    // Fallback: try to extract dates from the rendered datepicker
    this.log('debug', 'No AJAX intercept, extracting from DOM...');
    await this.sleep(500); // give calendar time to render

    const domDates = await this.page.evaluate(() => {
      // The datepicker uses jQuery UI — available dates have class "ui-datepicker-current-day" or
      // are <td> elements with data-handler="selectDay" that aren't disabled
      const results = [];
      const cells = document.querySelectorAll('.ui-datepicker td[data-handler="selectDay"]');
      cells.forEach(cell => {
        const a = cell.querySelector('a');
        if (!a) return;
        const day = a.textContent.trim();
        // Get month and year from the datepicker header
        const picker = cell.closest('.ui-datepicker');
        const monthEl = picker && picker.querySelector('.ui-datepicker-month');
        const yearEl = picker && picker.querySelector('.ui-datepicker-year');
        if (monthEl && yearEl) {
          const month = monthEl.textContent.trim();
          const year = yearEl.textContent.trim();
          const dateStr = new Date(month + ' ' + day + ', ' + year).toISOString().split('T')[0];
          if (dateStr && dateStr !== 'Invalid Date') {
            results.push({ date: dateStr, business_day: true });
          }
        }
      });
      return results;
    });

    if (domDates.length > 0) {
      this.log('debug', 'Extracted ' + domDates.length + ' dates from datepicker DOM.');
      return domDates;
    }

    // Check if session expired
    const pageContent = await this.page.content();
    if (pageContent.includes('sign_in') || pageContent.includes('Sign In')) {
      throw new Error('SESSION_EXPIRED');
    }

    // The calendar might show "No appointments available" or similar
    this.log('debug', 'No dates found in DOM for facility ' + facilityId);
    return [];
  }

  // ============================================================
  // CHECK TIMES — DOM-based (simulates user clicking a date)
  // After checkDates, the calendar is already loaded.
  // We click the target date in the datepicker, then wait for
  // the time dropdown to populate via AJAX.
  // ============================================================
  async checkTimes(facilityId, date) {
    // Ensure we're on the appointment page with the right facility selected
    await this.ensureOnAppointmentPage();

    // Make sure the correct facility is selected
    await this.page.evaluate((facId) => {
      const sel = document.querySelector('#appointments_consulate_appointment_facility_id');
      if (sel && sel.value !== String(facId)) {
        sel.value = String(facId);
        sel.dispatchEvent(new Event('change', { bubbles: true }));
      }
    }, facilityId);

    // Set up intercept for the times.json AJAX
    const timesJsonPromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.page.off('response', handler);
        resolve(null);
      }, 15000);

      const handler = async (response) => {
        const url = response.url();
        if (url.includes('/appointment/times/') && url.includes('.json')) {
          clearTimeout(timeout);
          this.page.off('response', handler);
          try {
            const json = await response.json();
            resolve(json);
          } catch (e) {
            resolve(null);
          }
        }
      };
      this.page.on('response', handler);
    });

    // Set the date in the date input field (simulates datepicker selection)
    // The date input is readonly and set by jQuery UI datepicker
    this.log('debug', 'Setting date ' + date + ' in appointment form...');
    await this.page.evaluate((dateStr) => {
      const dateInput = document.querySelector('#appointments_consulate_appointment_date');
      if (!dateInput) return;
      // Use jQuery datepicker API if available
      if (window.jQuery && jQuery(dateInput).datepicker) {
        jQuery(dateInput).datepicker('setDate', dateStr);
      } else {
        // Fallback: set the value directly and fire change
        dateInput.value = dateStr;
        dateInput.dispatchEvent(new Event('change', { bubbles: true }));
      }
    }, date);

    await this.sleep(300 + Math.random() * 300);

    // Wait for the times AJAX
    const timesJson = await timesJsonPromise;

    if (timesJson !== null) {
      const times = (timesJson && timesJson.available_times) ? timesJson.available_times : (Array.isArray(timesJson) ? timesJson : []);
      this.log('debug', 'Got ' + times.length + ' time slots from intercepted AJAX.');
      return times;
    }

    // Fallback: extract times from the select dropdown
    this.log('debug', 'No AJAX intercept for times, extracting from DOM...');
    await this.sleep(3000);

    const domTimes = await this.page.evaluate(() => {
      const sel = document.querySelector('#appointments_consulate_appointment_time');
      if (!sel) return [];
      const times = [];
      sel.querySelectorAll('option').forEach(opt => {
        const val = opt.value ? opt.value.trim() : '';
        if (val) times.push(val);
      });
      return times;
    });

    return domTimes;
  }

  // ============================================================
  // BOOK APPOINTMENT — form-based (simulates user submitting)
  // Instead of a raw POST, we fill out the form on the
  // appointment page and submit it like a real user would.
  // ============================================================
  async bookAppointment(facilityId, date, time, attemptNum) {
    attemptNum = attemptNum || 1;
    this.log('info', '📝 Booking attempt #' + attemptNum + ': facility=' + facilityId + ' date=' + date + ' time=' + time);

    // Navigate to the appointment page fresh for each booking attempt
    const apptUrl = BASE_URL + '/' + this.config.country + '/niv/schedule/' + this.config.scheduleId + '/appointment';
    try {
      await this.page.goto(apptUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await this.sleep(1500 + Math.random() * 1000);
      await this.extractCsrf();
    } catch (err) {
      throw new Error('Could not load appointment page for booking: ' + err.message);
    }

    const currentUrl = this.page.url();
    if (currentUrl.includes('sign_in')) {
      return { success: false, verified: false, reason: 'Session expired during booking', date, time, facilityId };
    }

    // Wait for the form to be ready
    try {
      await this.page.waitForSelector('#appointments_consulate_appointment_facility_id', { timeout: 10000 });
    } catch (e) {
      return { success: false, verified: false, reason: 'Appointment form not loaded', date, time, facilityId };
    }

    // Step 1: Select the facility
    this.log('debug', 'Booking: selecting facility ' + facilityId);
    await this.page.select('#appointments_consulate_appointment_facility_id', String(facilityId));
    await this.sleep(1000 + Math.random() * 500);

    // Step 2: Wait for the date/time section to appear
    try {
      await this.page.waitForSelector('#consulate_date_time', { visible: true, timeout: 15000 });
    } catch (e) {
      this.log('debug', 'Date/time section did not appear, trying to continue...');
    }

    // Step 3: Set the date using jQuery datepicker
    this.log('debug', 'Booking: setting date ' + date);
    await this.page.evaluate((dateStr) => {
      const dateInput = document.querySelector('#appointments_consulate_appointment_date');
      if (!dateInput) return;
      if (window.jQuery && jQuery(dateInput).datepicker) {
        jQuery(dateInput).datepicker('setDate', dateStr);
      } else {
        dateInput.value = dateStr;
        dateInput.dispatchEvent(new Event('change', { bubbles: true }));
      }
    }, date);
    await this.sleep(1500 + Math.random() * 500);

    // Step 4: Wait for time slots to load, then select the time
    this.log('debug', 'Booking: selecting time ' + time);
    // Wait for the time <select> to have options
    try {
      await this.page.waitForFunction(
        () => {
          const sel = document.querySelector('#appointments_consulate_appointment_time');
          return sel && sel.options.length > 1;
        },
        { timeout: 10000 }
      );
    } catch (e) {
      this.log('debug', 'Time options did not populate, attempting to set directly...');
    }

    // Try native page.select, fall back to evaluate
    try {
      await this.page.select('#appointments_consulate_appointment_time', time);
    } catch (e) {
      await this.page.evaluate((t) => {
        const sel = document.querySelector('#appointments_consulate_appointment_time');
        if (sel) {
          // Add the option if it doesn't exist
          let found = false;
          for (let i = 0; i < sel.options.length; i++) {
            if (sel.options[i].value === t) { found = true; break; }
          }
          if (!found) {
            const opt = document.createElement('option');
            opt.value = t;
            opt.text = t;
            sel.add(opt);
          }
          sel.value = t;
          sel.dispatchEvent(new Event('change', { bubbles: true }));
        }
      }, time);
    }
    await this.sleep(500 + Math.random() * 300);

    // Step 5: Enable and click the submit button
    this.log('info', 'Booking: submitting form...');
    await this.page.evaluate(() => {
      const submitBtn = document.querySelector('#appointments_submit');
      if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.removeAttribute('disabled');
      }
    });
    await this.sleep(200);

    // Listen for navigation after submit
    const navPromise = this.page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => null);

    try {
      await this.page.click('#appointments_submit');
    } catch (e) {
      // Fallback: submit the form directly
      await this.page.evaluate(() => {
        const form = document.querySelector('#appointment-form');
        if (form) form.submit();
      });
    }

    await navPromise;
    await this.sleep(2000);

    // Step 6: Check the result
    const html = await this.page.content();
    const lowerHtml = html.toLowerCase();
    const resultUrl = this.page.url();

    let confirmed = false;
    let failReason = null;

    if (lowerHtml.includes('successfully scheduled') || lowerHtml.includes('successfully booked') || lowerHtml.includes('your appointment has been scheduled')) {
      confirmed = true;
    }

    // Check if we're on the confirmation/instructions page (step 5)
    if (!confirmed && (resultUrl.includes('/instructions') || lowerHtml.includes('instructions') && lowerHtml.includes('your appointment'))) {
      confirmed = true;
    }

    if (!confirmed) {
      if (lowerHtml.includes('no longer available') || lowerHtml.includes('no appointment available')) {
        failReason = 'Slot no longer available';
      } else if (lowerHtml.includes('there was a problem') || lowerHtml.includes('could not be processed')) {
        failReason = 'Server problem processing booking';
      } else if (lowerHtml.includes('sign_in')) {
        failReason = 'Session expired during booking';
      } else if (lowerHtml.includes('appointments[consulate_appointment][facility_id]') && resultUrl.includes('/appointment')) {
        failReason = 'Still on appointment form (booking not processed)';
      }
    }

    if (!confirmed && !failReason) {
      if (lowerHtml.includes('appointments[consulate_appointment][facility_id]') || lowerHtml.includes('reschedule appointment')) {
        failReason = 'Still on appointment form (booking not processed)';
      }
    }

    if (confirmed) {
      this.log('success', '🎉 BOOKING VERIFIED! (attempt #' + attemptNum + ')');
      return { success: true, verified: true, date, time, facilityId };
    }

    if (failReason) {
      this.log('warn', '❌ Booking NOT confirmed (attempt #' + attemptNum + '): ' + failReason);
      return { success: false, verified: false, reason: failReason, date, time, facilityId };
    }

    this.log('warn', '⚠️ Booking result UNCLEAR (attempt #' + attemptNum + ')');
    return { success: false, verified: false, reason: 'Ambiguous response', date, time, facilityId };
  }

  // ============================================================
  // DATE FILTERING
  // ============================================================
  filterDatesInRange(dates) {
    const start = new Date(this.config.startDate);
    const end = new Date(this.config.endDate);
    return dates
      .filter(d => d.business_day)
      .map(d => d.date)
      .filter(dateStr => {
        const d = new Date(dateStr);
        return d >= start && d <= end;
      })
      .sort();
  }

  // ============================================================
  // SINGLE CHECK CYCLE
  // ============================================================
  async runCheckCycle() {
    if (!this.running) return 'STOPPED';

    this.health.totalChecks++;
    this.health.lastCheckAt = new Date().toISOString();
    let anySuccess = false;
    let socketFailCount = 0;
    let lastError = null;

    const facilityIds = this.config.facilityIds;
    if (!facilityIds || facilityIds.length === 0) {
      this.log('warn', 'No facility IDs configured. Skipping cycle.');
      return 'CONTINUE';
    }

    // Get location names from cache
    const cachedLocs = db.getCachedLocations(this.jobId);
    const locationMap = {};
    cachedLocs.forEach(l => { locationMap[l.id] = l.name; });

    for (let i = 0; i < facilityIds.length; i++) {
      if (!this.running) return 'STOPPED';

      const facId = facilityIds[i];
      const facName = locationMap[facId] || ('Facility ' + facId);

      // Human-like pause between facility checks (not needed for the first one)
      if (i > 0) {
        const pauseMs = 100 + Math.random() * 30;
        this.log('debug', 'Pausing ' + Math.round(pauseMs / 1000) + 's before next facility...');
        await this.sleep(pauseMs);
      }

      try {
        const dates = await this.checkDates(facId);
        anySuccess = true;
        socketFailCount = 0; // reset on success
        const matching = this.filterDatesInRange(dates);

        if (matching.length > 0) {
          this.log('success', '🎯 ' + facName + ': ' + matching.length + ' date(s) in range! → ' + matching.slice(0, 5).join(', '));

          if (this.config.autoBook) {
            let booked = false;
            for (let d = 0; d < Math.min(matching.length, 3) && !booked; d++) {
              const targetDate = matching[d];
              for (let attempt = 1; attempt <= 3 && !booked; attempt++) {
                if (!this.running) return 'STOPPED';
                try {
                  this.log('info', 'Getting time slots for ' + targetDate + '...');
                  const times = await this.checkTimes(facId, targetDate);
                  if (times.length === 0) {
                    this.log('warn', 'No time slots for ' + targetDate);
                    break;
                  }

                  this.log('info', 'Booking ' + targetDate + ' ' + times[0] + '...');
                  const result = await this.bookAppointment(facId, targetDate, times[0], attempt);

                  if (result.success && result.verified) {
                    this.log('success', '═══════════════════════════════════════════');
                    this.log('success', '  🎉 APPOINTMENT BOOKED & VERIFIED!');
                    this.log('success', '  Location: ' + facName);
                    this.log('success', '  Date: ' + result.date);
                    this.log('success', '  Time: ' + result.time);
                    this.log('success', '═══════════════════════════════════════════');
                    booked = true;

                    db.updateJob(this.jobId, {
                      status: 'booked',
                      bookedDate: result.date,
                      bookedTime: result.time,
                      bookedFacility: facName + ' (' + facId + ')',
                      bookedAt: new Date().toISOString()
                    });

                    return 'BOOKED';
                  } else {
                    this.log('warn', 'Booking attempt #' + attempt + ' failed: ' + (result.reason || 'unknown'));
                    if (result.reason && result.reason.includes('Session expired')) {
                      await this.login();
                    }
                    if (attempt < 3) await this.sleep(500);
                  }
                } catch (bookErr) {
                  this.log('error', 'Booking error (attempt #' + attempt + '): ' + bookErr.message);
                  if (bookErr.message === 'SESSION_EXPIRED') {
                    try { await this.login(); } catch (e) { this.log('error', 'Re-login failed: ' + e.message); }
                  }
                  if (attempt < 3) await this.sleep(500);
                }
              }
            }
            if (!booked) {
              this.log('warn', '⚠️ All booking attempts failed for ' + facName);
            }
          }
        } else {
          const total = dates.length;
          const nearest = dates.length > 0 ? dates[0].date : 'none';
          this.log('info', facName + ': ' + total + ' total dates, 0 in range. Nearest: ' + nearest);
        }

      } catch (err) {
        lastError = err.message;

        // ── Socket hang-up / connection reset — possible IP-level block ──
        if (err._isSocketError && err._retriesExhausted) {
          socketFailCount++;
          this.log('warn', facName + ': socket error after all retries (' + socketFailCount + '/' + facilityIds.length + ' facilities affected)');

          if (socketFailCount >= facilityIds.length) {
            this.log('error', '🚫 ALL facilities returning socket errors — IP-level block detected.');
            this.health.failedChecks++;
            this.health.consecutiveFailures++;
            this.health.lastError = 'IP_BLOCKED';
            this.syncHealth();
            return 'IP_BLOCKED';
          }

          const pauseMs = 8000 + Math.random() * 4000;
          this.log('info', 'Pausing ' + Math.round(pauseMs / 1000) + 's before next facility...');
          await this.sleep(pauseMs);
          continue;
        }

        this.log('error', facName + ': ' + err.message);

        if (err.message === 'SESSION_EXPIRED') {
          this.log('warn', 'Session expired. Re-logging in...');
          try {
            await this.login();
            i--;
            continue;
          } catch (loginErr) {
            this.log('error', 'Re-login failed: ' + loginErr.message);
            return 'LOGIN_FAILED';
          }
        } else if (err.message === 'RATE_LIMITED') {
          this.log('warn', '🚦 Rate limited (429)! Waiting 5 minutes...');
          await this.sleep(5 * 60 * 1000);
        } else if (err.message === 'CSRF_EXPIRED') {
          this.log('warn', 'CSRF expired. Refreshing...');
          try { await this.fetchLocations(); } catch (e) { /* ignore */ }
        }
      }
    }

    if (anySuccess) {
      this.health.successfulChecks++;
      this.health.consecutiveFailures = 0;
    } else {
      this.health.failedChecks++;
      this.health.consecutiveFailures++;
      this.health.lastError = lastError;
    }

    if (this.health.totalChecks % 5 === 0) {
      this.syncHealth();
    }

    return 'CONTINUE';
  }

  // ============================================================
  // START — main entry point
  // ============================================================
  async start() {
    if (this.running) {
      this.log('warn', 'Already running.');
      return;
    }

    await loadModules();

    // Reload config from DB
    const job = db.getJob(this.jobId);
    if (!job) {
      this.log('error', 'Job not found in database.');
      return;
    }

    this.config = {
      email: job.email,
      password: job.password,
      scheduleId: job.scheduleId,
      country: job.country,
      facilityIds: job.facilityIds,
      startDate: job.startDate,
      endDate: job.endDate,
      checkIntervalSeconds: job.checkIntervalSeconds,
      intervalSchedule: job.intervalSchedule || [],
      autoBook: job.autoBook,
      maxReloginAttempts: job.maxReloginAttempts,
      requestTimeoutMs: job.requestTimeoutMs,
      maxRetries: job.maxRetries
    };

    this.running = true;
    this.stopping = false;
    this.health.startedAt = new Date().toISOString();

    db.updateJob(this.jobId, { status: 'running', startedAt: this.health.startedAt });
    this.log('info', '🚀 Starting scheduler for ' + this.config.email + ' (Puppeteer mode)');

    // Login (launches browser + navigates to login page)
    try {
      await this.login();
    } catch (err) {
      this.log('error', 'Initial login failed: ' + err.message);
      db.updateJob(this.jobId, { status: 'error', lastError: err.message });
      this.running = false;
      await this.closePage();
      return;
    }

    // Fetch locations (for cache)
    try {
      const locations = await this.fetchLocations();
      this.log('success', 'Found ' + locations.length + ' locations.');
    } catch (err) {
      this.log('warn', 'Failed to fetch locations: ' + err.message);
    }

    this.log('success', '🔄 Monitoring started.');

    // Main loop
    this.loopPromise = this._runLoop();
  }

  async _runLoop() {
    // Cooldown tiers when IP_BLOCKED is detected (escalates on repeated blocks)
    const BLOCK_COOLDOWNS = [
       5 * 60 * 1000,
      15 * 60 * 1000,
      30 * 60 * 1000,
      60 * 60 * 1000,
    ];
    let blockCount = 0;

    // Interval schedule tracking
    let scheduleStartTime = Date.now();
    let currentScheduleIndex = 0;

    while (this.running) {
      try {
        const result = await this.runCheckCycle();

        if (result === 'BOOKED') {
          this.log('success', 'Appointment booked! Stopping.');
          this.running = false;
          this.syncHealth();
          break;
        }

        if (result === 'STOPPED') {
          break;
        }

        if (result === 'IP_BLOCKED') {
          const cooldownMs = BLOCK_COOLDOWNS[Math.min(blockCount, BLOCK_COOLDOWNS.length - 1)];
          blockCount++;
          this.log('warn', '🕐 IP block #' + blockCount + ' — cooling down for ' + Math.round(cooldownMs / 60000) + ' min...');
          db.updateJob(this.jobId, { lastError: 'IP blocked — cooldown ' + Math.round(cooldownMs / 60000) + 'min (#' + blockCount + ')' });
          await this.sleep(cooldownMs);

          this.log('info', 'Cooldown done. Re-establishing session...');
          try {
            await this.login();
            this.log('success', 'Session refreshed after cooldown.');
            this.health.consecutiveFailures = 0;
          } catch (e) {
            this.log('error', 'Re-login after cooldown failed: ' + e.message);
          }
          // Reset schedule timer after cooldown
          scheduleStartTime = Date.now();
          currentScheduleIndex = 0;
          continue;
        }

        if (result === 'LOGIN_FAILED') {
          if (this.health.consecutiveFailures >= (this.config.maxReloginAttempts || 5)) {
            this.log('warn', 'Max re-login attempts reached. Cooling down 5 minutes...');
            this.health.consecutiveFailures = 0;
            await this.sleep(300000);
          } else {
            this.log('warn', 'Will retry login in 60 seconds...');
            await this.sleep(60000);
          }
          try {
            await this.login();
            this.log('success', 'Re-login succeeded.');
            this.health.consecutiveFailures = 0;
          } catch (e) {
            this.log('error', 'Re-login failed: ' + e.message);
            this.health.consecutiveFailures++;
          }
          continue;
        }

      } catch (loopErr) {
        this.log('error', 'Unexpected error (recovered): ' + loopErr.message);
        this.health.consecutiveFailures++;
        if (this.health.consecutiveFailures >= 10) {
          this.log('warn', 'Too many failures. Cooling down 5 minutes...');
          await this.sleep(300000);
          try {
            await this.login();
            this.health.consecutiveFailures = 0;
          } catch (e) { /* ignore */ }
        }
      }

      if (!this.running) break;

      // ── Calculate next interval based on schedule ──
      let intervalSeconds = this.config.checkIntervalSeconds || 30;

      if (this.config.intervalSchedule && this.config.intervalSchedule.length > 0) {
        const schedule = this.config.intervalSchedule;
        const elapsedMinutes = (Date.now() - scheduleStartTime) / 60000;

        // Find which schedule phase we're in
        let accumulatedMinutes = 0;
        for (let i = 0; i < schedule.length; i++) {
          const phase = schedule[i];
          accumulatedMinutes += phase.durationMinutes;

          if (elapsedMinutes < accumulatedMinutes) {
            intervalSeconds = phase.seconds;
            if (i !== currentScheduleIndex) {
              currentScheduleIndex = i;
              this.log('info', '⏱️ Interval schedule: switching to ' + intervalSeconds + 's for next ' + phase.durationMinutes + ' min');
            }
            break;
          }
        }

        // If we've gone through the entire schedule, loop back to the beginning
        if (elapsedMinutes >= accumulatedMinutes) {
          scheduleStartTime = Date.now();
          currentScheduleIndex = 0;
          intervalSeconds = schedule[0].seconds;
          this.log('info', '🔄 Interval schedule: restarting cycle — ' + intervalSeconds + 's for ' + schedule[0].durationMinutes + ' min');
        }
      }

      // Wait for next cycle with ±10% jitter
      const interval = intervalSeconds * 1000;
      const jitter = interval * 0.1 * (Math.random() - 0.5);
      const waitMs = Math.max(3000, interval + jitter);
      await this.sleep(waitMs);
    }

    this.syncHealth();
    await this.closePage();
  }

  // ============================================================
  // STOP
  // ============================================================
  async stop() {
    if (!this.running) return;
    this.log('info', '⏹️ Stopping scheduler...');
    this.running = false;
    this.stopping = true;
    this.cancelSleep();

    if (this.loopPromise) {
      try {
        await Promise.race([this.loopPromise, new Promise(r => setTimeout(r, 5000))]);
      } catch (e) { /* ignore */ }
    }

    this.syncHealth();
    await this.closePage();
    db.updateJob(this.jobId, { status: 'stopped' });
    this.log('info', '⏹️ Scheduler stopped.');
  }

  // ============================================================
  // GET STATUS
  // ============================================================
  getStatus() {
    return {
      running: this.running,
      health: { ...this.health }
    };
  }
}

module.exports = { SchedulerInstance, loadModules };
