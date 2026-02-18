// ============================================================
// SCHEDULER ENGINE - Isolated per-job scheduler instance
// Refactored from standalone/scheduler.js to support multiple
// concurrent instances with independent sessions.
// ============================================================

const db = require('./database');

// ESM module references (shared across instances)
let fetchModule, CookieJarClass, fetchCookieFactory, cheerioModule;
let modulesLoaded = false;

async function loadModules() {
  if (modulesLoaded) return;
  const fm = await import('node-fetch');
  fetchModule = fm.default;
  const tc = await import('tough-cookie');
  CookieJarClass = tc.CookieJar;
  const fc = await import('fetch-cookie');
  fetchCookieFactory = fc.default;
  cheerioModule = await import('cheerio');
  modulesLoaded = true;
}

const BASE_URL = 'https://ais.usvisa-info.com';
const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

class SchedulerInstance {
  constructor(jobId) {
    this.jobId = jobId;
    this.running = false;
    this.stopping = false;
    this.cookieJar = null;
    this.fetchWithCookies = null;
    this.csrfToken = null;
    this.config = null;
    this.loopPromise = null;

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
    // Save to DB (skip debug to keep DB lean)
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

  // ── HTTP Helpers ──
  getHeaders(extra) {
    const headers = {
      'User-Agent': USER_AGENT,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-GB,en;q=0.7',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    };
    if (this.csrfToken) headers['X-CSRF-Token'] = this.csrfToken;
    if (extra) Object.assign(headers, extra);
    return headers;
  }

  getJsonHeaders() {
    return this.getHeaders({
      'Accept': 'application/json, text/javascript, */*; q=0.01',
      'X-Requested-With': 'XMLHttpRequest'
    });
  }

  async fetchWithRetry(url, options, maxRetries) {
    maxRetries = maxRetries || this.config.maxRetries || 3;
    let attempt = 0;
    let lastError;
    const method = (options && options.method) || 'GET';

    while (attempt < maxRetries) {
      attempt++;
      this.log('debug', 'HTTP ' + method + ' [attempt ' + attempt + '/' + maxRetries + ']');
      try {
        const controller = new AbortController();
        const timeoutMs = this.config.requestTimeoutMs || 20000;
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
        const opts = Object.assign({}, options || {}, { signal: controller.signal });
        const response = await this.fetchWithCookies(url, opts);
        clearTimeout(timeoutId);
        return response;
      } catch (err) {
        lastError = err;
        this.log('debug', 'HTTP ' + method + ' FAILED: ' + err.message);
        if (attempt < maxRetries) {
          const delay = Math.min(1000 * Math.pow(2, attempt - 1), 10000) + Math.random() * 1000;
          this.log('warn', 'Request failed (' + err.message + '). Retry ' + attempt + '/' + maxRetries + ' in ' + Math.round(delay) + 'ms...');
          await this.sleep(delay);
        }
      }
    }
    throw lastError;
  }

  sleep(ms) {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, ms);
      // Store reference so we can cancel on stop
      this._sleepTimer = timer;
    });
  }

  cancelSleep() {
    if (this._sleepTimer) {
      clearTimeout(this._sleepTimer);
      this._sleepTimer = null;
    }
  }

  // ── Reset session ──
  resetSession() {
    this.cookieJar = new CookieJarClass();
    this.fetchWithCookies = fetchCookieFactory(fetchModule, this.cookieJar);
    this.csrfToken = null;
  }

  // ============================================================
  // LOGIN
  // ============================================================
  async login() {
    this.log('info', 'Logging in as ' + this.config.email + '...');
    this.resetSession();

    const loginUrl = BASE_URL + '/' + this.config.country + '/niv/users/sign_in';

    // Step 1: GET login page for CSRF token
    const pageResp = await this.fetchWithCookies(loginUrl, {
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9'
      },
      redirect: 'follow'
    });
    const html = await pageResp.text();
    this.log('info', 'GET login page: status=' + pageResp.status);

    // Extract CSRF
    let csrfMatch = html.match(/name="authenticity_token"[^>]*value="([^"]+)"/);
    this.csrfToken = csrfMatch ? csrfMatch[1] : null;
    if (!this.csrfToken) {
      const metaMatch = html.match(/meta name="csrf-token" content="([^"]+)"/);
      this.csrfToken = metaMatch ? metaMatch[1] : null;
    }
    if (!this.csrfToken) {
      const $ = cheerioModule.load(html);
      this.csrfToken = $('meta[name="csrf-token"]').attr('content') || $('input[name="authenticity_token"]').val();
    }
    if (!this.csrfToken) {
      throw new Error('Could not extract CSRF token from login page');
    }
    this.log('info', 'Got CSRF token.');

    // Step 2: POST login
    const formData = new URLSearchParams();
    formData.append('utf8', '✓');
    formData.append('user[email]', this.config.email);
    formData.append('user[password]', this.config.password);
    formData.append('policy_confirmed', '1');
    formData.append('commit', 'Sign In');

    const loginResp = await this.fetchWithCookies(loginUrl, {
      method: 'POST',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': BASE_URL,
        'Referer': loginUrl,
        'X-CSRF-Token': this.csrfToken,
        'X-Requested-With': 'XMLHttpRequest'
      },
      body: formData.toString(),
      redirect: 'manual'
    });

    const statusCode = loginResp.status;
    this.log('info', 'Login POST: status=' + statusCode);
    const responseBody = await loginResp.text();

    if (statusCode === 200) {
      const locationMatch = responseBody.match(/window\.location\s*=\s*["']([^"']+)["']/);
      if (locationMatch) {
        const redirectPath = locationMatch[1];
        const redirectUrl = redirectPath.startsWith('http') ? redirectPath : (BASE_URL + redirectPath);
        if (!redirectUrl.includes('sign_in')) {
          const redirectResp = await this.fetchWithCookies(redirectUrl, {
            method: 'GET',
            headers: { 'User-Agent': USER_AGENT, 'Accept': 'text/html', 'Referer': loginUrl },
            redirect: 'follow'
          });
          const redirectHtml = await redirectResp.text();
          const newCsrf = redirectHtml.match(/meta name="csrf-token" content="([^"]+)"/);
          if (newCsrf) this.csrfToken = newCsrf[1];
          this.log('success', 'Login successful!');
          this.health.reloginCount++;
          return true;
        }
      }

      if (responseBody.includes('Invalid Email or password') || responseBody.includes('invalid email or password')) {
        throw new Error('Invalid email or password.');
      }
      if (responseBody.includes('try again later') || responseBody.includes('Too many')) {
        throw new Error('Login rate limited. Try again later.');
      }
      if (responseBody.includes('sign_out') || responseBody.includes('dashboard') || responseBody.includes('Groups')) {
        this.log('success', 'Login successful!');
        this.health.reloginCount++;
        return true;
      }

      // Verify by accessing account page
      this.log('info', 'Verifying login...');
      const verifyUrl = BASE_URL + '/' + this.config.country + '/niv/groups/' + this.config.scheduleId;
      const verifyResp = await this.fetchWithCookies(verifyUrl, {
        method: 'GET',
        headers: { 'User-Agent': USER_AGENT, 'Accept': 'text/html', 'Referer': loginUrl },
        redirect: 'follow'
      });
      const verifyHtml = await verifyResp.text();
      const verifyFinalUrl = verifyResp.url || verifyUrl;

      if (!verifyFinalUrl.includes('sign_in') && verifyResp.status === 200) {
        const verifyCsrf = verifyHtml.match(/meta name="csrf-token" content="([^"]+)"/);
        if (verifyCsrf) this.csrfToken = verifyCsrf[1];
        this.log('success', 'Login verified!');
        this.health.reloginCount++;
        return true;
      }
      throw new Error('Login failed - could not access protected pages.');
    }

    // Handle redirects
    if (statusCode === 302 || statusCode === 301 || statusCode === 303) {
      let redirectUrl2 = loginResp.headers.get('location');
      if (redirectUrl2) {
        if (redirectUrl2.startsWith('/')) redirectUrl2 = BASE_URL + redirectUrl2;
        const rResp = await this.fetchWithCookies(redirectUrl2, {
          method: 'GET',
          headers: { 'User-Agent': USER_AGENT, 'Accept': 'text/html', 'Referer': loginUrl },
          redirect: 'follow'
        });
        const rHtml = await rResp.text();
        const rUrl = rResp.url || redirectUrl2;
        const rCsrf = rHtml.match(/meta name="csrf-token" content="([^"]+)"/);
        if (rCsrf) this.csrfToken = rCsrf[1];
        if (!rUrl.includes('sign_in')) {
          this.log('success', 'Login successful!');
          this.health.reloginCount++;
          return true;
        }
        if (rHtml.includes('Invalid Email or password')) {
          throw new Error('Invalid email or password.');
        }
      }
    }

    throw new Error('Login failed - status ' + statusCode);
  }

  // ============================================================
  // FETCH LOCATIONS
  // ============================================================
  async fetchLocations() {
    this.log('info', 'Fetching available locations...');
    const url = BASE_URL + '/' + this.config.country + '/niv/schedule/' + this.config.scheduleId + '/appointment';
    const resp = await this.fetchWithRetry(url, { method: 'GET', headers: this.getHeaders(), redirect: 'follow' });
    const html = await resp.text();

    if (resp.url && resp.url.includes('sign_in')) {
      throw new Error('SESSION_EXPIRED');
    }

    const $ = cheerioModule.load(html);
    const newCsrf = $('meta[name="csrf-token"]').attr('content');
    if (newCsrf) this.csrfToken = newCsrf;

    const locations = [];
    $('select[name="appointments[consulate_appointment][facility_id]"] option').each(function () {
      const val = $(this).val();
      const name = $(this).text().trim();
      if (val && val.trim() !== '') {
        locations.push({ id: val, name: name });
      }
    });

    if (locations.length === 0) {
      if (html.includes('sign_in') || html.includes('Sign In')) {
        throw new Error('SESSION_EXPIRED');
      }
    }

    // Cache locations in DB
    if (locations.length > 0) {
      db.cacheLocations(this.jobId, locations);
    }

    return locations;
  }

  // ============================================================
  // CHECK DATES
  // ============================================================
  async checkDates(facilityId) {
    const url = BASE_URL + '/' + this.config.country + '/niv/schedule/' + this.config.scheduleId + '/appointment/days/' + facilityId + '.json?appointments[expedite]=false';
    const resp = await this.fetchWithRetry(url, { method: 'GET', headers: this.getJsonHeaders(), redirect: 'manual' });

    if (resp.status === 401 || resp.status === 403) throw new Error('SESSION_EXPIRED');
    if (resp.status === 429) throw new Error('RATE_LIMITED');
    if (resp.status === 422) throw new Error('CSRF_EXPIRED');
    if (!resp.ok) throw new Error('HTTP_' + resp.status);

    const text = await resp.text();
    try {
      const data = JSON.parse(text);
      if (!Array.isArray(data)) {
        if (text.includes('sign_in')) throw new Error('SESSION_EXPIRED');
        return [];
      }
      return data;
    } catch (e) {
      if (text.includes('sign_in')) throw new Error('SESSION_EXPIRED');
      throw new Error('PARSE_ERROR: ' + e.message);
    }
  }

  // ============================================================
  // CHECK TIMES
  // ============================================================
  async checkTimes(facilityId, date) {
    const url = BASE_URL + '/' + this.config.country + '/niv/schedule/' + this.config.scheduleId + '/appointment/times/' + facilityId + '.json?date=' + date + '&appointments[expedite]=false';
    const resp = await this.fetchWithRetry(url, { method: 'GET', headers: this.getJsonHeaders(), redirect: 'manual' });

    if (resp.status === 401 || resp.status === 403) throw new Error('SESSION_EXPIRED');
    if (!resp.ok) throw new Error('HTTP_' + resp.status);

    const data = await resp.json();
    const times = (data && data.available_times) ? data.available_times : (Array.isArray(data) ? data : []);
    return times;
  }

  // ============================================================
  // BOOK APPOINTMENT
  // ============================================================
  async bookAppointment(facilityId, date, time, attemptNum) {
    attemptNum = attemptNum || 1;
    this.log('info', '📝 Booking attempt #' + attemptNum + ': facility=' + facilityId + ' date=' + date + ' time=' + time);

    if (!this.csrfToken || attemptNum > 1) {
      const pageUrl = BASE_URL + '/' + this.config.country + '/niv/schedule/' + this.config.scheduleId + '/appointment';
      const pageResp = await this.fetchWithRetry(pageUrl, { method: 'GET', headers: this.getHeaders(), redirect: 'follow' });
      const pageHtml = await pageResp.text();
      const $ = cheerioModule.load(pageHtml);
      const freshCsrf = $('input[name="authenticity_token"]').val() || $('meta[name="csrf-token"]').attr('content');
      if (freshCsrf) this.csrfToken = freshCsrf;
    }

    const formData = new URLSearchParams();
    formData.append('utf8', '\u2713');
    formData.append('authenticity_token', this.csrfToken);
    formData.append('appointments[consulate_appointment][facility_id]', facilityId);
    formData.append('appointments[consulate_appointment][date]', date);
    formData.append('appointments[consulate_appointment][time]', time);
    formData.append('confirmed', 'Confirm');

    const bookUrl = BASE_URL + '/' + this.config.country + '/niv/schedule/' + this.config.scheduleId + '/appointment';
    const resp = await this.fetchWithRetry(bookUrl, {
      method: 'POST',
      headers: this.getHeaders({
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': bookUrl
      }),
      body: formData.toString(),
      redirect: 'follow'
    }, 1);

    const html = await resp.text();
    const lowerHtml = html.toLowerCase();

    let confirmed = false;
    let failReason = null;

    if (lowerHtml.includes('successfully scheduled') || lowerHtml.includes('successfully booked') || lowerHtml.includes('your appointment has been scheduled')) {
      confirmed = true;
    }

    if (!confirmed) {
      if (lowerHtml.includes('no longer available') || lowerHtml.includes('no appointment available')) {
        failReason = 'Slot no longer available';
      } else if (lowerHtml.includes('there was a problem') || lowerHtml.includes('could not be processed')) {
        failReason = 'Server problem processing booking';
      } else if (lowerHtml.includes('sign_in')) {
        failReason = 'Session expired during booking';
      } else if (resp.status === 422) {
        failReason = 'CSRF token expired';
      } else if (resp.status === 401 || resp.status === 403) {
        failReason = 'Session expired (' + resp.status + ')';
      } else if (!resp.ok) {
        failReason = 'HTTP error ' + resp.status;
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

      try {
        const dates = await this.checkDates(facId);
        anySuccess = true;
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

                    // Save booking info to DB
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
        this.log('error', facName + ': ' + err.message);

        if (err.message === 'SESSION_EXPIRED') {
          this.log('warn', 'Session expired. Re-logging in...');
          try {
            await this.login();
            i--; // retry this facility
            continue;
          } catch (loginErr) {
            this.log('error', 'Re-login failed: ' + loginErr.message);
            return 'LOGIN_FAILED';
          }
        } else if (err.message === 'RATE_LIMITED') {
          this.log('warn', 'Rate limited! Waiting 60 seconds...');
          await this.sleep(60000);
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

    // Sync to DB every 5 cycles
    if (this.health.totalChecks % 5 === 0) {
      this.syncHealth();
    }

    return 'CONTINUE';
  }

  // ============================================================
  // START - main loop
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
      autoBook: job.autoBook,
      maxReloginAttempts: job.maxReloginAttempts,
      requestTimeoutMs: job.requestTimeoutMs,
      maxRetries: job.maxRetries
    };

    this.running = true;
    this.stopping = false;
    this.health.startedAt = new Date().toISOString();

    db.updateJob(this.jobId, { status: 'running', startedAt: this.health.startedAt });
    this.log('info', '🚀 Starting scheduler for ' + this.config.email);

    this.resetSession();

    // Login
    try {
      await this.login();
    } catch (err) {
      this.log('error', 'Initial login failed: ' + err.message);
      db.updateJob(this.jobId, { status: 'error', lastError: err.message });
      this.running = false;
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

      // Wait for next cycle
      const interval = (this.config.checkIntervalSeconds || 30) * 1000;
      const jitter = interval * 0.1 * (Math.random() - 0.5);
      const waitMs = Math.max(3000, interval + jitter);
      // await this.sleep(waitMs);
    }

    this.syncHealth();
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

    // Wait for loop to finish current cycle
    if (this.loopPromise) {
      try {
        await Promise.race([this.loopPromise, new Promise(r => setTimeout(r, 5000))]);
      } catch (e) { /* ignore */ }
    }

    this.syncHealth();
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
