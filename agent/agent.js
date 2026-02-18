// ============================================================
// AGENT - Runs on each DigitalOcean droplet
// Reads job config from env, runs SchedulerInstance,
// and POSTs logs + status back to the main VPS.
// ============================================================

// Load .env if present (when run via PM2 on droplet)
const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  fs.readFileSync(envPath, 'utf8').split('\n').forEach(line => {
    const [k, ...rest] = line.split('=');
    if (k && rest.length) process.env[k.trim()] = rest.join('=').trim();
  });
}

const https = require('https');
const http = require('http');

// ── Config from env ──
const JOB_ID          = process.env.JOB_ID;
const MAIN_VPS_URL    = process.env.MAIN_VPS_URL;   // e.g. http://1.2.3.4:3456
const CALLBACK_SECRET = process.env.CALLBACK_SECRET || 'changeme';
const CONFIG_B64      = process.env.JOB_CONFIG_B64;

if (!JOB_ID || !MAIN_VPS_URL || !CONFIG_B64) {
  console.error('[Agent] Missing required env vars: JOB_ID, MAIN_VPS_URL, JOB_CONFIG_B64');
  process.exit(1);
}

const jobConfig = JSON.parse(Buffer.from(CONFIG_B64, 'base64').toString('utf8'));
console.log('[Agent] Starting for job ' + JOB_ID + ' → ' + MAIN_VPS_URL);

// ── HTTP POST helper (supports http and https) ──
function postJson(url, payload) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(payload);
    const parsed = new URL(url);
    const lib = parsed.protocol === 'https:' ? https : http;
    const options = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        'x-callback-secret': CALLBACK_SECRET
      }
    };
    const req = lib.request(options, (res) => {
      res.resume(); // drain
      resolve(res.statusCode);
    });
    req.on('error', (err) => reject(err));
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('timeout')); });
    req.write(data);
    req.end();
  });
}

// ── Send log back to main VPS ──
async function sendLog(level, message) {
  try {
    await postJson(MAIN_VPS_URL + '/api/callback/log', { jobId: JOB_ID, level, message });
  } catch (e) {
    console.error('[Agent] Failed to send log:', e.message);
  }
}

// ── Send status update back to main VPS ──
async function sendStatus(status, extra) {
  try {
    await postJson(MAIN_VPS_URL + '/api/callback/status', { jobId: JOB_ID, status, ...extra });
  } catch (e) {
    console.error('[Agent] Failed to send status:', e.message);
  }
}

// ── Minimal in-process DB shim (no SQLite on droplet) ──
// The agent doesn't need a real DB - it ships data back to main VPS.
const dbShim = {
  addLog(jobId, level, message) {
    sendLog(level, message).catch(() => {});
  },
  updateJob(jobId, data) {
    sendStatus(data.status || 'running', data).catch(() => {});
  },
  getJob(jobId) { return null; },
  getCachedLocations(jobId) { return []; },
  cacheLocations(jobId, locations) {}
};

// Monkey-patch the database module to use the shim
// We do this by injecting the shim before requiring scheduler-engine
const Module = require('module');
const originalLoad = Module._load;
Module._load = function (request, parent, isMain) {
  if (request === './database' || request === '../database') {
    return dbShim;
  }
  return originalLoad.apply(this, arguments);
};

// Now safe to load scheduler-engine
const { SchedulerInstance, loadModules } = require('../scheduler-engine');

// ── Custom SchedulerInstance that uses a pre-loaded config ──
async function runAgent() {
  await loadModules();

  const instance = new SchedulerInstance(JOB_ID);

  // Inject config directly (no DB read needed)
  const originalStart = instance.start.bind(instance);
  instance.start = async function () {
    if (this.running) return;
    await loadModules();

    this.config = {
      email: jobConfig.email,
      password: jobConfig.password,
      scheduleId: jobConfig.scheduleId,
      country: jobConfig.country || 'en-ca',
      facilityIds: jobConfig.facilityIds || [],
      startDate: jobConfig.startDate,
      endDate: jobConfig.endDate,
      checkIntervalSeconds: jobConfig.checkIntervalSeconds || 30,
      autoBook: jobConfig.autoBook !== false,
      maxReloginAttempts: jobConfig.maxReloginAttempts || 5,
      requestTimeoutMs: jobConfig.requestTimeoutMs || 20000,
      maxRetries: jobConfig.maxRetries || 3
    };

    this.running = true;
    this.stopping = false;
    this.health.startedAt = new Date().toISOString();

    await sendStatus('running', { startedAt: this.health.startedAt });
    this.log('info', '🚀 Agent started for job ' + JOB_ID);

    this.resetSession();

    try {
      await this.login();
    } catch (err) {
      this.log('error', 'Initial login failed: ' + err.message);
      await sendStatus('error', { lastError: err.message });
      this.running = false;
      process.exit(1);
    }

    try {
      const locations = await this.fetchLocations();
      this.log('success', 'Found ' + locations.length + ' locations.');
    } catch (err) {
      this.log('warn', 'Failed to fetch locations: ' + err.message);
    }

    this.log('success', '🔄 Monitoring started.');
    this.loopPromise = this._runLoop();
    await this.loopPromise;
  };

  // Run and handle exit
  try {
    await instance.start();
    await sendStatus('stopped', {});
    console.log('[Agent] Job finished cleanly. Signalling main VPS to destroy droplet.');
    await postJson(MAIN_VPS_URL + '/api/callback/destroy', { jobId: JOB_ID });
    setTimeout(() => process.exit(0), 2000);
  } catch (err) {
    console.error('[Agent] Fatal error:', err.message);
    await sendStatus('error', { lastError: err.message });
    await postJson(MAIN_VPS_URL + '/api/callback/destroy', { jobId: JOB_ID });
    setTimeout(() => process.exit(1), 2000);
  }
}

// ── Graceful shutdown on SIGTERM (PM2 stop) ──
process.on('SIGTERM', async () => {
  console.log('[Agent] Received SIGTERM, sending stopped status...');
  await sendStatus('stopped', {}).catch(() => {});
  process.exit(0);
});

runAgent().catch(async (err) => {
  console.error('[Agent] Unhandled:', err);
  await sendStatus('error', { lastError: err.message }).catch(() => {});
  process.exit(1);
});
