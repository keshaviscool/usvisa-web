// ============================================================
// EXPRESS SERVER - REST API + Static frontend
// ============================================================

// Load .env if present
const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  fs.readFileSync(envPath, 'utf8').split('\n').forEach(line => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) return;
    const idx = trimmed.indexOf('=');
    if (idx === -1) return;
    const k = trimmed.slice(0, idx).trim();
    const v = trimmed.slice(idx + 1).trim();
    if (k && !process.env[k]) process.env[k] = v;
  });
}

const express = require('express');
const session = require('express-session');
const db = require('./database');
const jobManager = require('./job-manager');
const dropletManager = require('./droplet-manager');

const app = express();
const PORT = process.env.PORT || 3456;
const CALLBACK_SECRET = process.env.CALLBACK_SECRET || 'changeme';
const APP_PASSWORD = process.env.APP_PASSWORD || '';
const SESSION_SECRET = process.env.SESSION_SECRET || 'visa-scheduler-secret-' + Math.random().toString(36).slice(2);

if (!APP_PASSWORD) {
  console.warn('[Auth] WARNING: APP_PASSWORD is not set. The dashboard is unprotected!');
}

// ── Session middleware ──
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
  }
}));

// Middleware
app.use(express.json());

// ── Auth guard ──
function requireAuth(req, res, next) {
  if (!APP_PASSWORD) return next(); // no password set → open
  if (req.session && req.session.authenticated) return next();
  // if (req.path.startsWith('/api/')) {
  //   return res.status(401).json({ error: 'Unauthorized' });
  // }
  // For HTML pages, send the index (it will show the login screen)
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
}

// Static assets (CSS, JS, images) served without auth
app.use('/assets', express.static(path.join(__dirname, 'public', 'assets')));

// ── Login / Logout routes ──
app.post('/auth/login', (req, res) => {
  const { password } = req.body;
  if (!APP_PASSWORD || password === APP_PASSWORD) {
    req.session.authenticated = true;
    return res.json({ ok: true });
  }
  res.status(401).json({ error: 'Invalid password' });
});

app.post('/auth/logout', (req, res) => {
  req.session.destroy(() => {
    res.json({ ok: true });
  });
});

app.get('/auth/check', (req, res) => {
  res.json({ authenticated: !APP_PASSWORD || !!(req.session && req.session.authenticated) });
});

// All routes below require auth
app.use(requireAuth);

// Serve public files (index.html etc.) behind auth
app.use(express.static(path.join(__dirname, 'public')));

// ============================================================
// API ROUTES
// ============================================================

// ── Get all jobs ──
app.get('/api/jobs', (req, res) => {
  try {
    const jobs = jobManager.getAllJobs();
    // Strip passwords from response
    const safe = jobs.map(j => ({ ...j, password: '••••••••' }));
    res.json(safe);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Get single job ──
app.get('/api/jobs/:id', (req, res) => {
  try {
    const job = jobManager.getJob(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json({ ...job, password: '••••••••' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Create job ──
app.post('/api/jobs', (req, res) => {
  try {
    const { name, email, password, scheduleId, country, facilityIds,
            startDate, endDate, checkIntervalSeconds, autoBook } = req.body;

    if (!email || !password || !scheduleId) {
      return res.status(400).json({ error: 'email, password, and scheduleId are required.' });
    }
    if (!startDate || !endDate) {
      return res.status(400).json({ error: 'startDate and endDate are required.' });
    }

    const job = jobManager.createJob({
      name, email, password, scheduleId,
      country: country || 'en-ca',
      facilityIds: facilityIds || [],
      startDate, endDate,
      checkIntervalSeconds: checkIntervalSeconds || 30,
      autoBook: autoBook !== false
    });

    res.status(201).json({ ...job, password: '••••••••' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Update job ──
app.put('/api/jobs/:id', (req, res) => {
  try {
    const job = jobManager.updateJob(req.params.id, req.body);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json({ ...job, password: '••••••••' });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// ── Delete job ──
app.delete('/api/jobs/:id', async (req, res) => {
  try {
    await jobManager.deleteJob(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Start job ──
app.post('/api/jobs/:id/start', async (req, res) => {
  try {
    const job = await jobManager.startJob(req.params.id);
    res.json({ ...job, password: '••••••••' });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// ── Stop job ──
app.post('/api/jobs/:id/stop', async (req, res) => {
  try {
    const job = await jobManager.stopJob(req.params.id);
    res.json({ ...job, password: '••••••••' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Fetch locations ──
app.post('/api/jobs/:id/fetch-locations', async (req, res) => {
  try {
    const locations = await jobManager.fetchLocations(req.params.id);
    res.json({ locations });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch locations: ' + err.message });
  }
});

// ── Get cached locations ──
app.get('/api/jobs/:id/locations', (req, res) => {
  try {
    const locations = jobManager.getCachedLocations(req.params.id);
    res.json({ locations });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Get logs ──
app.get('/api/jobs/:id/logs', (req, res) => {
  try {
    const { limit, level, since } = req.query;
    const logs = jobManager.getLogs(req.params.id, {
      limit: limit ? parseInt(limit) : 200,
      level: level || undefined,
      since: since || undefined
    });
    res.json({ logs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Clear logs ──
app.delete('/api/jobs/:id/logs', (req, res) => {
  try {
    jobManager.clearLogs(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Reset booking ──
app.post('/api/jobs/:id/reset', (req, res) => {
  try {
    const job = jobManager.resetBooking(req.params.id);
    res.json({ ...job, password: '••••••••' });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// ── Droplet mode status ──
app.get('/api/droplet-mode', (req, res) => {
  res.json({ enabled: dropletManager.isEnabled() });
});

// ============================================================
// CALLBACK ROUTES - Called by droplet agents
// ============================================================

function requireCallbackSecret(req, res, next) {
  const secret = req.headers['x-callback-secret'];
  if (secret !== CALLBACK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ── Droplet log callback ──
app.post('/api/callback/log', requireCallbackSecret, (req, res) => {
  const { jobId, level, message } = req.body;
  if (!jobId || !level || !message) return res.status(400).json({ error: 'Missing fields' });
  try {
    db.addLog(jobId, level, '[droplet] ' + message);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Droplet status callback ──
app.post('/api/callback/status', requireCallbackSecret, (req, res) => {
  const { jobId, status, ...rest } = req.body;
  if (!jobId) return res.status(400).json({ error: 'Missing jobId' });
  try {
    const update = {};
    const allowed = [
      'status', 'lastError', 'totalChecks', 'successfulChecks', 'failedChecks',
      'consecutiveFailures', 'reloginCount', 'lastCheckAt', 'startedAt',
      'bookedDate', 'bookedTime', 'bookedFacility', 'bookedAt'
    ];
    if (status) update.status = status;
    for (const key of allowed) {
      if (rest[key] !== undefined) update[key] = rest[key];
    }
    if (Object.keys(update).length) db.updateJob(jobId, update);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Droplet destroy callback (agent signals it's done) ──
app.post('/api/callback/destroy', requireCallbackSecret, async (req, res) => {
  const { jobId } = req.body;
  if (!jobId) return res.status(400).json({ error: 'Missing jobId' });
  res.json({ ok: true }); // respond immediately, destroy in background
  try {
    await jobManager.destroyJobDroplet(jobId);
  } catch (err) {
    console.error('[Server] Failed to destroy droplet for job ' + jobId + ':', err.message);
  }
});

// ── SPA fallback ──
app.get('*', (req, res) => {
  // Callback routes should not fall through to SPA
  if (req.path.startsWith('/api/callback/')) return res.status(404).end();
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ============================================================
// START SERVER
// ============================================================
async function startServer() {
  // Init database
  db.initDatabase();

  // Init job manager
  await jobManager.init();

  app.listen(PORT, () => {
    console.log('');
    console.log('  ╔═══════════════════════════════════════════╗');
    console.log('  ║  🇺🇸 US Visa Scheduler - Web Dashboard    ║');
    console.log('  ║  http://localhost:' + PORT + '                   ║');
    console.log('  ╚═══════════════════════════════════════════╝');
    console.log('');
  });

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\nShutting down...');
    await jobManager.shutdown();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

startServer().catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
