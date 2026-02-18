// ============================================================
// EXPRESS SERVER - REST API + Static frontend
// ============================================================

const express = require('express');
const path = require('path');
const db = require('./database');
const jobManager = require('./job-manager');

const app = express();
const PORT = process.env.PORT || 3456;

// Middleware
app.use(express.json());
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

// ── SPA fallback ──
app.get('*', (req, res) => {
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
