// ============================================================
// DATABASE LAYER - SQLite with better-sqlite3
// ============================================================
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

const DB_PATH = path.join(__dirname, 'data', 'scheduler.db');

let db;

function initDatabase() {
  // Ensure data directory exists
  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  // Create tables
  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      password TEXT NOT NULL,
      schedule_id TEXT NOT NULL,
      country TEXT NOT NULL DEFAULT 'en-ca',
      facility_ids TEXT NOT NULL DEFAULT '[]',
      start_date TEXT NOT NULL,
      end_date TEXT NOT NULL,
      check_interval_seconds INTEGER NOT NULL DEFAULT 30,
      auto_book INTEGER NOT NULL DEFAULT 1,
      max_relogin_attempts INTEGER NOT NULL DEFAULT 5,
      request_timeout_ms INTEGER NOT NULL DEFAULT 20000,
      max_retries INTEGER NOT NULL DEFAULT 3,
      status TEXT NOT NULL DEFAULT 'stopped',
      booked_date TEXT,
      booked_time TEXT,
      booked_facility TEXT,
      booked_at TEXT,
      total_checks INTEGER NOT NULL DEFAULT 0,
      successful_checks INTEGER NOT NULL DEFAULT 0,
      failed_checks INTEGER NOT NULL DEFAULT 0,
      consecutive_failures INTEGER NOT NULL DEFAULT 0,
      relogin_count INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      last_check_at TEXT,
      started_at TEXT,
      droplet_id TEXT,
      droplet_ip TEXT,
      droplet_status TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS job_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL,
      level TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);
    CREATE INDEX IF NOT EXISTS idx_job_logs_created_at ON job_logs(created_at);
    CREATE INDEX IF NOT EXISTS idx_job_logs_level ON job_logs(level);

    CREATE TABLE IF NOT EXISTS locations_cache (
      job_id TEXT NOT NULL,
      facility_id TEXT NOT NULL,
      name TEXT NOT NULL,
      cached_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (job_id, facility_id),
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );
  `);

  // Migrate existing DBs: add droplet columns if missing
  const cols = db.prepare("PRAGMA table_info(jobs)").all().map(c => c.name);
  if (!cols.includes('droplet_id'))     db.exec("ALTER TABLE jobs ADD COLUMN droplet_id TEXT");
  if (!cols.includes('droplet_ip'))     db.exec("ALTER TABLE jobs ADD COLUMN droplet_ip TEXT");
  if (!cols.includes('droplet_status')) db.exec("ALTER TABLE jobs ADD COLUMN droplet_status TEXT");
  if (!cols.includes('interval_schedule')) db.exec("ALTER TABLE jobs ADD COLUMN interval_schedule TEXT");

  return db;
}

// ── Job CRUD ──

function createJob(data) {
  const id = uuidv4().substring(0, 8);
  const stmt = db.prepare(`
    INSERT INTO jobs (id, name, email, password, schedule_id, country, facility_ids,
      start_date, end_date, check_interval_seconds, interval_schedule, auto_book,
      max_relogin_attempts, request_timeout_ms, max_retries)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  stmt.run(
    id,
    data.name || 'Job ' + id,
    data.email,
    data.password,
    data.scheduleId,
    data.country || 'en-ca',
    JSON.stringify(data.facilityIds || []),
    data.startDate,
    data.endDate,
    data.checkIntervalSeconds || 30,
    JSON.stringify(data.intervalSchedule || []),
    data.autoBook !== false ? 1 : 0,
    data.maxReloginAttempts || 5,
    data.requestTimeoutMs || 20000,
    data.maxRetries || 3
  );
  return getJob(id);
}

function getJob(id) {
  const row = db.prepare('SELECT * FROM jobs WHERE id = ?').get(id);
  if (!row) return null;
  return formatJob(row);
}

function getAllJobs() {
  const rows = db.prepare('SELECT * FROM jobs ORDER BY created_at DESC').all();
  return rows.map(formatJob);
}

function updateJob(id, data) {
  const fields = [];
  const values = [];

  const allowedFields = {
    name: 'name', email: 'email', password: 'password',
    scheduleId: 'schedule_id', country: 'country',
    startDate: 'start_date', endDate: 'end_date',
    checkIntervalSeconds: 'check_interval_seconds',
    intervalSchedule: 'interval_schedule',
    autoBook: 'auto_book',
    maxReloginAttempts: 'max_relogin_attempts',
    requestTimeoutMs: 'request_timeout_ms',
    maxRetries: 'max_retries',
    facilityIds: 'facility_ids',
    status: 'status',
    bookedDate: 'booked_date', bookedTime: 'booked_time',
    bookedFacility: 'booked_facility', bookedAt: 'booked_at',
    totalChecks: 'total_checks', successfulChecks: 'successful_checks',
    failedChecks: 'failed_checks', consecutiveFailures: 'consecutive_failures',
    reloginCount: 'relogin_count', lastError: 'last_error',
    lastCheckAt: 'last_check_at', startedAt: 'started_at',
    dropletId: 'droplet_id', dropletIp: 'droplet_ip', dropletStatus: 'droplet_status'
  };

  for (const [key, col] of Object.entries(allowedFields)) {
    if (data[key] !== undefined) {
      fields.push(col + ' = ?');
      let val = data[key];
      if (key === 'facilityIds' || key === 'intervalSchedule') val = JSON.stringify(val);
      if (key === 'autoBook') val = val ? 1 : 0;
      values.push(val);
    }
  }

  if (fields.length === 0) return getJob(id);

  fields.push("updated_at = datetime('now')");
  values.push(id);

  db.prepare('UPDATE jobs SET ' + fields.join(', ') + ' WHERE id = ?').run(...values);
  return getJob(id);
}

function deleteJob(id) {
  db.prepare('DELETE FROM jobs WHERE id = ?').run(id);
}

function formatJob(row) {
  return {
    id: row.id,
    name: row.name,
    email: row.email,
    password: row.password,
    scheduleId: row.schedule_id,
    country: row.country,
    facilityIds: JSON.parse(row.facility_ids || '[]'),
    startDate: row.start_date,
    endDate: row.end_date,
    checkIntervalSeconds: row.check_interval_seconds,
    intervalSchedule: JSON.parse(row.interval_schedule || '[]'),
    autoBook: !!row.auto_book,
    maxReloginAttempts: row.max_relogin_attempts,
    requestTimeoutMs: row.request_timeout_ms,
    maxRetries: row.max_retries,
    status: row.status,
    bookedDate: row.booked_date,
    bookedTime: row.booked_time,
    bookedFacility: row.booked_facility,
    bookedAt: row.booked_at,
    totalChecks: row.total_checks,
    successfulChecks: row.successful_checks,
    failedChecks: row.failed_checks,
    consecutiveFailures: row.consecutive_failures,
    reloginCount: row.relogin_count,
    lastError: row.last_error,
    lastCheckAt: row.last_check_at,
    startedAt: row.started_at,
    dropletId: row.droplet_id,
    dropletIp: row.droplet_ip,
    dropletStatus: row.droplet_status,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

// ── Logs ──

function addLog(jobId, level, message) {
  db.prepare('INSERT INTO job_logs (job_id, level, message) VALUES (?, ?, ?)').run(jobId, level, message);
}

function getLogs(jobId, opts = {}) {
  const { limit = 200, offset = 0, level, since } = opts;
  let sql = 'SELECT * FROM job_logs WHERE job_id = ?';
  const params = [jobId];

  if (level) {
    sql += ' AND level = ?';
    params.push(level);
  }
  if (since) {
    sql += ' AND created_at > ?';
    params.push(since);
  }

  sql += ' ORDER BY id DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  return db.prepare(sql).all(params).reverse();
}

function clearLogs(jobId) {
  db.prepare('DELETE FROM job_logs WHERE job_id = ?').run(jobId);
}

// ── Locations Cache ──

function cacheLocations(jobId, locations) {
  const del = db.prepare('DELETE FROM locations_cache WHERE job_id = ?');
  const ins = db.prepare('INSERT INTO locations_cache (job_id, facility_id, name) VALUES (?, ?, ?)');
  const tx = db.transaction((locs) => {
    del.run(jobId);
    for (const loc of locs) {
      ins.run(jobId, loc.id, loc.name);
    }
  });
  tx(locations);
}

function getCachedLocations(jobId) {
  return db.prepare('SELECT facility_id as id, name FROM locations_cache WHERE job_id = ?').all(jobId);
}

// ── Cleanup old logs ──

function cleanupOldLogs(daysToKeep = 7) {
  db.prepare("DELETE FROM job_logs WHERE created_at < datetime('now', '-' || ? || ' days')").run(daysToKeep);
}

module.exports = {
  initDatabase,
  createJob,
  getJob,
  getAllJobs,
  updateJob,
  deleteJob,
  addLog,
  getLogs,
  clearLogs,
  cacheLocations,
  getCachedLocations,
  cleanupOldLogs
};
