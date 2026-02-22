// ============================================================
// JOB MANAGER - Manages all scheduler instances
// Supports local mode (default) and droplet mode (DO_API_TOKEN set)
// ============================================================

const db = require('./database');
const { SchedulerInstance, loadModules } = require('./scheduler-engine');
const dropletManager = require('./droplet-manager');

class JobManager {
  constructor() {
    // Active scheduler instances: jobId → SchedulerInstance
    this.instances = new Map();
  }

  // ── Initialize: restore running jobs on server restart ──
  async init() {
    await loadModules();

    // Any jobs that were 'running' or 'provisioning' when server died → mark as 'stopped'
    const allJobs = db.getAllJobs();
    for (const job of allJobs) {
      if (job.status === 'running' || job.status === 'provisioning') {
        db.updateJob(job.id, { status: 'stopped' });
      }
    }

    // Cleanup old logs
    db.cleanupOldLogs(7);
  }

  // ── Create a new job ──
  createJob(data) {
    return db.createJob(data);
  }

  // ── Get all jobs with live status ──
  getAllJobs() {
    const jobs = db.getAllJobs();
    return jobs.map(job => this._enrichWithLiveStatus(job));
  }

  // ── Get single job with live status ──
  getJob(id) {
    const job = db.getJob(id);
    if (!job) return null;
    return this._enrichWithLiveStatus(job);
  }

  // ── Update job config (only when stopped) ──
  updateJob(id, data) {
    const job = db.getJob(id);
    if (!job) return null;

    const instance = this.instances.get(id);
    if (instance && instance.running) {
      throw new Error('Cannot update a running job. Stop it first.');
    }

    return db.updateJob(id, data);
  }

  // ── Delete a job ──
  async deleteJob(id) {
    // Stop if running
    await this.stopJob(id);
    db.deleteJob(id);
  }

  // ── Start a job ──
  async startJob(id) {
    const job = db.getJob(id);
    if (!job) throw new Error('Job not found');

    if (job.status === 'booked') {
      throw new Error('Job already has a confirmed booking.');
    }

    // Check if already running
    const existing = this.instances.get(id);
    if (existing && existing.running) {
      throw new Error('Job is already running.');
    }
    if (job.dropletId && (job.status === 'running' || job.status === 'provisioning')) {
      throw new Error('Job already has an active droplet.');
    }

    // Validate required fields
    if (!job.email || !job.password || !job.scheduleId) {
      throw new Error('Job is missing email, password, or scheduleId.');
    }
    if (!job.facilityIds || job.facilityIds.length === 0) {
      throw new Error('No facility IDs configured. Fetch locations first and select facilities.');
    }

    // ── DROPLET MODE ──
    if (dropletManager.isEnabled()) {
      return this._startDropletJob(id, job);
    }

    // ── LOCAL MODE ──
    return this._startLocalJob(id, job);
  }

  // ── Start job locally (original behaviour) ──
  async _startLocalJob(id, job) {
    const instance = new SchedulerInstance(id);
    this.instances.set(id, instance);

    instance.start().catch(err => {
      console.error('[JobManager] Job ' + id + ' crashed:', err.message);
      db.updateJob(id, { status: 'error', lastError: err.message });
    });

    return this.getJob(id);
  }

  // ── Start job on a new DigitalOcean droplet ──
  async _startDropletJob(id, job) {
    console.log('[JobManager] Droplet mode: spawning droplet for job ' + id);
    db.updateJob(id, { status: 'provisioning', dropletId: null, dropletIp: null, dropletStatus: 'creating' });

    const jobConfig = {
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

    // Spawn droplet in background - don't block HTTP response
    this._provisionDroplet(id, jobConfig).catch(err => {
      console.error('[JobManager] Droplet provisioning failed for job ' + id + ':', err.message);
      db.updateJob(id, { status: 'error', lastError: 'Droplet provisioning failed: ' + err.message, dropletStatus: 'failed' });
      db.addLog(id, 'error', 'Droplet provisioning failed: ' + err.message);
    });

    return this.getJob(id);
  }

  async _provisionDroplet(id, jobConfig) {
    // 1. Create droplet
    const droplet = await dropletManager.createDroplet(id, jobConfig);
    db.updateJob(id, { dropletId: String(droplet.id), dropletStatus: 'booting' });
    db.addLog(id, 'info', 'Droplet #' + droplet.id + ' created. Waiting for it to boot...');

    // 2. Wait until active
    const { ip } = await dropletManager.waitForActive(droplet.id);
    db.updateJob(id, { dropletIp: ip, dropletStatus: 'active', status: 'running' });
    db.addLog(id, 'success', 'Droplet active at ' + ip + '. Agent starting...');
  }

  // ── Stop a job ──
  async stopJob(id) {
    // ── Droplet mode: destroy the droplet ──
    if (dropletManager.isEnabled()) {
      const job = db.getJob(id);
      if (job && job.dropletId) {
        await this.destroyJobDroplet(id);
        return this.getJob(id);
      }
    }

    // ── Local mode ──
    const instance = this.instances.get(id);
    if (instance && instance.running) {
      await instance.stop();
      this.instances.delete(id);
    } else {
      // Just ensure DB status is correct
      db.updateJob(id, { status: 'stopped' });
    }
    return this.getJob(id);
  }

  // ── Destroy the droplet for a job and clean up ──
  async destroyJobDroplet(id) {
    const job = db.getJob(id);
    if (!job || !job.dropletId) return;

    console.log('[JobManager] Destroying droplet ' + job.dropletId + ' for job ' + id);
    await dropletManager.destroyDroplet(job.dropletId);
    db.updateJob(id, {
      status: job.status === 'booked' ? 'booked' : 'stopped',
      dropletId: null,
      dropletIp: null,
      dropletStatus: 'destroyed'
    });
    db.addLog(id, 'info', 'Droplet destroyed.');
  }

  // ── Fetch locations for a job (login + scrape, return locations) ──
  async fetchLocations(id) {
    const job = db.getJob(id);
    if (!job) throw new Error('Job not found');

    await loadModules();

    // Create a temporary instance just for fetching locations
    const tempInstance = new SchedulerInstance(id);
    tempInstance.config = {
      email: job.email,
      password: job.password,
      scheduleId: job.scheduleId,
      country: job.country,
      facilityIds: [],
      startDate: job.startDate,
      endDate: job.endDate,
      checkIntervalSeconds: 30,
      autoBook: false,
      maxReloginAttempts: 3,
      requestTimeoutMs: 20000,
      maxRetries: 3
    };
    // Login (initPage() is called internally by login() — no resetSession needed)
    await tempInstance.login();

    // Fetch locations
    const locations = await tempInstance.fetchLocations();

    // Close the browser page — we're done with this temp instance
    await tempInstance.closePage();

    return locations;
  }

  // ── Get logs for a job ──
  getLogs(id, opts) {
    return db.getLogs(id, opts);
  }

  // ── Clear logs for a job ──
  clearLogs(id) {
    return db.clearLogs(id);
  }

  // ── Get cached locations ──
  getCachedLocations(id) {
    return db.getCachedLocations(id);
  }

  // ── Reset booking status (to re-run) ──
  resetBooking(id) {
    const instance = this.instances.get(id);
    if (instance && instance.running) {
      throw new Error('Cannot reset a running job. Stop it first.');
    }
    return db.updateJob(id, {
      status: 'stopped',
      bookedDate: null,
      bookedTime: null,
      bookedFacility: null,
      bookedAt: null,
      totalChecks: 0,
      successfulChecks: 0,
      failedChecks: 0,
      consecutiveFailures: 0,
      reloginCount: 0,
      lastError: null,
      lastCheckAt: null,
      startedAt: null
    });
  }

  // ── Enrich DB job with live instance status ──
  _enrichWithLiveStatus(job) {
    // Local instance live stats
    const instance = this.instances.get(job.id);
    if (instance && instance.running) {
      const status = instance.getStatus();
      job.status = 'running';
      job.totalChecks = status.health.totalChecks;
      job.successfulChecks = status.health.successfulChecks;
      job.failedChecks = status.health.failedChecks;
      job.consecutiveFailures = status.health.consecutiveFailures;
      job.reloginCount = status.health.reloginCount;
      job.lastError = status.health.lastError;
      job.lastCheckAt = status.health.lastCheckAt;
      job.startedAt = status.health.startedAt;
    }
    return job;
  }

  // ── Graceful shutdown ──
  async shutdown() {
    console.log('[JobManager] Shutting down all jobs...');
    const promises = [];
    for (const [id, instance] of this.instances) {
      if (instance.running) {
        promises.push(instance.stop());
      }
    }
    await Promise.allSettled(promises);
    this.instances.clear();
    console.log('[JobManager] All jobs stopped.');
  }
}

module.exports = new JobManager();
