// ============================================================
// JOB MANAGER - Manages all scheduler instances
// ============================================================

const db = require('./database');
const { SchedulerInstance, loadModules } = require('./scheduler-engine');

class JobManager {
  constructor() {
    // Active scheduler instances: jobId → SchedulerInstance
    this.instances = new Map();
  }

  // ── Initialize: restore running jobs on server restart ──
  async init() {
    await loadModules();

    // Any jobs that were 'running' when server died → mark as 'stopped'
    const allJobs = db.getAllJobs();
    for (const job of allJobs) {
      if (job.status === 'running') {
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

    // Validate required fields
    if (!job.email || !job.password || !job.scheduleId) {
      throw new Error('Job is missing email, password, or scheduleId.');
    }
    if (!job.facilityIds || job.facilityIds.length === 0) {
      throw new Error('No facility IDs configured. Fetch locations first and select facilities.');
    }

    // Create and start instance
    const instance = new SchedulerInstance(id);
    this.instances.set(id, instance);

    // Start in background (don't await the full loop)
    instance.start().catch(err => {
      console.error('[JobManager] Job ' + id + ' crashed:', err.message);
      db.updateJob(id, { status: 'error', lastError: err.message });
    });

    return this.getJob(id);
  }

  // ── Stop a job ──
  async stopJob(id) {
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
    tempInstance.resetSession();

    // Login
    await tempInstance.login();

    // Fetch locations
    const locations = await tempInstance.fetchLocations();
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
