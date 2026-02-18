// ============================================================
// DROPLET MANAGER - DigitalOcean API wrapper
// Creates/destroys a droplet per job.
// Requires env: DO_API_TOKEN, CALLBACK_SECRET, MAIN_VPS_URL
// ============================================================

const https = require('https');

const DO_API_TOKEN = process.env.DO_API_TOKEN;
const CALLBACK_SECRET = process.env.CALLBACK_SECRET || 'changeme';
const MAIN_VPS_URL = process.env.MAIN_VPS_URL; // e.g. http://1.2.3.4:3456

// Droplet config - cheapest droplet, closest region to visa servers
const DROPLET_SIZE   = process.env.DO_DROPLET_SIZE   || 's-1vcpu-512mb-10gb'; // $4/mo
const DROPLET_REGION = process.env.DO_DROPLET_REGION || 'nyc3';
const DROPLET_IMAGE  = process.env.DO_DROPLET_IMAGE  || 'ubuntu-24-04-x64';
const REPO_URL       = process.env.REPO_URL          || 'https://github.com/keshaviscool/usvisa-web.git';

// ── Low-level DO API call ──
function doRequest(method, path, body) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const options = {
      hostname: 'api.digitalocean.com',
      path: '/v2' + path,
      method,
      headers: {
        'Authorization': 'Bearer ' + DO_API_TOKEN,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    };
    if (payload) options.headers['Content-Length'] = Buffer.byteLength(payload);

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          const parsed = data ? JSON.parse(data) : {};
          if (res.statusCode >= 400) {
            reject(new Error('DO API ' + method + ' ' + path + ' → ' + res.statusCode + ': ' + (parsed.message || data)));
          } else {
            resolve(parsed);
          }
        } catch (e) {
          reject(new Error('DO API parse error: ' + e.message));
        }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

// ── Build cloud-init user_data script ──
function buildUserData(jobId, jobConfig) {
  // Serialize the job config as base64 to safely pass through shell
  const configB64 = Buffer.from(JSON.stringify(jobConfig)).toString('base64');

  return `#!/bin/bash
set -e

# ── System setup ──
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git curl

# ── Install Node.js 20 ──
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y -qq nodejs

# ── Install PM2 ──
npm install -g pm2 --quiet

# ── Clone repo ──
cd /root
git clone ${REPO_URL} app
cd /root/app

# ── Install deps ──
npm install --quiet

# ── Write env file ──
cat > /root/app/agent/.env << 'ENVEOF'
JOB_ID=${jobId}
JOB_CONFIG_B64=${configB64}
MAIN_VPS_URL=${MAIN_VPS_URL}
CALLBACK_SECRET=${CALLBACK_SECRET}
ENVEOF

# ── Start agent with PM2 ──
pm2 start /root/app/agent/agent.js --name visa-agent-${jobId} --no-autorestart
pm2 save
`;
}

// ── Create a droplet for a job ──
async function createDroplet(jobId, jobConfig) {
  if (!DO_API_TOKEN) throw new Error('DO_API_TOKEN is not set.');
  if (!MAIN_VPS_URL) throw new Error('MAIN_VPS_URL is not set.');

  const userData = buildUserData(jobId, jobConfig);

  const body = {
    name: 'visa-job-' + jobId,
    region: DROPLET_REGION,
    size: DROPLET_SIZE,
    image: DROPLET_IMAGE,
    user_data: userData,
    tags: ['visa-scheduler', 'job-' + jobId],
    ipv6: false,
    monitoring: false
  };

  const result = await doRequest('POST', '/droplets', body);
  const droplet = result.droplet;
  console.log('[DropletManager] Created droplet ' + droplet.id + ' for job ' + jobId);
  return droplet;
}

// ── Get droplet info ──
async function getDroplet(dropletId) {
  const result = await doRequest('GET', '/droplets/' + dropletId);
  return result.droplet;
}

// ── Wait until droplet is active and has an IP ──
async function waitForActive(dropletId, timeoutMs) {
  timeoutMs = timeoutMs || 180000; // 3 minutes max
  const start = Date.now();
  const poll = 5000;

  while (Date.now() - start < timeoutMs) {
    const droplet = await getDroplet(dropletId);
    if (droplet.status === 'active') {
      // Find public IPv4
      const network = (droplet.networks.v4 || []).find(n => n.type === 'public');
      if (network && network.ip_address) {
        return { droplet, ip: network.ip_address };
      }
    }
    await new Promise(r => setTimeout(r, poll));
  }
  throw new Error('Droplet ' + dropletId + ' did not become active in time.');
}

// ── Destroy a droplet ──
async function destroyDroplet(dropletId) {
  if (!dropletId) return;
  try {
    await doRequest('DELETE', '/droplets/' + dropletId);
    console.log('[DropletManager] Destroyed droplet ' + dropletId);
  } catch (err) {
    console.error('[DropletManager] Failed to destroy droplet ' + dropletId + ':', err.message);
  }
}

// ── List all visa-scheduler droplets (for cleanup) ──
async function listJobDroplets() {
  const result = await doRequest('GET', '/droplets?tag_name=visa-scheduler');
  return result.droplets || [];
}

module.exports = {
  createDroplet,
  getDroplet,
  waitForActive,
  destroyDroplet,
  listJobDroplets,
  isEnabled: () => !!DO_API_TOKEN
};
