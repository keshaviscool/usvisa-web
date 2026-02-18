# US Visa Scheduler — Web Dashboard

A web-based management dashboard for running multiple US Visa auto-rescheduler instances simultaneously. Built on top of the standalone scheduler logic.

## Features

- **Multi-job management** — Create and run multiple scheduler jobs with different credentials, schedule IDs, date ranges, and locations
- **Real-time dashboard** — Live status indicators, health stats, and log streaming
- **Location picker** — Automatically logs in and fetches available consulate locations
- **Auto-booking** — When a matching date is found, books it instantly
- **Persistent storage** — SQLite database stores all jobs, configs, and logs
- **Graceful lifecycle** — Start, stop, reset jobs. Server restart preserves job configs
- **VPS-ready** — Deploy on any VPS and manage everything from your browser

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Start the server
npm start

# 3. Open in browser
open http://localhost:3456
```

## How to Use

1. **Create a Job** — Click "+ New Job", fill in your US visa account credentials, schedule ID, date range, and check interval.

2. **Fetch Locations** — Click "📍 Locations" on the job to log in and fetch available consulate locations. Select the ones you want to monitor.

3. **Start the Job** — Click "▶ Start". The scheduler will continuously check for available dates in your range and auto-book when found.

4. **Monitor** — Watch real-time logs, health stats, and status on the dashboard. The dashboard auto-refreshes every 5 seconds.

5. **Stop / Reset** — Stop a running job anytime. If a booking was made, you can reset it to re-run.

## Configuration Options

| Field | Description | Default |
|-------|-------------|---------|
| Email | US visa account email | — |
| Password | Account password | — |
| Schedule ID | Your appointment schedule ID | — |
| Country | Country code (`en-ca`, `en-us`, etc.) | `en-ca` |
| Start Date | Earliest acceptable date | — |
| End Date | Latest acceptable date | — |
| Check Interval | Seconds between checks | `30` |
| Auto Book | Automatically book when found | `true` |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/jobs` | List all jobs |
| `POST` | `/api/jobs` | Create a job |
| `GET` | `/api/jobs/:id` | Get job details |
| `PUT` | `/api/jobs/:id` | Update job config |
| `DELETE` | `/api/jobs/:id` | Delete a job |
| `POST` | `/api/jobs/:id/start` | Start a job |
| `POST` | `/api/jobs/:id/stop` | Stop a job |
| `POST` | `/api/jobs/:id/fetch-locations` | Login & fetch locations |
| `GET` | `/api/jobs/:id/locations` | Get cached locations |
| `GET` | `/api/jobs/:id/logs` | Get job logs |
| `DELETE` | `/api/jobs/:id/logs` | Clear job logs |
| `POST` | `/api/jobs/:id/reset` | Reset booking status |

## Deploying on a VPS

```bash
# Clone and install
git clone <repo-url>
cd web
npm install

# Run with a process manager (recommended)
npm install -g pm2
pm2 start server.js --name visa-scheduler
pm2 save

# Or run directly
PORT=3456 node server.js
```

To access remotely, either:
- Open port 3456 in your firewall
- Use nginx as a reverse proxy
- Use an SSH tunnel: `ssh -L 3456:localhost:3456 your-vps`

## Tech Stack

- **Backend**: Node.js, Express
- **Database**: SQLite (better-sqlite3)
- **Frontend**: Vanilla HTML/CSS/JS (no build step)
- **HTTP Client**: node-fetch + tough-cookie
- **HTML Parser**: cheerio
