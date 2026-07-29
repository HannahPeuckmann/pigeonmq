# AwesomeQueue

A Redis-backed distributed job queue service.

## Installation Options

### Option 1: Install via Snap (Recommended)

1. **Build the snap:**
   ```bash
   cd /path/to/awesomequeue
   snapcraft
   ```

2. **Install locally:**
   ```bash
   sudo snap install awesomequeue_1.0_amd64.snap --dangerous
   ```

3. **The service starts automatically!** Check status:
   ```bash
   snap services awesomequeue
   ```

4. **View logs:**
   ```bash
   snap logs awesomequeue.awesomequeue -f
   ```

**Note:** The Snap includes its own Redis server, so you don't need to install Redis separately.

### Option 2: Manual Installation with systemd

1. **Run the installation script:**
   ```bash
   cd /path/to/awesomequeue
   ./install.sh
   ```

2. **Enable and start the service:**
   ```bash
   sudo systemctl enable awesomequeue
   sudo systemctl start awesomequeue
   ```

3. **Check status:**
   ```bash
   sudo systemctl status awesomequeue
   ```

4. **View logs:**
   ```bash
   sudo journalctl -u awesomequeue -f
   ```

**Note:** Redis must be installed and running separately for this option.

## Configuration

### Environment Variables

- `REDIS_ADDR` - Redis server address (default: `localhost:6379`)
- `LISTEN_ADDR` - HTTP server listen address (default: `localhost:11300`)

### For systemd service:

Edit `/etc/systemd/system/awesomequeue.service` and modify the Environment variables, then reload:
```bash
sudo systemctl daemon-reload
sudo systemctl restart awesomequeue
```

### For snap:

Snap uses the defaults in the snapcraft.yaml. To modify, rebuild the snap.

## API Endpoints

- `POST /register/?queue=<name>` - Register a new worker on a queue, returns `{"workerID": <int>}`
- `POST /unregister/?worker=<id>` - Unregister a worker
- `POST /queue/?queue=<name>&priority=<0-3>` - Queue a new job, body is the raw job payload; `priority` is optional (0=urgent, 1=high, 2=medium (default), 3=low); returns `{"jobID": <int>}`
- `POST /reserve/?worker=<id>` - Reserve a job for processing; returns `200` with the job, or `204` if no job is available
- `POST /complete/?worker=<id>` - Called by a worker to mark its currently-assigned job done
- `POST /cancel/?job=<id>` - Cancel a job that hasn't started building yet (must still be `QUEUED`)
- `POST /heartbeat/?worker=<id>` - Worker heartbeat
- `GET /aggregate-status/?queue=<name>` - Get queue status counts as JSON (`totalWorkers`, `idleWorkers`, `busyWorkers`, `jobsReady`, `jobsReserved`, `jobsDone`); `queue` is optional and scopes the counts to one tube
- `GET /job-status/?job=<id>` - Get a single job's status by ID
- `GET /status/?queue=<name>` - Get the full queue overview (jobs, workers, dead workers) as a rendered HTML page; `queue` is optional and scopes it to one tube
- `GET /status.json/?queue=<name>` - Same overview as `/status/`, but as JSON; `queue` is optional and scopes it to one tube

## CLI

A command-line client (`aq`) is included for interacting with a running
AwesomeQueue server without writing HTTP requests by hand. It talks to
`http://localhost:11300` by default; override with the `AWESOMEQUEUE_ADDR`
environment variable (e.g. `AWESOMEQUEUE_ADDR=http://queue.example.com:11300`).
Once installed via the snap it's available as `awesomequeue.cli`:

```bash
awesomequeue.cli status [--tube <tube>]              # queue status, optionally scoped to a tube
awesomequeue.cli job-status <job_id>                 # a single job's status
awesomequeue.cli queue <body> --tube <tube> [--priority urgent|high|medium|low]
                                                      # queue a job (default priority: medium)
```

`cancel <job_id>` is registered but not yet implemented.

Run `sudo snap alias awesomequeue.cli aq` for a shorter alias if you'd like
one.

## Development

Build the binaries:
```bash
cd service
go build -o awesomequeue-server main.go
go build -o awesomequeue-cli ./cli
```

Run the tests:
```bash
cd service
go test ./...
```

Run locally:
```bash
./awesomequeue-server
./awesomequeue-cli status
```
