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
- `POST /unregister/` - Unregister a worker (JSON body: `{"id": "<workerID>"}`)
- `POST /queue/?queue=<name>&priority=<0-3>` - Queue a new job, body is the raw job payload; `priority` is optional (0=urgent, 1=high, 2=medium (default), 3=low); returns `{"jobID": <int>}`
- `POST /reserve/` - Reserve a job for processing (JSON body: `{"id": "<workerID>"}`); returns `200` with the job, or `204` if no job is available
- `POST /delete/` - Mark the worker's in-progress job as done (JSON body: `{"id": "<workerID>"}`)
- `POST /heartbeat/` - Worker heartbeat (JSON body: `{"id": "<workerID>"}`)
- `GET /json-status/` - Get queue status as JSON (`totalWorkers`, `idleWorkers`, `busyWorkers`, `jobsReady`, `jobsReserved`, `jobsDone`)
- `GET /job-status/?job=<id>` - Get a single job's status by ID
- `GET /status/` - Get queue status as a rendered HTML page

## Development

Build the binary:
```bash
cd service
go build -o awesomequeue-server main.go
```

Run the tests:
```bash
cd service
go test ./...
```

Run locally:
```bash
./awesomequeue-server
```
