package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"awesomequeue/handlers"
	"strings"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var mr *miniredis.Miniredis
var rdb *goredis.Client
var srv *server

func TestMain(m *testing.M) {
	var err error
	mr, err = miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer mr.Close()

	rdb = goredis.NewClient(&goredis.Options{
		Addr: mr.Addr(),
	})
	srv = &server{rdb: rdb}

	os.Exit(m.Run())
}

// newQueryRequest builds a request with the given query params set, for
// handlers that read their arguments from the URL rather than a JSON body.
func newQueryRequest(method, target string, params map[string]string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	query := req.URL.Query()
	for k, v := range params {
		query.Add(k, v)
	}
	req.URL.RawQuery = query.Encode()
	return req
}

// --- registerWorkerHandler ---

func TestRegisterWorker_ReturnsWorkerID(t *testing.T) {
	mr.FlushAll()

	req := httptest.NewRequest(http.MethodPost, "/register/", nil)
	query := req.URL.Query()
	query.Add("queue", "default")
	req.URL.RawQuery = query.Encode()
	w := httptest.NewRecorder()
	srv.registerWorkerHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp map[string]int
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp, "workerID")
}

func TestRegisterWorker_CreatesWorkerInRedis(t *testing.T) {
	mr.FlushAll()

	req := httptest.NewRequest(http.MethodPost, "/register/", nil)
	query := req.URL.Query()
	query.Add("queue", "default")
	req.URL.RawQuery = query.Encode()
	w := httptest.NewRecorder()
	srv.registerWorkerHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp map[string]int
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))

	ctx := context.Background()
	workerKey := fmt.Sprintf("worker: %d", resp["workerID"])

	exists, err := rdb.Exists(ctx, workerKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "worker key should exist in Redis")

	status, err := rdb.HGet(ctx, workerKey, "status").Result()
	require.NoError(t, err)
	assert.Equal(t, "IDLE", status)

	jobID, err := rdb.HGet(ctx, workerKey, "jobID").Result()
	require.NoError(t, err)
	assert.Equal(t, "none", jobID)
}

// --- unregisterWorkerHandler ---

func TestUnregisterWorker_NotFound(t *testing.T) {
	mr.FlushAll()

	req := newQueryRequest(http.MethodPost, "/unregister/", map[string]string{"worker": "ghost-worker"})
	w := httptest.NewRecorder()
	srv.unregisterWorkerHandler(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestUnregisterWorker_MissingWorkerParam(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/unregister/", nil)
	w := httptest.NewRecorder()
	srv.unregisterWorkerHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUnregisterWorker_RemovesWorkerFromRedis(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	workerKey := "worker: worker-42"
	rdb.HSet(ctx, workerKey, "status", "IDLE", "jobID", "none", "lastHeartbeat", "0")

	req := newQueryRequest(http.MethodPost, "/unregister/", map[string]string{"worker": "worker-42"})
	w := httptest.NewRecorder()
	srv.unregisterWorkerHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	exists, err := rdb.Exists(ctx, workerKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "worker should have been deleted from Redis")
}

func TestUnregisterWorker_WithJob_RequeuesJob(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	workerKey := "worker: worker-99"
	jobKey := "job: 7"
	queueName := "default"
	rdb.HSet(ctx, workerKey, "status", "BUSY", "jobID", "7", "lastHeartbeat", "0")
	rdb.HSet(ctx, jobKey,
		"id", "7",
		"priority", "2",
		"status", "BUILDING",
		"queue", queueName,
		"worker", "worker-99",
		"build-start", time.Now().Unix(),
	)

	req := newQueryRequest(http.MethodPost, "/unregister/", map[string]string{"worker": "worker-99"})
	w := httptest.NewRecorder()
	srv.unregisterWorkerHandler(w, req)

	jobStatus, err := rdb.HGet(ctx, jobKey, "status").Result()
	require.NoError(t, err)
	assert.Equal(t, "QUEUED", jobStatus, "job should have been requeued when worker unregisters")

	jobWorker, err := rdb.HGet(ctx, jobKey, "worker").Result()
	require.NoError(t, err)
	assert.Equal(t, "", jobWorker)

	jobBuildStart, err := rdb.HGet(ctx, jobKey, "build-start").Result()
	require.NoError(t, err)
	assert.Equal(t, "", jobBuildStart)

	// the job must be put back on its original priority queue, otherwise it
	// will never be picked up again even though its status says QUEUED.
	score, err := rdb.ZScore(ctx, queueName, jobKey).Result()
	require.NoError(t, err)
	assert.Equal(t, float64(2), score)
}

// --- QueueHandler ---

func TestQueueHandler_CreatesJobInRedis(t *testing.T) {
	mr.FlushAll()

	req := httptest.NewRequest(http.MethodPost, "/queue/", strings.NewReader("Test job"))
	query := req.URL.Query()
	query.Add("queue", "default")
	req.URL.RawQuery = query.Encode()
	w := httptest.NewRecorder()
	srv.queueHandler(w, req)

	jobKeys, err := rdb.Keys(context.Background(), "job: *").Result()
	require.NoError(t, err)
	assert.Equal(t, len(jobKeys), 1)

	// make sure the job was created in redis
	for _, key := range jobKeys {
		jobData, err := rdb.HGetAll(context.Background(), key).Result()
		require.NoError(t, err)

		assert.Equal(t, jobData["status"], "QUEUED")
		assert.Equal(t, jobData["priority"], "2")
	}

	// make sure the job was put on the correct queue
	jobZ, err := rdb.ZPopMin(context.Background(), "default", 1).Result()
	assert.Equal(t, len(jobZ), 1)

	jobKey := jobZ[0].Member.(string)

	jobData, err := rdb.HGetAll(context.Background(), jobKey).Result()
	assert.Equal(t, jobData["status"], "QUEUED")
	assert.Equal(t, jobData["priority"], "2")

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp map[string]int
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp, "jobID")
}

func TestQueueHandler_CreatesJobWithPriority(t *testing.T) {
	mr.FlushAll()

	req := httptest.NewRequest(http.MethodPost, "/queue/", strings.NewReader("Test job"))
	query := req.URL.Query()
	query.Add("priority", "0")
	query.Add("queue", "default")
	req.URL.RawQuery = query.Encode()

	w := httptest.NewRecorder()
	srv.queueHandler(w, req)

	jobKeys, err := rdb.Keys(context.Background(), "job: *").Result()
	require.NoError(t, err)
	assert.Equal(t, len(jobKeys), 1)

	for _, key := range jobKeys {
		jobData, err := rdb.HGetAll(context.Background(), key).Result()
		require.NoError(t, err)

		assert.Equal(t, jobData["status"], "QUEUED")
		assert.Equal(t, jobData["priority"], "0")
	}

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp map[string]int
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp, "jobID")
}

func TestQueueHandler_CreateEmptyJob(t *testing.T) {
	mr.FlushAll()

	req := httptest.NewRequest(http.MethodPost, "/queue/", nil)
	query := req.URL.Query()
	query.Add("priority", "urgent")
	req.URL.RawQuery = query.Encode()

	w := httptest.NewRecorder()
	srv.queueHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- ReserveHandler ---

func TestReserveHandler_SetPropertiesOnJobAndWorker(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	workerKey := "worker: worker-30"
	jobKey := "job: 7"
	rdb.HSet(ctx, workerKey, "status", "IDLE", "jobID", "none", "lastHeartbeat", "0", "listenQueue", "default")
	rdb.HSet(ctx, jobKey, "id", "7", "priority", "3", "status", "QUEUED")
	rdb.ZAdd(ctx, "default", redis.Z{Score: 3, Member: jobKey})

	req := newQueryRequest(http.MethodPost, "/reserve/", map[string]string{"worker": "worker-30"})
	w := httptest.NewRecorder()
	srv.reserveHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp map[string]string
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp, "id")
	assert.Equal(t, resp["id"], "7")

	newJobStatus, err := rdb.HGet(ctx, jobKey, "status").Result()
	assert.Equal(t, newJobStatus, "BUILDING")
	require.NoError(t, err)

	newWorkerStatus, err := rdb.HGet(ctx, workerKey, "status").Result()
	assert.Equal(t, newWorkerStatus, "BUSY")
	require.NoError(t, err)

	newWorkerJob, err := rdb.HGet(ctx, workerKey, "jobID").Result()
	assert.Equal(t, newWorkerJob, "7")
	require.NoError(t, err)
}

func TestReserveHandler_ConcurrentReserves(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	rdb.HSet(ctx, "job: 1", "id", 1, "priority", "2", "status", "QUEUED")
	rdb.HSet(ctx, "job: 2", "id", 2, "priority", "3", "status", "QUEUED")
	rdb.HSet(ctx, "job: 3", "id", 3, "priority", "2", "status", "QUEUED")
	rdb.HSet(ctx, "job: 4", "id", 4, "priority", "2", "status", "QUEUED")
	rdb.HSet(ctx, "job: 5", "id", 5, "priority", "3", "status", "QUEUED")
	rdb.HSet(ctx, "job: 6", "id", 6, "priority", "2", "status", "QUEUED")
	rdb.ZAdd(ctx, "default", redis.Z{Score: 2, Member: "job: 1"})
	rdb.ZAdd(ctx, "default", redis.Z{Score: 3, Member: "job: 2"})
	rdb.ZAdd(ctx, "default", redis.Z{Score: 2, Member: "job: 3"})
	rdb.ZAdd(ctx, "default", redis.Z{Score: 2, Member: "job: 4"})
	rdb.ZAdd(ctx, "default", redis.Z{Score: 3, Member: "job: 5"})
	rdb.ZAdd(ctx, "default", redis.Z{Score: 2, Member: "job: 6"})

	rdb.HSet(ctx, "worker: 1", "status", "IDLE", "jobID", "none", "lastHeartbeat", time.Now().Unix(), "listenQueue", "default")
	rdb.HSet(ctx, "worker: 2", "status", "IDLE", "jobID", "none", "lastHeartbeat", time.Now().Unix(), "listenQueue", "default")
	rdb.HSet(ctx, "worker: 3", "status", "IDLE", "jobID", "none", "lastHeartbeat", time.Now().Unix(), "listenQueue", "default")
	rdb.HSet(ctx, "worker: 4", "status", "IDLE", "jobID", "none", "lastHeartbeat", time.Now().Unix(), "listenQueue", "default")

	reserve_routine := func(workerID string, wg *sync.WaitGroup, ch chan int) {
		defer wg.Done()
		req := newQueryRequest(http.MethodPost, "/reserve/", map[string]string{"worker": workerID})
		w := httptest.NewRecorder()
		srv.reserveHandler(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		assert.Contains(t, resp, "id")
		jobID, _ := strconv.Atoi(resp["id"])
		ch <- jobID
	}

	var wg sync.WaitGroup
	ch := make(chan int, 4)
	wg.Add(1)
	go reserve_routine("1", &wg, ch)
	wg.Add(1)
	go reserve_routine("2", &wg, ch)
	wg.Add(1)
	go reserve_routine("3", &wg, ch)
	wg.Add(1)
	go reserve_routine("4", &wg, ch)
	go func() {
		wg.Wait()
		close(ch)
	}()

	var reservedIDs []int
	for id := range ch {
		reservedIDs = append(reservedIDs, id)
	}

	assert.Len(t, reservedIDs, 4)
	seen := make(map[int]bool)
	for _, id := range reservedIDs {
		assert.False(t, seen[id], "job ID %d was reserved more than once", id)
		seen[id] = true
	}
}

func TestReserveHandler_NoJob(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	workerKey := "worker: worker-30"
	rdb.HSet(ctx, workerKey, "status", "IDLE", "jobID", "none", "lastHeartbeat", "0", "listenQueue", "default")

	req := newQueryRequest(http.MethodPost, "/reserve/", map[string]string{"worker": "worker-30"})
	w := httptest.NewRecorder()
	srv.reserveHandler(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestReserveHandler_WorkerNotRegistered(t *testing.T) {
	mr.FlushAll()

	req := newQueryRequest(http.MethodPost, "/reserve/", map[string]string{"worker": "worker-30"})
	w := httptest.NewRecorder()
	srv.reserveHandler(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestReserveHandler_WorkerIsBusy(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	workerKey := "worker: worker-30"
	rdb.HSet(ctx, workerKey, "status", "BUSY", "jobID", "7", "lastHeartbeat", "0")

	req := newQueryRequest(http.MethodPost, "/reserve/", map[string]string{"worker": "worker-30"})
	w := httptest.NewRecorder()
	srv.reserveHandler(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestReserveHandler_MissingWorkerParam(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/reserve/", nil)
	w := httptest.NewRecorder()
	srv.reserveHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- completeHandler ---

func TestCompleteHandler_SetPropertiesOnJobAndWorker(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	workerKey := "worker: worker-30"
	jobKey := "job: 7"
	rdb.HSet(ctx, workerKey, "status", "BUSY", "jobID", "7", "lastHeartbeat", "0")
	rdb.HSet(ctx, jobKey, "id", "7", "status", "BUILDING")
	rdb.LPush(ctx, "job_queue", jobKey)

	req := newQueryRequest(http.MethodPost, "/complete/", map[string]string{"worker": "worker-30"})
	w := httptest.NewRecorder()
	srv.completeHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	newJobStatus, err := rdb.HGet(ctx, jobKey, "status").Result()
	assert.Equal(t, newJobStatus, "DONE")
	require.NoError(t, err)

	newWorkerStatus, err := rdb.HGet(ctx, workerKey, "status").Result()
	assert.Equal(t, newWorkerStatus, "IDLE")
	require.NoError(t, err)

	newWorkerJob, err := rdb.HGet(ctx, workerKey, "jobID").Result()
	assert.Equal(t, newWorkerJob, "none")
	require.NoError(t, err)
}

func TestCompleteHandler_WorkerIsIdle(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	workerKey := "worker: worker-30"
	rdb.HSet(ctx, workerKey, "status", "IDLE", "jobID", "none", "lastHeartbeat", "0")

	req := newQueryRequest(http.MethodPost, "/complete/", map[string]string{"worker": "worker-30"})
	w := httptest.NewRecorder()
	srv.completeHandler(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestCompleteHandler_MissingWorkerParam(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/complete/", nil)
	w := httptest.NewRecorder()
	srv.completeHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCompleteHandler_WorkerNotRegistered(t *testing.T) {
	mr.FlushAll()

	req := newQueryRequest(http.MethodPost, "/complete/", map[string]string{"worker": "ghost-worker"})
	w := httptest.NewRecorder()
	srv.completeHandler(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --- cancelHandler ---

func TestCancelHandler_CancelsQueuedJob(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	jobKey := "job: 7"
	queueName := "default"
	rdb.HSet(ctx, jobKey, "id", "7", "status", "QUEUED", "priority", "2", "queue", queueName)
	rdb.ZAdd(ctx, queueName, redis.Z{Score: 2, Member: jobKey})

	req := newQueryRequest(http.MethodPost, "/cancel/", map[string]string{"job": "7"})
	w := httptest.NewRecorder()
	srv.cancelHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	newJobStatus, err := rdb.HGet(ctx, jobKey, "status").Result()
	require.NoError(t, err)
	assert.Equal(t, "DONE", newJobStatus)

	// a cancelled job must also come off its priority queue, otherwise a
	// worker can still reserve it later despite it being marked DONE.
	_, err = rdb.ZScore(ctx, queueName, jobKey).Result()
	assert.ErrorIs(t, err, redis.Nil, "job should have been removed from its queue")
}

func TestCancelHandler_JobNotQueued(t *testing.T) {
	mr.FlushAll()

	ctx := context.Background()
	rdb.HSet(ctx, "job: 7", "id", "7", "status", "BUILDING", "queue", "default")

	req := newQueryRequest(http.MethodPost, "/cancel/", map[string]string{"job": "7"})
	w := httptest.NewRecorder()
	srv.cancelHandler(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestCancelHandler_MissingJobParam(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/cancel/", nil)
	w := httptest.NewRecorder()
	srv.cancelHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCancelHandler_JobNotFound(t *testing.T) {
	mr.FlushAll()

	req := newQueryRequest(http.MethodPost, "/cancel/", map[string]string{"job": "does-not-exist"})
	w := httptest.NewRecorder()
	srv.cancelHandler(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --- heartbeatHandler ---

func TestHeartbeatHandler_UpdatesLastHeartbeat(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	workerKey := "worker: worker-1"
	rdb.HSet(ctx, workerKey, "status", "IDLE", "jobID", "none", "lastHeartbeat", "0")

	req := newQueryRequest(http.MethodPost, "/heartbeat/", map[string]string{"worker": "worker-1"})
	w := httptest.NewRecorder()
	srv.heartbeatHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	heartbeat, err := rdb.HGet(ctx, workerKey, "lastHeartbeat").Result()
	require.NoError(t, err)
	assert.NotEqual(t, "0", heartbeat, "lastHeartbeat should have been updated")
}

func TestHeartbeatHandler_WorkerNotRegistered(t *testing.T) {
	mr.FlushAll()

	req := newQueryRequest(http.MethodPost, "/heartbeat/", map[string]string{"worker": "ghost-worker"})
	w := httptest.NewRecorder()
	srv.heartbeatHandler(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHeartbeatHandler_MissingWorkerParam(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/heartbeat/", nil)
	w := httptest.NewRecorder()
	srv.heartbeatHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- checkHeartbeat ---
func TestCheckHeartbeat_CleanUpStaleWorkers(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	// now minus 20 minutes
	heartbeat := (time.Now().Unix()) - 20*60*1000
	workerKey := "worker: worker-30"
	jobKey := "job: 7"
	queueName := "default"
	rdb.HSet(ctx, workerKey, "status", "BUSY", "jobID", "7", "lastHeartbeat", heartbeat)
	rdb.HSet(ctx, jobKey,
		"id", "7",
		"priority", "2",
		"status", "BUILDING",
		"queue", queueName,
		"worker", "worker-30",
		"build-start", time.Now().Unix(),
	)

	handlers.CheckHeartbeats(rdb)

	newJobStatus, err := rdb.HGet(ctx, jobKey, "status").Result()
	require.NoError(t, err)
	assert.Equal(t, "QUEUED", newJobStatus)

	newJobWorker, err := rdb.HGet(ctx, jobKey, "worker").Result()
	require.NoError(t, err)
	assert.Equal(t, "", newJobWorker)

	newJobBuildStart, err := rdb.HGet(ctx, jobKey, "build-start").Result()
	require.NoError(t, err)
	assert.Equal(t, "", newJobBuildStart)

	// the job must be put back on its original priority queue, otherwise it
	// will never be picked up again even though its status says QUEUED.
	score, err := rdb.ZScore(ctx, queueName, jobKey).Result()
	require.NoError(t, err)
	assert.Equal(t, float64(2), score)

	exists, err := rdb.Exists(ctx, workerKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}

// --- aggregateStatusHandler ---
func TestAggregateStatusHandler(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	rdb.HSet(ctx, "job: 1", "id", 1, "priority", "medium", "status", "BUILDING")
	rdb.HSet(ctx, "job: 2", "id", 2, "priority", "medium", "status", "QUEUED")
	rdb.HSet(ctx, "job: 3", "id", 3, "priority", "medium", "status", "QUEUED")
	rdb.HSet(ctx, "worker: worker-1", "status", "BUSY", "jobID", "7", "lastHeartbeat", "0")

	req := httptest.NewRequest(http.MethodGet, "/aggregate-status/", nil)
	w := httptest.NewRecorder()
	srv.aggregateStatusHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]int
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, 1, resp["jobsReserved"])
	assert.Equal(t, 2, resp["jobsReady"])
	assert.Equal(t, 1, resp["busyWorkers"])
}

// --- jobStatusHandler ---

func TestJobStatusHandler_ReturnsJob(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	queueStart := time.Now().Add(-time.Minute).Unix()
	buildStart := time.Now().Unix()
	rdb.HSet(ctx, "job: 7",
		"id", "7",
		"priority", "2",
		"status", "BUILDING",
		"body", "Test job",
		"queue", "default",
		"queue-start", queueStart,
		"build-start", buildStart,
		// build-finish intentionally omitted: the job hasn't finished building yet.
	)

	req := httptest.NewRequest(http.MethodGet, "/job-status/", nil)
	query := req.URL.Query()
	query.Add("job", "7")
	req.URL.RawQuery = query.Encode()
	w := httptest.NewRecorder()
	srv.jobStatusHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var job handlers.Job
	require.NoError(t, json.NewDecoder(w.Body).Decode(&job))
	assert.Equal(t, "7", job.ID)
	assert.Equal(t, "BUILDING", job.Status)
	assert.Equal(t, "Test job", string(job.Payload))
	assert.Equal(t, "2", job.Priority)
	assert.Equal(t, strconv.FormatInt(queueStart, 10), job.QueueStart)
	assert.Equal(t, strconv.FormatInt(buildStart, 10), job.BuildStart)
	assert.Empty(t, job.BuildFinish)
}

func TestJobStatusHandler_MissingJobParam(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/job-status/", nil)
	w := httptest.NewRecorder()
	srv.jobStatusHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestJobStatusHandler_JobNotFound(t *testing.T) {
	mr.FlushAll()

	req := httptest.NewRequest(http.MethodGet, "/job-status/", nil)
	query := req.URL.Query()
	query.Add("job", "does-not-exist")
	req.URL.RawQuery = query.Encode()
	w := httptest.NewRecorder()
	srv.jobStatusHandler(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --- aggregateStatusHandler, scoped to a tube ---

func TestAggregateStatusHandler_ScopesCountsToTube(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	// two workers on "default", one idle, one busy
	rdb.HSet(ctx, "worker: 1", "status", "IDLE", "jobID", "none", "lastHeartbeat", "0", "listenQueue", "default")
	rdb.HSet(ctx, "worker: 2", "status", "BUSY", "jobID", "7", "lastHeartbeat", "0", "listenQueue", "default")
	// one worker on a different tube, must not be counted
	rdb.HSet(ctx, "worker: 3", "status", "IDLE", "jobID", "none", "lastHeartbeat", "0", "listenQueue", "other")

	// jobs on "default": one queued, one building, one done
	rdb.HSet(ctx, "job: 1", "id", "1", "status", "QUEUED", "queue", "default")
	rdb.HSet(ctx, "job: 2", "id", "2", "status", "BUILDING", "queue", "default")
	rdb.HSet(ctx, "job: 3", "id", "3", "status", "DONE", "queue", "default")
	// job on a different tube, must not be counted
	rdb.HSet(ctx, "job: 4", "id", "4", "status", "QUEUED", "queue", "other")

	req := httptest.NewRequest(http.MethodGet, "/aggregate-status/", nil)
	query := req.URL.Query()
	query.Add("queue", "default")
	req.URL.RawQuery = query.Encode()
	w := httptest.NewRecorder()
	srv.aggregateStatusHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var status handlers.JsonStatus
	require.NoError(t, json.NewDecoder(w.Body).Decode(&status))
	assert.Equal(t, 2, status.TotalWorkers)
	assert.Equal(t, 1, status.IdleWorkers)
	assert.Equal(t, 1, status.BusyWorkers)
	assert.Equal(t, 1, status.JobsReady)
	assert.Equal(t, 1, status.JobsReserved)
	assert.Equal(t, 1, status.JobsDone)
}

func TestAggregateStatusHandler_NoMatchingTube(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	rdb.HSet(ctx, "worker: 1", "status", "IDLE", "jobID", "none", "lastHeartbeat", "0", "listenQueue", "default")
	rdb.HSet(ctx, "job: 1", "id", "1", "status", "QUEUED", "queue", "default")

	req := httptest.NewRequest(http.MethodGet, "/aggregate-status/", nil)
	query := req.URL.Query()
	query.Add("queue", "nonexistent")
	req.URL.RawQuery = query.Encode()
	w := httptest.NewRecorder()
	srv.aggregateStatusHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var status handlers.JsonStatus
	require.NoError(t, json.NewDecoder(w.Body).Decode(&status))
	assert.Equal(t, handlers.JsonStatus{}, status)
}

func TestAggregateStatusHandler_MissingQueueParamCountsAllTubes(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	rdb.HSet(ctx, "worker: 1", "status", "IDLE", "jobID", "none", "lastHeartbeat", "0", "listenQueue", "default")
	rdb.HSet(ctx, "worker: 2", "status", "IDLE", "jobID", "none", "lastHeartbeat", "0", "listenQueue", "other")
	rdb.HSet(ctx, "job: 1", "id", "1", "status", "QUEUED", "queue", "default")
	rdb.HSet(ctx, "job: 2", "id", "2", "status", "QUEUED", "queue", "other")

	req := httptest.NewRequest(http.MethodGet, "/aggregate-status/", nil)
	w := httptest.NewRecorder()
	srv.aggregateStatusHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var status handlers.JsonStatus
	require.NoError(t, json.NewDecoder(w.Body).Decode(&status))
	assert.Equal(t, 2, status.TotalWorkers)
	assert.Equal(t, 2, status.JobsReady)
}

// --- htmlStatusHandler ---

func TestHtmlStatusHandler_RendersJobsAndWorkers(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	rdb.HSet(ctx, "job: 1", "id", "1", "status", "QUEUED", "priority", "2")
	rdb.HSet(ctx, "worker: 99", "status", "BUSY", "jobID", "1", "lastHeartbeat", "0")

	req := httptest.NewRequest(http.MethodGet, "/status/", nil)
	w := httptest.NewRecorder()
	srv.htmlStatusHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// the template used to fail mid-render (index into a non-indexable
	// struct), so also assert the page isn't truncated before the
	// Workers section.
	assert.Contains(t, body, "<h2>Workers</h2>")
	assert.Contains(t, body, "<td>1</td>")
	assert.Contains(t, body, "<td>QUEUED</td>")
	assert.Contains(t, body, "<td>BUSY</td>")
	// worker ID isn't a redis hash field, it comes from the key, so
	// assert it specifically to catch that extraction breaking.
	assert.Contains(t, body, "<td>99</td>")
}

func TestHtmlStatusHandler_NoJobsOrWorkers(t *testing.T) {
	mr.FlushAll()

	req := httptest.NewRequest(http.MethodGet, "/status/", nil)
	w := httptest.NewRecorder()
	srv.htmlStatusHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "No jobs")
	assert.Contains(t, body, "No workers")
}

// --- overviewHandler ---

func TestOverviewHandler_ReturnsJobsAndWorkersAsJson(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	rdb.HSet(ctx, "job: 1", "id", "1", "status", "QUEUED", "priority", "2", "queue", "default")
	rdb.HSet(ctx, "job: 2", "id", "2", "status", "QUEUED", "priority", "2", "queue", "other")
	rdb.HSet(ctx, "worker: 99", "status", "BUSY", "jobID", "1", "lastHeartbeat", "0", "listenQueue", "default")

	req := httptest.NewRequest(http.MethodGet, "/status.json/", nil)
	w := httptest.NewRecorder()
	srv.overviewHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var data handlers.StatusData
	require.NoError(t, json.NewDecoder(w.Body).Decode(&data))
	assert.Len(t, data.Jobs, 2)
	assert.Len(t, data.Workers, 1)
}

func TestOverviewHandler_ScopesToTube(t *testing.T) {
	mr.FlushAll()
	ctx := context.Background()

	rdb.HSet(ctx, "job: 1", "id", "1", "status", "QUEUED", "priority", "2", "queue", "default")
	rdb.HSet(ctx, "job: 2", "id", "2", "status", "QUEUED", "priority", "2", "queue", "other")

	req := httptest.NewRequest(http.MethodGet, "/status.json/", nil)
	query := req.URL.Query()
	query.Add("queue", "default")
	req.URL.RawQuery = query.Encode()
	w := httptest.NewRecorder()
	srv.overviewHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var data handlers.StatusData
	require.NoError(t, json.NewDecoder(w.Body).Decode(&data))
	require.Len(t, data.Jobs, 1)
	assert.Equal(t, "1", data.Jobs[0].ID)
}
