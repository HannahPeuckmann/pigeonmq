package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// main.go will pick the HTTP status code based on these
var (
	ErrNotFound       = errors.New("not found")
	ErrConflict       = errors.New("conflict")
	ErrNoJobAvailable = errors.New("no job available")
)

// in redis everything is stored as a string, so IDs are strings to avoid repeated conversions.
type Job struct {
	ID          string `json:"id" redis:"id"`
	Payload     []byte `json:"body" redis:"body"`
	Status      string `json:"status" redis:"status"`
	Priority    string `json:"priority" redis:"priority"`
	BuildStart  string `json:"build-start,omitempty" redis:"build-start"`
	BuildFinish string `json:"build-finish,omitempty" redis:"build-finish"`
	QueueStart  string `json:"queue-start" redis:"queue-start"`
	Queue       string `json:"queue" redis:"queue"`
	Worker      string `json:"worker" redis:"worker"`
}
type Worker struct {
	ID            string `json:"id" redis:"-"`
	Job           string `json:"job" redis:"jobID"`
	LastHeartbeat int64  `json:"heartbeat" redis:"lastHeartbeat"`
	Status        string `json:"status" redis:"status"`
	ListenQueue   string `json:"listen-queue" redis:"listenQueue"`
	Nickname      string `json:"nickname" redis:"nickname"`
}

func ReserveJob(ctx context.Context, rdb *redis.Client, workerID string) (Job, error) {
	workerKey := fmt.Sprintf("worker: %s", workerID)
	slog.Info("Reserve request", "worker", workerID)

	exists, err := rdb.Exists(ctx, workerKey).Result()
	if err != nil {
		return Job{}, err
	}
	if exists == 0 {
		return Job{}, fmt.Errorf("worker not registered: %w", ErrNotFound)
	}

	status, err := rdb.HGet(ctx, workerKey, "status").Result()
	if err != nil {
		return Job{}, err
	}
	if status != "IDLE" {
		return Job{}, fmt.Errorf("worker is not idle, delete the job before reserving a new one: %w", ErrConflict)
	}

	var job Job
	queueName, err := rdb.HGet(ctx, workerKey, "listenQueue").Result()
	if err != nil {
		return Job{}, err
	}

	jobZ, err := rdb.ZPopMin(ctx, queueName, 1).Result()
	if len(jobZ) == 0 {
		return Job{}, ErrNoJobAvailable
	}

	jobKey := jobZ[0].Member.(string)

	if err := rdb.HGetAll(ctx, jobKey).Scan(&job); err != nil {
		return Job{}, err
	}
	slog.Info("reserved job", "job", jobKey)

	p := rdb.Pipeline()
	p.HSet(ctx, jobKey, "status", "BUILDING")
	p.HSet(ctx, jobKey, "build-start", time.Now())
	p.HSet(ctx, jobKey, "worker", workerID)
	p.HSet(ctx, workerKey, "jobID", job.ID)
	p.HSet(ctx, workerKey, "status", "BUSY")
	job.Status = "BUILDING"

	if _, err := p.Exec(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

// CompleteJob is called by a worker to mark its currently-assigned job done
// and free itself up for the next one.
func CompleteJob(ctx context.Context, rdb *redis.Client, workerID string) error {
	workerKey := fmt.Sprintf("worker: %s", workerID)

	exists, err := rdb.Exists(ctx, workerKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("worker not registered: %w", ErrNotFound)
	}
	jobID, err := rdb.HGet(ctx, workerKey, "jobID").Result()
	if err != nil {
		return fmt.Errorf("failed to read job id from redis: %w", err)
	}
	if jobID == "none" {
		return fmt.Errorf("worker has no job assigned: %w", ErrConflict)
	}
	jobKey := fmt.Sprintf("job: %s", jobID)

	p := rdb.Pipeline()
	p.HSet(ctx, jobKey, "status", "DONE")
	p.HSet(ctx, jobKey, "build-finish", time.Now())
	p.Expire(ctx, jobKey, 5*24*time.Hour)
	p.HSet(ctx, workerKey, "status", "IDLE")
	p.HSet(ctx, workerKey, "jobID", "none")

	_, err = p.Exec(ctx)
	return err
}

// CancelJob cancels a job that hasn't started building yet, removing it from
// its tube so no worker will ever reserve it.
func CancelJob(ctx context.Context, rdb *redis.Client, jobID string) error {
	jobKey := fmt.Sprintf("job: %s", jobID)

	exists, err := rdb.Exists(ctx, jobKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("job does not exist: %w", ErrNotFound)
	}

	jobData, err := rdb.HGetAll(ctx, jobKey).Result()
	if err != nil {
		return fmt.Errorf("failed to read job from redis: %w", err)
	}
	if jobData["status"] != "QUEUED" {
		return fmt.Errorf("job is not queued, can not be cancelled: %w", ErrConflict)
	}

	p := rdb.Pipeline()
	p.HSet(ctx, jobKey, "status", "DONE")
	p.HSet(ctx, jobKey, "build-finish", time.Now())
	p.Expire(ctx, jobKey, 5*24*time.Hour)
	p.ZRem(ctx, jobData["queue"], jobKey)

	_, err = p.Exec(ctx)
	return err
}

func QueueJob(ctx context.Context, rdb *redis.Client, body []byte, priority float64, queueName string) (int64, error) {
	jobID, err := rdb.Incr(ctx, "jobID").Result()
	if err != nil {
		return 0, err
	}

	jobKey := fmt.Sprintf("job: %d", jobID)
	_, err = rdb.HSet(ctx, jobKey,
		"id", jobID,
		"status", "QUEUED",
		"priority", priority,
		"queue-start", time.Now(),
		"body", body,
		"queue", queueName,
	).Result()
	if err != nil {
		return 0, err
	}
	rdb.ZAdd(ctx, queueName, redis.Z{Score: priority, Member: jobKey})

	return jobID, nil
}
