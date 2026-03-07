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
	BuildStart  string `json:"build-start" redis:"build-start"`
	BuildFinish string `json:"build-finish" redis:"build-finish"`
	QueueStart  string `json:"queue-start" redis:"queue-start"`
}

type Worker struct {
	ID            string    `json:"id"`
	Job           string    `json:"job"`
	LastHeartbeat time.Time `json:"heartbeat"`
	Status        string    `json:"status"`
	ListenQueue   string    `json:"listen-queue"`
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
	p.HSet(ctx, workerKey, "jobID", job.ID)
	p.HSet(ctx, workerKey, "status", "BUSY")
	job.Status = "BUILDING"

	if _, err := p.Exec(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func DeleteJob(ctx context.Context, rdb *redis.Client, workerID string) error {
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

	if _, err := p.Exec(ctx); err != nil {
		return err
	}
	return nil
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
		"build-start", 0,
		"build-finish", 0,
		"body", body,
		"queue", queueName,
	).Result()
	if err != nil {
		return 0, err
	}
	rdb.ZAdd(ctx, queueName, redis.Z{Score: priority, Member: jobKey})

	return jobID, nil
}
