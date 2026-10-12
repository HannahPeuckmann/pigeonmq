package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func RegisterWorker(ctx context.Context, rdb *redis.Client, queue, nickname string) (int64, error) {
	workerID, err := rdb.Incr(ctx, "workerID").Result()
	if err != nil {
		return 0, err
	}
	_, err = rdb.HSet(ctx,
		fmt.Sprintf("worker: %d", workerID),
		"lastHeartbeat", time.Now().Unix(),
		"jobID", "none",
		"status", "IDLE",
		"listenQueue", queue,
		"nickname", nickname,
	).Result()
	if err != nil {
		return 0, err
	}
	return workerID, nil
}

func UnregisterWorker(ctx context.Context, rdb *redis.Client, workerID string) error {
	workerKey := fmt.Sprintf("worker: %s", workerID)

	exists, err := rdb.Exists(ctx, workerKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("worker not registered: %w", ErrNotFound)
	}
	var w Worker
	if err := rdb.HGetAll(ctx, workerKey).Scan(&w); err != nil {
		return err
	}

	if w.Job != "none" && w.Job != "" {
		jobKey := fmt.Sprintf("job: %s", w.Job)
		slog.Info("worker unregistering with job assigned, requeueing", "worker", workerKey, "jobID", w.Job)

		jobData, err := rdb.HGetAll(ctx, jobKey).Result()
		if err != nil {
			slog.Error("failed to read job, skipping requeue", "job", jobKey, "err", err)
		} else {
			priority, err := strconv.ParseFloat(jobData["priority"], 64)
			if err != nil {
				slog.Error("failed to parse job priority, skipping requeue", "job", jobKey, "err", err)
			} else {
				p := rdb.Pipeline()
				p.HSet(ctx, jobKey, "status", "QUEUED")
				p.HSet(ctx, jobKey, "worker", "")
				p.HSet(ctx, jobKey, "build-start", "")
				p.ZAdd(ctx, jobData["queue"], redis.Z{Score: priority, Member: jobKey})
				if _, err := p.Exec(ctx); err != nil {
					slog.Error("failed to requeue job", "job", jobKey, "err", err)
				}
			}
		}
	}

	// keep a history of dead workers for five days
	_, err = rdb.HSet(ctx,
		fmt.Sprintf("dead-worker: %s", workerID),
		"listenQueue", w.ListenQueue,
		"nickname", w.Nickname,
	).Result()

	rdb.Expire(ctx, fmt.Sprintf("dead-worker: %s", workerID), 5*24*time.Hour)

	if err != nil {
		return err
	}

	_, err = rdb.Del(ctx, workerKey).Result()
	return err
}

func newID() int {
	return rand.IntN(10000)
}
