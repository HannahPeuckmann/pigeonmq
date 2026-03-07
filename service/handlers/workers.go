package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
)

func RegisterWorker(ctx context.Context, rdb *redis.Client, queue string) (int64, error) {
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

	jobID, err := rdb.HGet(ctx, workerKey, "jobID").Result()
	if err != nil {
		slog.Error("failed to get worker jobID, continuing to remove the worker", "err", err)
	}
	if jobID != "none" {
		slog.Info("worker unregistering with job assigned, requeueing", "worker", workerKey, "jobID", jobID)
		rdb.HSet(ctx, fmt.Sprintf("job: %s", jobID), "status", "QUEUED")
	}

	_, err = rdb.Del(ctx, workerKey).Result()
	return err
}

func newID() int {
	return rand.IntN(10000)
}
