package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func UpdateHeartbeat(ctx context.Context, rdb *redis.Client, workerID string) error {
	workerKey := fmt.Sprintf("worker: %s", workerID)

	exists, err := rdb.Exists(ctx, workerKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("worker not registered: %w", ErrNotFound)
	}

	_, err = rdb.HSet(ctx, workerKey, "lastHeartbeat", time.Now().Unix()).Result()
	return err
}

const heartbeatTimeout = 5 * time.Minute

func CheckHeartbeatsLoop(rdb *redis.Client) {
	ticker := time.NewTicker(heartbeatTimeout)
	defer ticker.Stop()
	for range ticker.C {
		CheckHeartbeats(rdb)
	}
}

func CheckHeartbeats(rdb *redis.Client) error {
	workerKeys, err := rdb.Keys(context.Background(), "worker: *").Result()
	if err != nil {
		slog.Error("Failed to get worker keys", "err", err)
		return err
	}
	for _, key := range workerKeys {
		workerData, err := rdb.HGetAll(context.Background(), key).Result()
		if err != nil {
			continue
		}

		lastHeartbeatUnix, err := strconv.ParseInt(workerData["lastHeartbeat"], 10, 64)
		if err != nil {
			continue
		}

		timeSinceLastHeartbeat := time.Since(time.Unix(lastHeartbeatUnix, 0))
		if timeSinceLastHeartbeat > heartbeatTimeout {
			if workerData["jobID"] != "none" {
				slog.Info("Stale worker detected, requeueing job", "worker", key, "jobID", workerData["jobID"])
				rdb.HSet(context.Background(), fmt.Sprintf("job: %s", workerData["jobID"]), "status", "QUEUED")
			}
			slog.Info("Removing stale worker", "worker", key, "lastHeartbeat", timeSinceLastHeartbeat)
			if _, err := rdb.Del(context.Background(), key).Result(); err != nil {
				slog.Error("Failed to delete stale worker", "worker", key, "err", err)
			}
		}
	}
	return nil
}
