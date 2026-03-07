package handlers

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type StatusData struct {
	Jobs    []Job
	Workers []Worker
}

type JsonStatus struct {
	TotalWorkers int `json:"totalWorkers"`
	IdleWorkers  int `json:"idleWorkers"`
	BusyWorkers  int `json:"busyWorkers"`
	JobsReady    int `json:"jobsReady"`
	JobsReserved int `json:"jobsReserved"`
	JobsDone     int `json:"jobsDone"`
}

func GetStatus(ctx context.Context, rdb *redis.Client) (StatusData, error) {
	var data StatusData

	workerKeys, err := rdb.Keys(ctx, "worker: *").Result()
	if err != nil {
		return data, err
	}
	for _, wkey := range workerKeys {
		var w Worker
		if err := rdb.HGetAll(ctx, wkey).Scan(&w); err != nil {
			continue
		}
		data.Workers = append(data.Workers, w)
	}

	jobKeys, err := rdb.Keys(ctx, "job: *").Result()
	if err != nil {
		return data, err
	}
	for _, jkey := range jobKeys {
		var j Job
		if err := rdb.HGetAll(ctx, jkey).Scan(&j); err != nil {
			continue
		}
		data.Jobs = append(data.Jobs, j)
	}

	return data, nil
}

func GetJobStatus(ctx context.Context, rdb *redis.Client, jobID string) (Job, error) {
	var job Job

	jobKey := fmt.Sprintf("job: %s", jobID)

	exists, err := rdb.Exists(ctx, jobKey).Result()
	if err != nil {
		return Job{}, err
	}
	if exists == 0 {
		return Job{}, fmt.Errorf("job does not exist: %w", ErrNotFound)
	}

	if err := rdb.HGetAll(ctx, jobKey).Scan(&job); err != nil {
		return Job{}, err
	}

	return job, nil
}

func GetJsonStatus(ctx context.Context, rdb *redis.Client) (JsonStatus, error) {
	var status JsonStatus

	workerKeys, err := rdb.Keys(ctx, "worker: *").Result()
	if err != nil {
		return status, err
	}
	for _, key := range workerKeys {
		workerData, err := rdb.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}
		status.TotalWorkers++
		if workerData["status"] == "IDLE" {
			status.IdleWorkers++
		} else {
			status.BusyWorkers++
		}
	}

	jobKeys, err := rdb.Keys(ctx, "job: *").Result()
	if err != nil {
		return status, err
	}
	for _, key := range jobKeys {
		jobData, err := rdb.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}
		switch jobData["status"] {
		case "QUEUED":
			status.JobsReady++
		case "BUILDING":
			status.JobsReserved++
		case "DONE":
			status.JobsDone++
		}
	}

	return status, nil
}
