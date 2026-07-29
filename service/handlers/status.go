package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type StatusData struct {
	Jobs        []Job
	Workers     []Worker
	DeadWorkers []Worker
}

type JsonStatus struct {
	TotalWorkers int `json:"totalWorkers"`
	IdleWorkers  int `json:"idleWorkers"`
	BusyWorkers  int `json:"busyWorkers"`
	JobsReady    int `json:"jobsReady"`
	JobsReserved int `json:"jobsReserved"`
	JobsDone     int `json:"jobsDone"`
}

func GetStatusOverview(ctx context.Context, rdb *redis.Client, tube string) (StatusData, error) {
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
		if tube != "" && w.ListenQueue != tube {
			continue
		}
		w.ID = strings.TrimPrefix(wkey, "worker: ")
		data.Workers = append(data.Workers, w)
	}

	deadWorkerKeys, err := rdb.Keys(ctx, "dead-worker: *").Result()
	if err != nil {
		return data, err
	}
	for _, wkey := range deadWorkerKeys {
		var w Worker
		if err := rdb.HGetAll(ctx, wkey).Scan(&w); err != nil {
			continue
		}
		if tube != "" && w.ListenQueue != tube {
			continue
		}
		w.ID = strings.TrimPrefix(wkey, "worker: ")
		data.DeadWorkers = append(data.DeadWorkers, w)
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
		if tube != "" && j.Queue != tube {
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

// GetAggregateStatus returns worker/job counts. If tube is empty, all tubes are counted;
// otherwise only workers/jobs belonging to that tube are counted.
func GetAggregateStatus(ctx context.Context, rdb *redis.Client, tube string) (JsonStatus, error) {
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
		if tube != "" && workerData["listenQueue"] != tube {
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
		if tube != "" && jobData["queue"] != tube {
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
