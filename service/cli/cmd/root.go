package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"

	"awesomequeue/handlers"

	"github.com/fatih/color"
	"github.com/rodaine/table"
	"github.com/spf13/cobra"
)

const defaultAddr = "http://localhost:11300"

// maxBodyWidth caps the width of the "Body" column so long job payloads
// don't blow out the terminal line; anything longer wraps onto extra lines.
const maxBodyWidth = 60

// serverAddr is the base URL of the awesomequeue-server to talk to. It
// defaults to defaultAddr, overridable via the AWESOMEQUEUE_ADDR env var.
var serverAddr = func() string {
	if addr := os.Getenv("AWESOMEQUEUE_ADDR"); addr != "" {
		return strings.TrimSuffix(addr, "/")
	}
	return defaultAddr
}()

var rootCmd = &cobra.Command{
	Use:   "aq",
	Short: "Awesomequeue admin CLI",
	Long:  "Awesomequeue control tool for managing and monitoring the queue",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func init() {
	statusCmd.Flags().String("tube", "", "filter status by tube")
	statusCmd.Flags().Bool("all", false, "include DONE and CANCELLED jobs")
	queueCmd.Flags().String("tube", "", "tube to queue the job on")
	queueCmd.MarkFlagRequired("tube")
	queueCmd.Flags().String("priority", "medium", "job priority: urgent, high, medium, or low")

	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(jobStatusCmd)
	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(cancelCmd)
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Display the server status",
	Run: func(cmd *cobra.Command, args []string) {
		tube, _ := cmd.Flags().GetString("tube")
		all, _ := cmd.Flags().GetBool("all")

		status, err := getStatus(tube, all)
		if err != nil {
			slog.Error("error fetching status", "err", err)
			return
		}

		// create table layout and colors
		headerFmt := color.New(color.FgGreen, color.Underline).SprintfFunc()
		columnFmt := color.New(color.FgYellow).SprintfFunc()

		jobtbl := table.New("ID", "Body", "Status", "Worker", "QueueStart", "BuildStart", "BuildStop")
		jobtbl.WithHeaderFormatter(headerFmt).WithFirstColumnFormatter(columnFmt)

		for _, j := range status.Jobs {
			jobtbl.AddRow(j.ID, wrapText(string(j.Payload), maxBodyWidth), j.Status, j.Worker, j.QueueStart, j.BuildStart, j.BuildFinish)
		}

		workertbl := table.New("ID", "Nickname", "Status", "Job", "LastHeartbeat")
		workertbl.WithHeaderFormatter(headerFmt).WithFirstColumnFormatter(columnFmt)

		for _, w := range status.Workers {
			workertbl.AddRow(w.ID, w.Nickname, w.Status, w.Job, w.LastHeartbeat)
		}

		jobtbl.Print()
		workertbl.Print()

	},
}

var jobStatusCmd = &cobra.Command{
	Use:   "job-status [job_id]",
	Short: "Display the status of a job",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		statusURL, err := url.Parse(fmt.Sprintf("%s/job-status/", serverAddr))
		if err != nil {
			slog.Error("error parsing status url", "err", err)
			return
		}
		query := statusURL.Query()
		query.Add("job", args[0])
		statusURL.RawQuery = query.Encode()

		resp, err := http.Get(statusURL.String())
		if err != nil {
			slog.Error("An Error Occured", "error", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			slog.Error("job-status: unexpected status", "status", resp.StatusCode)
			return
		}

		var j handlers.Job
		if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
			slog.Error("error decoding job status json", "err", err)
			return
		}

		// create table layout and colors
		headerFmt := color.New(color.FgGreen, color.Underline).SprintfFunc()
		columnFmt := color.New(color.FgYellow).SprintfFunc()

		jobtbl := table.New("ID", "Body", "Status", "Worker", "QueueStart", "BuildStart", "BuildStop")
		jobtbl.WithHeaderFormatter(headerFmt).WithFirstColumnFormatter(columnFmt)

		jobtbl.AddRow(j.ID, wrapText(string(j.Payload), maxBodyWidth), j.Status, j.Worker, j.QueueStart, j.BuildStart, j.BuildFinish)
		jobtbl.Print()

	},
}

var queueCmd = &cobra.Command{
	Use:   "queue [body] --tube <tube>",
	Short: "Queue a job",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		tube, _ := cmd.Flags().GetString("tube")
		priority, _ := cmd.Flags().GetString("priority")

		jobID, err := queueJob(args[0], tube, priority)
		if err != nil {
			slog.Error("error queueing job", "err", err)
			return
		}

		fmt.Printf("queued job %d on tube %q\n", jobID, tube)
	},
}

var cancelCmd = &cobra.Command{
	Use:   "cancel [job_id]",
	Short: "Cancel a queued job",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cancelURL, err := url.Parse(fmt.Sprintf("%s/cancel/", serverAddr))
		if err != nil {
			slog.Error("error parsing cancel url", "err", err)
			return
		}
		query := cancelURL.Query()
		query.Add("job", args[0])
		cancelURL.RawQuery = query.Encode()

		resp, err := http.Post(cancelURL.String(), "", nil)
		if err != nil {
			slog.Error("An Error Occured", "error", err)
			return
		}

		if resp.StatusCode != http.StatusOK {
			slog.Error("cancel: unexpected status", "status", resp.StatusCode)
			return
		}
		fmt.Printf("cancelled job %s\n", args[0])

	},
}

func getStatus(tube string, all bool) (*handlers.StatusData, error) {
	statusURL, err := url.Parse(fmt.Sprintf("%s/status.json/", serverAddr))
	if err != nil {
		return nil, err
	}
	if tube != "" {
		query := statusURL.Query()
		query.Add("queue", tube)
		statusURL.RawQuery = query.Encode()
	}

	resp, err := http.Get(statusURL.String())
	if err != nil {
		slog.Error("An Error Occured", "error", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status: unexpected status %d", resp.StatusCode)
	}

	var status handlers.StatusData
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		slog.Error("error decoding status response json", "err", err)
		return nil, err
	}

	if !all {
		status.Jobs = slices.DeleteFunc(status.Jobs, func(j handlers.Job) bool {
			return j.Status == "CANCELLED" || j.Status == "DONE"
		})
	}

	return &status, nil
}

// wrapText breaks s into width-rune chunks joined by newlines, so it prints
// as multiple aligned lines within a single table cell.
func wrapText(s string, width int) string {
	runes := []rune(s)
	if width <= 0 || len(runes) <= width {
		return s
	}

	var lines []string
	for len(runes) > width {
		lines = append(lines, string(runes[:width]))
		runes = runes[width:]
	}
	lines = append(lines, string(runes))
	return strings.Join(lines, "\n")
}

var priorityValues = map[string]string{
	"urgent": "0",
	"high":   "1",
	"medium": "2",
	"low":    "3",
}

func queueJob(body, tube, priority string) (int64, error) {
	priorityParam, ok := priorityValues[priority]
	if !ok {
		return 0, fmt.Errorf("invalid priority %q: must be urgent, high, medium, or low", priority)
	}

	queueURL, err := url.Parse(fmt.Sprintf("%s/queue/", serverAddr))
	if err != nil {
		return 0, err
	}
	query := queueURL.Query()
	query.Add("queue", tube)
	query.Add("priority", priorityParam)
	queueURL.RawQuery = query.Encode()

	resp, err := http.Post(queueURL.String(), "application/octet-stream", strings.NewReader(body))
	if err != nil {
		slog.Error("An Error Occured", "error", err)
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("queue: unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var result struct {
		JobID int64 `json:"jobID"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		slog.Error("error decoding queue response json", "err", err)
		return 0, err
	}
	return result.JobID, nil
}
