/*
aq is the command-line client for AwesomeQueue. It talks to a running
awesomequeue-server over HTTP, at http://localhost:11300 by default; set
AWESOMEQUEUE_ADDR to point it elsewhere.

Without arguments, aq prints command help. With a command, it executes the
requested operation. Commands may define their own flags.

Usage:

	aq <command> [command flags] [args...]

The available commands:

	status
	    Display the server status, optionally scoped to a tube with --tube.

	job-status [job_id]
	    Display the status of a single job.

	queue [body] --tube <tube>
	    Queue a job on a tube, with an optional --priority.

	delete [job_id]
	    Delete a job. Registered but not yet implemented.
*/
package main

import "awesomequeue/cli/cmd"

func main() {
	cmd.Execute()
}
