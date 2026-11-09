// simulate reads one Case as JSON on stdin and writes the Result as JSON on stdout.
// On a bad case it writes {"error":"..."} and exits 0, so a parity run can compare errors too.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"graft/kernel/pipeline"
)

func main() {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var c pipeline.Case
	if err := json.Unmarshal(raw, &c); err != nil {
		fmt.Fprintln(os.Stderr, "bad json:", err)
		os.Exit(2)
	}
	res, err := pipeline.Simulate(c)
	enc := json.NewEncoder(os.Stdout)
	if err != nil {
		enc.Encode(map[string]string{"error": err.Error()})
		return
	}
	enc.Encode(res)
}
