package version

import (
	"flag"
	"fmt"
	"io"
	"os"
)

var (
	version     string
	date        string
	commit      string
	branch      string
	showVersion *bool
)

func Init() {
	if !flag.Parsed() {
		flag.Parse()
	}
	vv := flag.Lookup("version")
	if vv != nil && vv.Value.String() == "true" {
		printVersion(os.Stdout, version, commit, date, branch)
		os.Exit(0)
	}
}

func init() {
	vv := flag.Lookup("version")
	if vv == nil {
		showVersion = flag.Bool("version", false, "Print version of this binary (only valid if compiled with make)")
	}

	os.Setenv("app.version", version)
	os.Setenv("app.date", date)
	os.Setenv("app.commit", commit)
	os.Setenv("app.branch", branch)
}

func printVersion(w io.Writer, version, commit, date, branch string) {
	fmt.Fprintf(w, "Version: %s\n", version)
	fmt.Fprintf(w, "Branch: %s\n", branch)
	fmt.Fprintf(w, "CommitID: %s\n", commit)
	fmt.Fprintf(w, "Binary: %s\n", os.Args[0])
	fmt.Fprintf(w, "Compile date: %s\n", date)
	fmt.Fprintf(w, "(version and date only valid if compiled with make)\n")
}
