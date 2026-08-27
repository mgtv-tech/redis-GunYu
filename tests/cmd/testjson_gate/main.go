package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type testEvent struct {
	Action  string  `json:"Action"`
	Package string  `json:"Package"`
	Test    string  `json:"Test"`
	Elapsed float64 `json:"Elapsed"`
}

type gateResult struct {
	Packages map[string]string `json:"packages"`
	Tests    map[string]string `json:"tests"`
	Skipped  []string          `json:"skipped"`
	Failures []string          `json:"failures"`
	Missing  []string          `json:"missing"`
}

func main() {
	var requiredPackages stringList
	var requiredTests stringList
	input := flag.String("input", "-", "go test -json input file, or - for stdin")
	jsonOutput := flag.String("json-output", "", "write the gate result as JSON")
	markdownOutput := flag.String("markdown-output", "", "write the gate result as Markdown")
	allowSkip := flag.Bool("allow-skip", false, "allow skipped tests")
	flag.Var(&requiredPackages, "require-package", "package that must finish with PASS (repeatable)")
	flag.Var(&requiredTests, "require-test", "test name that must run and pass (repeatable)")
	flag.Parse()

	reader, closeInput, err := openInput(*input)
	if err != nil {
		fatalf("open input: %v", err)
	}
	defer closeInput()

	result, err := evaluate(reader, requiredPackages, requiredTests, *allowSkip)
	if err != nil {
		fatalf("evaluate test events: %v", err)
	}
	if err := writeOutputs(result, *jsonOutput, *markdownOutput); err != nil {
		fatalf("write output: %v", err)
	}

	printSummary(os.Stdout, result)
	if len(result.Failures) > 0 || len(result.Missing) > 0 || (!*allowSkip && len(result.Skipped) > 0) {
		os.Exit(1)
	}
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "-" {
		return os.Stdin, func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, func() {}, err
	}
	return f, func() { _ = f.Close() }, nil
}

func evaluate(reader io.Reader, requiredPackages, requiredTests []string, allowSkip bool) (gateResult, error) {
	result := gateResult{
		Packages: make(map[string]string),
		Tests:    make(map[string]string),
	}
	runTests := make(map[string]bool)

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event testEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return result, fmt.Errorf("decode line: %w", err)
		}
		if event.Package == "" {
			continue
		}
		if event.Test == "" {
			switch event.Action {
			case "pass", "fail", "skip":
				result.Packages[event.Package] = event.Action
				if event.Action == "fail" {
					result.Failures = append(result.Failures, event.Package)
				}
			}
			continue
		}

		key := event.Package + ":" + event.Test
		switch event.Action {
		case "run":
			runTests[key] = true
		case "pass", "fail", "skip":
			result.Tests[key] = event.Action
			if event.Action == "fail" {
				result.Failures = append(result.Failures, key)
			}
			if event.Action == "skip" && !allowSkip {
				result.Skipped = append(result.Skipped, key)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}

	for _, pkg := range requiredPackages {
		if result.Packages[pkg] != "pass" {
			result.Missing = append(result.Missing, "package:"+pkg)
		}
	}
	for _, required := range requiredTests {
		matched := false
		for key, ran := range runTests {
			if ran && (key == required || strings.HasSuffix(key, ":"+required)) && result.Tests[key] == "pass" {
				matched = true
				break
			}
		}
		if !matched {
			result.Missing = append(result.Missing, "test:"+required)
		}
	}

	sort.Strings(result.Skipped)
	sort.Strings(result.Failures)
	sort.Strings(result.Missing)
	return result, nil
}

func writeOutputs(result gateResult, jsonPath, markdownPath string) error {
	if jsonPath != "" {
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(jsonPath, append(data, '\n'), 0o644); err != nil {
			return err
		}
	}
	if markdownPath != "" {
		f, err := os.Create(markdownPath)
		if err != nil {
			return err
		}
		printSummary(f, result)
		return f.Close()
	}
	return nil
}

func printSummary(w io.Writer, result gateResult) {
	status := "PASS"
	if len(result.Failures) > 0 || len(result.Missing) > 0 || len(result.Skipped) > 0 {
		status = "FAIL"
	}
	fmt.Fprintf(w, "# Go Test Gate\n\n- Status: %s\n- Packages: %d\n- Tests: %d\n- Failures: %d\n- Skipped: %d\n- Missing: %d\n", status, len(result.Packages), len(result.Tests), len(result.Failures), len(result.Skipped), len(result.Missing))
	for _, item := range result.Failures {
		fmt.Fprintf(w, "- Failure: `%s`\n", item)
	}
	for _, item := range result.Skipped {
		fmt.Fprintf(w, "- Skip: `%s`\n", item)
	}
	for _, item := range result.Missing {
		fmt.Fprintf(w, "- Missing: `%s`\n", item)
	}
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
