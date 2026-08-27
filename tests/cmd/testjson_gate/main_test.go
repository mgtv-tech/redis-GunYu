package main

import (
	"strings"
	"testing"
)

func TestEvaluatePassesRequiredPackageAndTest(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Action":"run","Package":"example/pkg","Test":"TestRequired"}`,
		`{"Action":"pass","Package":"example/pkg","Test":"TestRequired"}`,
		`{"Action":"pass","Package":"example/pkg"}`,
	}, "\n"))

	result, err := evaluate(input, []string{"example/pkg"}, []string{"TestRequired"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Failures) != 0 || len(result.Skipped) != 0 || len(result.Missing) != 0 {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestEvaluateRejectsSkipAndMissingTest(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Action":"run","Package":"example/pkg","Test":"TestSkipped"}`,
		`{"Action":"skip","Package":"example/pkg","Test":"TestSkipped"}`,
		`{"Action":"pass","Package":"example/pkg"}`,
	}, "\n"))

	result, err := evaluate(input, []string{"example/pkg"}, []string{"TestRequired"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Skipped) != 1 || len(result.Missing) != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestEvaluateRejectsPackageFailure(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Action":"fail","Package":"example/pkg","Test":"TestBroken"}`,
		`{"Action":"fail","Package":"example/pkg"}`,
	}, "\n"))

	result, err := evaluate(input, []string{"example/pkg"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Failures) != 2 || len(result.Missing) != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
}
