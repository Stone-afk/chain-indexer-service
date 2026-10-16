package main

import (
	"bytes"
	"context"
	"testing"
)

func TestMigrateRequiresDirection(t *testing.T) {
	assertMigrateFailure(t, nil)
}

func TestMigrateRequiresConfig(t *testing.T) {
	assertMigrateFailure(t, []string{"status"})
}

func TestMigrateRejectsUnknownDirection(t *testing.T) {
	assertMigrateFailure(t, []string{"force", "--config", "ignored.yaml"})
}

func TestMigrateRejectsExtraArguments(t *testing.T) {
	assertMigrateFailure(t, []string{"status", "--config", "ignored.yaml", "extra"})
}

func assertMigrateFailure(t *testing.T, args []string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr, func(string) (string, bool) {
		t.Fatal("argument error must not read environment")
		return "", false
	})
	if code == 0 {
		t.Fatalf("run(%v) succeeded", args)
	}
}
