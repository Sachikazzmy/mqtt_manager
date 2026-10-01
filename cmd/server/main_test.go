package main

import (
	"testing"
	"time"

	"Project/internal/storage"
)

func TestMigrationTimeoutFromEnv(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("MIGRATION_TIMEOUT", "")
		got, err := migrationTimeoutFromEnv()
		if err != nil || got != storage.DefaultMigrationTimeout {
			t.Fatalf("default migration timeout = %v, %v", got, err)
		}
	})
	t.Run("configured", func(t *testing.T) {
		t.Setenv("MIGRATION_TIMEOUT", " 45m ")
		got, err := migrationTimeoutFromEnv()
		if err != nil || got != 45*time.Minute {
			t.Fatalf("configured migration timeout = %v, %v", got, err)
		}
	})
	for _, value := range []string{"0s", "-1m", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("MIGRATION_TIMEOUT", value)
			if _, err := migrationTimeoutFromEnv(); err == nil {
				t.Fatalf("MIGRATION_TIMEOUT=%q 应报错", value)
			}
		})
	}
}

func TestRemoveLastRune(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty", input: "", want: ""},
		{name: "ascii", input: "abc", want: "ab"},
		{name: "multibyte", input: "a温度", want: "a温"},
		{name: "single multibyte", input: "温", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := string(removeLastRune([]byte(test.input))); got != test.want {
				t.Fatalf("removeLastRune(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}
