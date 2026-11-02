package playground

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNormalizePayload(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		task    string
		payload string
		max     int
		wantErr bool
	}{
		{"empty defaults to object", TypeEcho, "", 1024, false},
		{"echo message", TypeEcho, `{"message":"hello"}`, 1024, false},
		{"echo too long", TypeEcho, `{"message":"` + strings.Repeat("x", 1001) + `"}`, 2048, true},
		{"send email", TypeSendEmail, `{"to":"a@example.com","subject":"hi"}`, 1024, false},
		{"send email long", TypeSendEmail, `{"to":"` + strings.Repeat("a", 321) + `","subject":"hi"}`, 1024, true},
		{"flaky future", TypeFlaky, `{"succeed_at":` + itoa(now.Add(time.Minute).UnixMilli()) + `}`, 1024, false},
		{"flaky missing", TypeFlaky, `{}`, 1024, true},
		{"flaky outside window", TypeFlaky, `{"succeed_at":` + itoa(now.Add(2*time.Hour).UnixMilli()) + `}`, 1024, true},
		{"array rejected", TypeEcho, `[]`, 1024, true},
		{"invalid json", TypeEcho, `{`, 1024, true},
		{"payload too large", TypeRealisticWork, `{"label":"` + strings.Repeat("x", 100) + `"}`, 50, true},
		{"unknown type", "shell", `{}`, 1024, true},
		{"crash type not public", TypeCrash, `{"duration_ms":1}`, 1024, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := normalizePayload(tt.task, json.RawMessage(tt.payload), now, tt.max)
			if (err != nil) != tt.wantErr {
				t.Errorf("normalizePayload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateCount(t *testing.T) {
	if got, err := validateCount(0, 25, 50); err != nil || got != 25 {
		t.Errorf("default = %d, %v", got, err)
	}
	if got, err := validateCount(7, 25, 50); err != nil || got != 7 {
		t.Errorf("explicit = %d, %v", got, err)
	}
	if _, err := validateCount(51, 25, 50); err == nil {
		t.Error("over max accepted")
	}
	if _, err := validateCount(-1, 25, 50); err == nil {
		t.Error("negative accepted")
	}
}

func TestAllowedTaskTypes(t *testing.T) {
	for _, typ := range []string{TypeEcho, TypeSendEmail, TypeAlwaysFail, TypeSlowTask, TypeRealisticWork, TypeFlaky} {
		if !isAllowedType(typ) {
			t.Errorf("%s should be allowed", typ)
		}
	}
	for _, typ := range []string{TypeCrash, "shell", "http_get", ""} {
		if isAllowedType(typ) {
			t.Errorf("%s should not be allowed through the public API", typ)
		}
	}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
