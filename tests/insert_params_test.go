package tests

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/md-talim/dhara"
)

func TestInsertParams_NormalizeAppliesDefaults(t *testing.T) {
	now := time.Now()
	params := &dhara.InsertParams{Type: "send_email"}

	params.Normalize(now)

	if string(params.Payload) != "{}" {
		t.Errorf("payload = %s, want {}", params.Payload)
	}
	if params.Priority == nil || *params.Priority != 0 {
		t.Errorf("priority = %v, want 0", params.Priority)
	}
	if params.MaxRetries == nil || *params.MaxRetries != 5 {
		t.Errorf("max_retries = %v, want 5", params.MaxRetries)
	}
	if params.RunAt == nil || !params.RunAt.Equal(now) {
		t.Errorf("run_at = %v, want %v", params.RunAt, now)
	}
}

func TestInsertParams_NormalizeKeepsProvidedValues(t *testing.T) {
	now := time.Now()
	future := now.Add(time.Hour)
	params := &dhara.InsertParams{
		Type:       "send_email",
		Payload:    json.RawMessage(`{"to":"x"}`),
		Priority:   new(7),
		MaxRetries: new(0),
		RunAt:      new(future),
	}

	params.Normalize(now)

	if *params.Priority != 7 {
		t.Errorf("priority = %d, want 7", *params.Priority)
	}
	if *params.MaxRetries != 0 {
		t.Errorf("max_retries = %d, want 0 (explicit zero must be preserved)", *params.MaxRetries)
	}
	if !params.RunAt.Equal(future) {
		t.Errorf("run_at = %v, want %v", params.RunAt, future)
	}
}

func TestInsertParams_Validate(t *testing.T) {
	now := time.Now()
	bigPayload := json.RawMessage(make([]byte, 64*1024+1))

	tests := []struct {
		name    string
		params  *dhara.InsertParams
		wantErr bool
	}{
		{
			name:    "valid minimal",
			params:  &dhara.InsertParams{Type: "send_email", Payload: json.RawMessage(`{}`), Priority: new(0), MaxRetries: new(5), RunAt: new(now)},
			wantErr: false,
		},
		{
			name:    "missing type",
			params:  &dhara.InsertParams{Type: "", Payload: json.RawMessage(`{}`), Priority: new(0), MaxRetries: new(5), RunAt: new(now)},
			wantErr: true,
		},
		{
			name:    "priority out of range",
			params:  &dhara.InsertParams{Type: "send_email", Payload: json.RawMessage(`{}`), Priority: new(101), MaxRetries: new(5), RunAt: new(now)},
			wantErr: true,
		},
		{
			name:    "max retries too high",
			params:  &dhara.InsertParams{Type: "send_email", Payload: json.RawMessage(`{}`), Priority: new(0), MaxRetries: new(21), RunAt: new(now)},
			wantErr: true,
		},
		{
			name:    "payload too large",
			params:  &dhara.InsertParams{Type: "send_email", Payload: bigPayload, Priority: new(0), MaxRetries: new(5), RunAt: new(now)},
			wantErr: true,
		},
		{
			name:    "run_at too far in the past",
			params:  &dhara.InsertParams{Type: "send_email", Payload: json.RawMessage(`{}`), Priority: new(0), MaxRetries: new(5), RunAt: new(now.Add(-10 * time.Minute))},
			wantErr: true,
		},
		{
			name:    "run_at too far in the future",
			params:  &dhara.InsertParams{Type: "send_email", Payload: json.RawMessage(`{}`), Priority: new(0), MaxRetries: new(5), RunAt: new(now.Add(31 * 24 * time.Hour))},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.params.Validate(now)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
