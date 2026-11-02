package playground

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/dharatype"
)

type fakeTaskClient struct {
	inserts   []dhara.InsertParams
	insertErr error
	queue     *dhara.QueueMetrics
	metrics   dhara.MetricsSnapshot
	nextTask  *dhara.Task
	listTasks []dhara.Task
	getTask   *dhara.Task
}

func (f *fakeTaskClient) Insert(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error) {
	if f.insertErr != nil {
		return nil, f.insertErr
	}
	f.inserts = append(f.inserts, params)
	task := f.nextTask
	if task == nil {
		task = &dhara.Task{
			ID:         uuid.New(),
			Type:       params.Type,
			Status:     dhara.TaskStatusPending,
			Priority:   0,
			MaxRetries: 5,
			RunAt:      time.Now(),
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
	}
	return &dhara.EnqueueResult{Task: task}, nil
}

func (f *fakeTaskClient) GetTask(ctx context.Context, id string) (*dhara.Task, error) {
	if f.getTask != nil {
		return f.getTask, nil
	}
	return nil, dhara.ErrTaskNotFound
}
func (f *fakeTaskClient) ListTasks(ctx context.Context, filter dharatype.TaskListFilter) ([]dhara.Task, int, error) {
	return f.listTasks, len(f.listTasks), nil
}
func (f *fakeTaskClient) CancelTask(ctx context.Context, id string) (*dhara.Task, error) {
	return nil, dhara.ErrTaskNotFound
}
func (f *fakeTaskClient) RetryTask(ctx context.Context, id string) (*dhara.Task, error) {
	return nil, dhara.ErrTaskNotFound
}
func (f *fakeTaskClient) QueueMetrics(ctx context.Context) (*dhara.QueueMetrics, error) {
	if f.queue != nil {
		return f.queue, nil
	}
	return &dhara.QueueMetrics{}, nil
}
func (f *fakeTaskClient) Metrics() dhara.MetricsSnapshot { return f.metrics }

func TestServiceCreateTaskNamespacesSession(t *testing.T) {
	cfg := testConfig()
	client := &fakeTaskClient{}
	svc := NewService(cfg, client, nil, nil, nil)
	session := uuid.NewString()

	result, err := svc.CreateTask(context.Background(), session, CreateTaskInput{
		Type:    TypeEcho,
		Payload: []byte(`{"message":"hi"}`),
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if result.Task == nil {
		t.Fatal("task is nil")
	}
	if len(client.inserts) != 1 {
		t.Fatalf("inserts = %d, want 1", len(client.inserts))
	}
	key := client.inserts[0].IdempotencyKey
	if key == nil || !keyBelongsToSession(key, session) {
		t.Errorf("key = %v, want session-owned", key)
	}
}

func TestServiceCreateTaskRejectsUnknownTypeAndBadPayload(t *testing.T) {
	svc := NewService(testConfig(), &fakeTaskClient{}, nil, nil, nil)
	session := uuid.NewString()

	if _, err := svc.CreateTask(context.Background(), session, CreateTaskInput{Type: "shell"}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("unknown type error = %v, want ErrBadRequest", err)
	}
	if _, err := svc.CreateTask(context.Background(), session, CreateTaskInput{Type: TypeEcho, Payload: []byte(`[]`)}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("bad payload error = %v, want ErrBadRequest", err)
	}
	future := time.Now().Add(2 * time.Hour)
	if _, err := svc.CreateTask(context.Background(), session, CreateTaskInput{Type: TypeEcho, RunAt: &future}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("future run_at error = %v, want ErrBadRequest", err)
	}
}

func TestServiceQueueCap(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPending = 10
	client := &fakeTaskClient{queue: &dhara.QueueMetrics{TasksPending: 9}}
	svc := NewService(cfg, client, nil, nil, nil)

	_, err := svc.EnqueueBurst(context.Background(), uuid.NewString(), BurstRequest{Size: 2})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("error = %v, want ErrQueueFull", err)
	}
	if len(client.inserts) != 0 {
		t.Errorf("inserted %d tasks despite full queue", len(client.inserts))
	}
}

func TestServiceEnqueueBurst(t *testing.T) {
	cfg := testConfig()
	client := &fakeTaskClient{}
	svc := NewService(cfg, client, nil, nil, nil)
	session := uuid.NewString()

	result, err := svc.EnqueueBurst(context.Background(), session, BurstRequest{Size: 3})
	if err != nil {
		t.Fatalf("EnqueueBurst: %v", err)
	}
	if result.Count != 3 || len(result.TaskIDs) != 3 {
		t.Fatalf("result = %+v, want 3 tasks", result)
	}
	for _, in := range client.inserts {
		if in.Type != TypeEcho {
			t.Errorf("type = %q, want echo", in.Type)
		}
		if in.IdempotencyKey == nil || !keyBelongsToSession(in.IdempotencyKey, session) {
			t.Error("task key is not session-owned")
		}
	}
}

func TestServiceEnqueueFlakyModes(t *testing.T) {
	client := &fakeTaskClient{}
	svc := NewService(testConfig(), client, nil, nil, nil)
	session := uuid.NewString()

	if _, err := svc.EnqueueFlaky(context.Background(), session, FlakyRequest{Count: 2, Mode: "recover"}); err != nil {
		t.Fatalf("recover mode: %v", err)
	}
	if len(client.inserts) != 2 {
		t.Fatalf("recover inserts = %d, want 2", len(client.inserts))
	}
	for _, in := range client.inserts {
		if in.Type != TypeFlaky {
			t.Errorf("recover type = %q", in.Type)
		}
		if in.MaxRetries == nil || *in.MaxRetries != 8 {
			t.Errorf("recover max retries = %v, want 8", in.MaxRetries)
		}
	}

	client.inserts = nil
	if _, err := svc.EnqueueFlaky(context.Background(), session, FlakyRequest{Count: 1, Mode: "dead"}); err != nil {
		t.Fatalf("dead mode: %v", err)
	}
	if len(client.inserts) != 1 || client.inserts[0].Type != TypeAlwaysFail {
		t.Fatalf("dead mode inserts = %+v", client.inserts)
	}
	if *client.inserts[0].MaxRetries != 3 {
		t.Errorf("dead max retries = %d, want 3", *client.inserts[0].MaxRetries)
	}

	if _, err := svc.EnqueueFlaky(context.Background(), session, FlakyRequest{Mode: "invalid"}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("invalid mode error = %v", err)
	}
}

func TestServiceEnqueueSlow(t *testing.T) {
	client := &fakeTaskClient{}
	svc := NewService(testConfig(), client, nil, nil, nil)

	result, err := svc.EnqueueSlow(context.Background(), uuid.NewString(), SlowRequest{Count: 2})
	if err != nil {
		t.Fatalf("EnqueueSlow: %v", err)
	}
	if result.Count != 2 {
		t.Errorf("count = %d", result.Count)
	}
	for _, in := range client.inserts {
		if in.Type != TypeSlowTask {
			t.Errorf("type = %q", in.Type)
		}
		if *in.MaxRetries != 2 {
			t.Errorf("max retries = %d, want 2", *in.MaxRetries)
		}
	}
}

func TestServiceRejectsTooLargeBatch(t *testing.T) {
	svc := NewService(testConfig(), &fakeTaskClient{}, nil, nil, nil)
	if _, err := svc.EnqueueBurst(context.Background(), uuid.NewString(), BurstRequest{Size: 51}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("error = %v, want ErrBadRequest", err)
	}
}
