package tasks

func NewDemoRegistry() *MapRegistry {
	return NewRegistry(map[string]HandlerFunc{
		"echo":        Echo,
		"send_email":  SendEmail,
		"always_fail": AlwaysFails,
		"slow_task":   SlowTask,
	})
}
