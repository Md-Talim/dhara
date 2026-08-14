-- Rollback for 000001_init_task_queue

DROP TABLE IF EXISTS dead_letters;
DROP TABLE IF EXISTS task_logs;
DROP TABLE IF EXISTS tasks;
