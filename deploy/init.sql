CREATE TABLE IF NOT EXISTS tasks (
       id TEXT PRIMARY KEY,
       title TEXT NOT NULL,
       description TEXT NOT NULL DEFAULT '',
       due_date TEXT NOT NULL DEFAULT '',
       done BOOLEAN NOT NULL DEFAULT FALSE,
       created_at TIMESTAMP NOT NULL DEFAULT NOW()
);