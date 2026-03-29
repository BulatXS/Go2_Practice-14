package repository

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

type Task struct {
	ID          string
	Title       string
	Description string
	DueDate     string
	Done        bool
	CreatedAt   time.Time
}

type TaskRepository interface {
	Create(ctx context.Context, task Task) error
	GetAll(ctx context.Context) ([]Task, error)
	GetByID(ctx context.Context, id string) (Task, bool, error)
	Update(ctx context.Context, task Task) error
	Delete(ctx context.Context, id string) (bool, error)
	SearchByTitleSafe(ctx context.Context, title string) ([]Task, error)
}

type PostgresTaskRepository struct {
	db *sql.DB
}

func NewPostgresTaskRepository(db *sql.DB) *PostgresTaskRepository {
	return &PostgresTaskRepository{db: db}
}

func OpenPostgres(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func (r *PostgresTaskRepository) Create(ctx context.Context, task Task) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO tasks (id, title, description, due_date, done) VALUES ($1, $2, $3, $4, $5)`,
		task.ID, task.Title, task.Description, task.DueDate, task.Done,
	)
	return err
}

func (r *PostgresTaskRepository) GetAll(ctx context.Context) ([]Task, error) {
	rows, err := r.db.QueryContext(
		ctx,
		`SELECT id, title, description, due_date, done, created_at FROM tasks ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Title, &t.Description, &t.DueDate, &t.Done, &t.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, t)
	}

	return result, rows.Err()
}

func (r *PostgresTaskRepository) GetByID(ctx context.Context, id string) (Task, bool, error) {
	var t Task
	err := r.db.QueryRowContext(
		ctx,
		`SELECT id, title, description, due_date, done, created_at FROM tasks WHERE id = $1`,
		id,
	).Scan(&t.ID, &t.Title, &t.Description, &t.DueDate, &t.Done, &t.CreatedAt)

	if err == sql.ErrNoRows {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, err
	}

	return t, true, nil
}

func (r *PostgresTaskRepository) Update(ctx context.Context, task Task) error {
	_, err := r.db.ExecContext(
		ctx,
		`UPDATE tasks SET title = $1, description = $2, due_date = $3, done = $4 WHERE id = $5`,
		task.Title, task.Description, task.DueDate, task.Done, task.ID,
	)
	return err
}

func (r *PostgresTaskRepository) Delete(ctx context.Context, id string) (bool, error) {
	res, err := r.db.ExecContext(ctx, `DELETE FROM tasks WHERE id = $1`, id)
	if err != nil {
		return false, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return affected > 0, nil
}

func (r *PostgresTaskRepository) SearchByTitleSafe(ctx context.Context, title string) ([]Task, error) {
	rows, err := r.db.QueryContext(
		ctx,
		`SELECT id, title, description, due_date, done, created_at FROM tasks WHERE title = $1 ORDER BY created_at DESC`,
		title,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Title, &t.Description, &t.DueDate, &t.Done, &t.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, t)
	}

	return result, rows.Err()
}
