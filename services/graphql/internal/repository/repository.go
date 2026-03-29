package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

type Repository struct {
	db *sql.DB
}

func New(db *sql.DB) *Repository {
	return &Repository{db: db}
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

func (r *Repository) GetAll(ctx context.Context) ([]Task, error) {
	rows, err := r.db.QueryContext(
		ctx,
		`SELECT id, title, description, due_date, done, created_at
		 FROM tasks
		 ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(
			&t.ID,
			&t.Title,
			&t.Description,
			&t.DueDate,
			&t.Done,
			&t.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, t)
	}

	return result, rows.Err()
}

func (r *Repository) GetByID(ctx context.Context, id string) (*Task, error) {
	var t Task

	err := r.db.QueryRowContext(
		ctx,
		`SELECT id, title, description, due_date, done, created_at
		 FROM tasks
		 WHERE id = $1`,
		id,
	).Scan(
		&t.ID,
		&t.Title,
		&t.Description,
		&t.DueDate,
		&t.Done,
		&t.CreatedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (r *Repository) nextID(ctx context.Context) (string, error) {
	var count int

	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tasks`).Scan(&count)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("t_%03d", count+1), nil
}

func (r *Repository) Create(ctx context.Context, title, description, dueDate string) (*Task, error) {
	id, err := r.nextID(ctx)
	if err != nil {
		return nil, err
	}

	_, err = r.db.ExecContext(
		ctx,
		`INSERT INTO tasks (id, title, description, due_date, done)
		 VALUES ($1, $2, $3, $4, $5)`,
		id, title, description, dueDate, false,
	)
	if err != nil {
		return nil, err
	}

	return r.GetByID(ctx, id)
}

func (r *Repository) Update(
	ctx context.Context,
	id string,
	title *string,
	description *string,
	dueDate *string,
	done *bool,
) (*Task, error) {
	task, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, nil
	}

	if title != nil {
		task.Title = *title
	}
	if description != nil {
		task.Description = *description
	}
	if dueDate != nil {
		task.DueDate = *dueDate
	}
	if done != nil {
		task.Done = *done
	}

	_, err = r.db.ExecContext(
		ctx,
		`UPDATE tasks
		 SET title = $1, description = $2, due_date = $3, done = $4
		 WHERE id = $5`,
		task.Title, task.Description, task.DueDate, task.Done, task.ID,
	)
	if err != nil {
		return nil, err
	}

	return r.GetByID(ctx, id)
}

func (r *Repository) Delete(ctx context.Context, id string) (bool, error) {
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
