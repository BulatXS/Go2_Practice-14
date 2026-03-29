package service

import (
	"context"

	"singularity.com/pr14/services/graphql/internal/repository"
)

type Task struct {
	ID          string
	Title       string
	Description string
	DueDate     string
	Done        bool
}

type CreateTaskInput struct {
	Title       string
	Description *string
	DueDate     *string
}

type UpdateTaskInput struct {
	Title       *string
	Description *string
	DueDate     *string
	Done        *bool
}

type Service struct {
	repo *repository.Repository
}

func New(repo *repository.Repository) *Service {
	return &Service{repo: repo}
}

func toServiceTask(t *repository.Task) *Task {
	if t == nil {
		return nil
	}

	return &Task{
		ID:          t.ID,
		Title:       t.Title,
		Description: t.Description,
		DueDate:     t.DueDate,
		Done:        t.Done,
	}
}

func (s *Service) Tasks(ctx context.Context) ([]*Task, error) {
	items, err := s.repo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*Task, 0, len(items))
	for _, item := range items {
		task := item
		result = append(result, toServiceTask(&task))
	}

	return result, nil
}

func (s *Service) Task(ctx context.Context, id string) (*Task, error) {
	item, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return toServiceTask(item), nil
}

func (s *Service) CreateTask(ctx context.Context, input CreateTaskInput) (*Task, error) {
	description := ""
	dueDate := ""

	if input.Description != nil {
		description = *input.Description
	}
	if input.DueDate != nil {
		dueDate = *input.DueDate
	}

	item, err := s.repo.Create(ctx, input.Title, description, dueDate)
	if err != nil {
		return nil, err
	}

	return toServiceTask(item), nil
}

func (s *Service) UpdateTask(ctx context.Context, id string, input UpdateTaskInput) (*Task, error) {
	item, err := s.repo.Update(
		ctx,
		id,
		input.Title,
		input.Description,
		input.DueDate,
		input.Done,
	)
	if err != nil {
		return nil, err
	}

	return toServiceTask(item), nil
}

func (s *Service) DeleteTask(ctx context.Context, id string) (bool, error) {
	return s.repo.Delete(ctx, id)
}
