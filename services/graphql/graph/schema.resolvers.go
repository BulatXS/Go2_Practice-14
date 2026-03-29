package graph

import (
	"context"

	"singularity.com/pr14/services/graphql/internal/service"
)

func (r *queryResolver) Tasks(ctx context.Context) ([]*Task, error) {
	items, err := r.Resolver.Service.Tasks(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*Task, 0, len(items))
	for _, item := range items {
		description := item.Description
		dueDate := item.DueDate

		result = append(result, &Task{
			ID:          item.ID,
			Title:       item.Title,
			Description: &description,
			DueDate:     &dueDate,
			Done:        item.Done,
		})
	}

	return result, nil
}

func (r *queryResolver) Task(ctx context.Context, id string) (*Task, error) {
	item, err := r.Resolver.Service.Task(ctx, id)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	description := item.Description
	dueDate := item.DueDate

	return &Task{
		ID:          item.ID,
		Title:       item.Title,
		Description: &description,
		DueDate:     &dueDate,
		Done:        item.Done,
	}, nil
}

func (r *mutationResolver) CreateTask(ctx context.Context, input CreateTaskInput) (*Task, error) {
	item, err := r.Resolver.Service.CreateTask(ctx, service.CreateTaskInput{
		Title:       input.Title,
		Description: input.Description,
		DueDate:     input.DueDate,
	})
	if err != nil {
		return nil, err
	}

	description := item.Description
	dueDate := item.DueDate

	return &Task{
		ID:          item.ID,
		Title:       item.Title,
		Description: &description,
		DueDate:     &dueDate,
		Done:        item.Done,
	}, nil
}

func (r *mutationResolver) UpdateTask(ctx context.Context, id string, input UpdateTaskInput) (*Task, error) {
	item, err := r.Resolver.Service.UpdateTask(ctx, id, service.UpdateTaskInput{
		Title:       input.Title,
		Description: input.Description,
		DueDate:     input.DueDate,
		Done:        input.Done,
	})
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	description := item.Description
	dueDate := item.DueDate

	return &Task{
		ID:          item.ID,
		Title:       item.Title,
		Description: &description,
		DueDate:     &dueDate,
		Done:        item.Done,
	}, nil
}

func (r *mutationResolver) DeleteTask(ctx context.Context, id string) (bool, error) {
	return r.Resolver.Service.DeleteTask(ctx, id)
}

func (r *Resolver) Query() QueryResolver { return &queryResolver{r} }

func (r *Resolver) Mutation() MutationResolver { return &mutationResolver{r} }

type queryResolver struct{ *Resolver }
type mutationResolver struct{ *Resolver }
