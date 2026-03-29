package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"singularity.com/pr14/services/tasks/internal/rabbit"
	"singularity.com/pr14/services/tasks/internal/repository"

	"github.com/redis/go-redis/v9"
)

type TaskService struct {
	repo     repository.TaskRepository
	producer *rabbit.Producer
	cache    *redis.Client
	ttl      time.Duration
	jitter   time.Duration
	counter  uint64
}

func NewTaskService(
	repo repository.TaskRepository,
	producer *rabbit.Producer,
	cache *redis.Client,
	ttl time.Duration,
	jitter time.Duration,
) *TaskService {
	rand.Seed(time.Now().UnixNano())

	return &TaskService{
		repo:     repo,
		producer: producer,
		cache:    cache,
		ttl:      ttl,
		jitter:   jitter,
	}
}

func (s *TaskService) taskCacheKey(id string) string {
	return "tasks:task:" + id
}

func (s *TaskService) effectiveTTL() time.Duration {
	if s.jitter <= 0 {
		return s.ttl
	}
	return s.ttl + time.Duration(rand.Int63n(int64(s.jitter)+1))
}

func (s *TaskService) cacheSetTask(ctx context.Context, task Task) {
	if s.cache == nil {
		return
	}

	key := s.taskCacheKey(task.ID)

	data, err := json.Marshal(task)
	if err != nil {
		log.Printf("cache marshal error: %v", err)
		return
	}

	if err := s.cache.Set(ctx, key, data, s.effectiveTTL()).Err(); err != nil {
		log.Printf("redis set error for key=%s: %v", key, err)
	}
}

func (s *TaskService) invalidateTaskCache(ctx context.Context, id string) {
	if s.cache == nil {
		return
	}

	key := s.taskCacheKey(id)
	if err := s.cache.Del(ctx, key).Err(); err != nil {
		log.Printf("redis del error for key=%s: %v", key, err)
	}
}

func (s *TaskService) publishTaskCreated(taskID string) {
	if s.producer == nil {
		return
	}

	event := rabbit.TaskEvent{
		Event:    "task.created",
		TaskID:   taskID,
		TS:       time.Now().UTC().Format(time.RFC3339),
		Producer: "tasks",
		Version:  "v1",
	}

	if err := s.producer.Publish(event); err != nil {
		log.Printf("rabbit publish failed: %v", err)
	}
}

func (s *TaskService) Create(ctx context.Context, title, description, dueDate string) (Task, error) {
	id := fmt.Sprintf("t_%03d", atomic.AddUint64(&s.counter, 1))

	task := repository.Task{
		ID:          id,
		Title:       title,
		Description: description,
		DueDate:     dueDate,
		Done:        false,
	}

	if err := s.repo.Create(ctx, task); err != nil {
		return Task{}, err
	}

	result := Task{
		ID:          task.ID,
		Title:       task.Title,
		Description: task.Description,
		DueDate:     task.DueDate,
		Done:        task.Done,
	}

	s.publishTaskCreated(task.ID)

	return result, nil
}

func (s *TaskService) GetAll(ctx context.Context) ([]Task, error) {
	items, err := s.repo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]Task, 0, len(items))
	for _, t := range items {
		result = append(result, Task{
			ID:          t.ID,
			Title:       t.Title,
			Description: t.Description,
			DueDate:     t.DueDate,
			Done:        t.Done,
		})
	}

	return result, nil
}

func (s *TaskService) GetByID(ctx context.Context, id string) (Task, bool, error) {
	key := s.taskCacheKey(id)

	if s.cache != nil {
		data, err := s.cache.Get(ctx, key).Bytes()
		switch {
		case err == nil:
			var task Task
			if err := json.Unmarshal(data, &task); err == nil {
				log.Printf("cache hit key=%s", key)
				return task, true, nil
			}
			log.Printf("cache decode error for key=%s: %v", key, err)

		case err == redis.Nil:
			log.Printf("cache miss key=%s", key)

		default:
			log.Printf("redis get error for key=%s: %v", key, err)
		}
	}

	t, ok, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return Task{}, false, err
	}
	if !ok {
		return Task{}, false, nil
	}

	result := Task{
		ID:          t.ID,
		Title:       t.Title,
		Description: t.Description,
		DueDate:     t.DueDate,
		Done:        t.Done,
	}

	s.cacheSetTask(ctx, result)

	return result, true, nil
}

func (s *TaskService) Update(
	ctx context.Context,
	id string,
	title *string,
	description *string,
	dueDate *string,
	done *bool,
) (Task, bool, error) {
	t, ok, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return Task{}, false, err
	}
	if !ok {
		return Task{}, false, nil
	}

	if title != nil {
		t.Title = *title
	}
	if description != nil {
		t.Description = *description
	}
	if dueDate != nil {
		t.DueDate = *dueDate
	}
	if done != nil {
		t.Done = *done
	}

	if err := s.repo.Update(ctx, t); err != nil {
		return Task{}, false, err
	}

	s.invalidateTaskCache(ctx, id)

	result := Task{
		ID:          t.ID,
		Title:       t.Title,
		Description: t.Description,
		DueDate:     t.DueDate,
		Done:        t.Done,
	}

	return result, true, nil
}

func (s *TaskService) Delete(ctx context.Context, id string) (bool, error) {
	ok, err := s.repo.Delete(ctx, id)
	if err != nil {
		return false, err
	}

	if ok {
		s.invalidateTaskCache(ctx, id)
	}

	return ok, nil
}

func (s *TaskService) SearchByTitleSafe(ctx context.Context, title string) ([]Task, error) {
	items, err := s.repo.SearchByTitleSafe(ctx, title)
	if err != nil {
		return nil, err
	}

	result := make([]Task, 0, len(items))
	for _, t := range items {
		result = append(result, Task{
			ID:          t.ID,
			Title:       t.Title,
			Description: t.Description,
			DueDate:     t.DueDate,
			Done:        t.Done,
		})
	}

	return result, nil
}
