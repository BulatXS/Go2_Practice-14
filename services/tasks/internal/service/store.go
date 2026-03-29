package service

import (
	"sync"
)

type Store struct {
	mu    sync.RWMutex
	tasks map[string]Task
}

func NewStore() *Store {
	return &Store{
		tasks: make(map[string]Task),
	}
}

func (s *Store) Create(task Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID] = task
}

func (s *Store) GetAll() []Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		result = append(result, t)
	}
	return result
}

func (s *Store) Get(id string) (Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tasks[id]
	return t, ok
}

func (s *Store) Update(id string, updated Task) (Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tasks[id]; !ok {
		return Task{}, false
	}

	s.tasks[id] = updated
	return updated, true
}

func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tasks[id]; !ok {
		return false
	}

	delete(s.tasks, id)
	return true
}
