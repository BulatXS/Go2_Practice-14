package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ProcessTaskJob struct {
	Job       string `json:"job"`
	TaskID    string `json:"task_id"`
	Attempt   int    `json:"attempt"`
	MessageID string `json:"message_id"`
}

type processedStore struct {
	mu   sync.RWMutex
	seen map[string]time.Time
}

func newProcessedStore() *processedStore {
	return &processedStore{seen: make(map[string]time.Time)}
}

func (s *processedStore) Exists(messageID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.seen[messageID]
	return ok
}

func (s *processedStore) Mark(messageID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[messageID] = time.Now().UTC()
}

func main() {
	rabbitURL := getEnv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
	queueName := getEnv("QUEUE_NAME", "task_jobs")
	dlxName := getEnv("DLX_NAME", "task_jobs_dlx")
	dlqName := getEnv("DLQ_NAME", "task_jobs_dlq")
	prefetch := getEnvInt("WORKER_PREFETCH", 1)
	maxAttempts := getEnvInt("MAX_ATTEMPTS", 3)
	minDelayMs := getEnvInt("PROCESSING_MIN_MS", 2000)
	maxDelayMs := getEnvInt("PROCESSING_MAX_MS", 5000)

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbit dial failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel open failed: %v", err)
	}
	defer ch.Close()

	if err := declareTopology(ch, queueName, dlxName, dlqName); err != nil {
		log.Fatalf("topology declare failed: %v", err)
	}

	q, err := ch.QueueDeclarePassive(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("queue declare failed: %v", err)
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		log.Fatalf("qos set failed: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, // autoAck = false
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("consume start failed: %v", err)
	}

	store := newProcessedStore()
	processor := &jobProcessor{
		channel:      ch,
		queueName:    queueName,
		maxAttempts:  maxAttempts,
		minDelay:     time.Duration(minDelayMs) * time.Millisecond,
		maxDelay:     time.Duration(maxDelayMs) * time.Millisecond,
		processedIDs: store,
		random:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	log.Printf(
		"Worker started (queue=%s, dlq=%s, prefetch=%d, max_attempts=%d)",
		queueName,
		dlqName,
		prefetch,
		maxAttempts,
	)

	for msg := range msgs {
		if err := processor.handle(msg); err != nil {
			log.Printf("message handling failed: %v", err)
		}
	}
}

type jobProcessor struct {
	channel      *amqp.Channel
	queueName    string
	maxAttempts  int
	minDelay     time.Duration
	maxDelay     time.Duration
	processedIDs *processedStore
	random       *rand.Rand
}

func (p *jobProcessor) handle(msg amqp.Delivery) error {
	var job ProcessTaskJob
	if err := json.Unmarshal(msg.Body, &job); err != nil {
		log.Printf("invalid payload, dead-lettering message: %v", err)
		return msg.Nack(false, false)
	}

	if job.Job != "process_task" || strings.TrimSpace(job.TaskID) == "" || strings.TrimSpace(job.MessageID) == "" {
		log.Printf("invalid job contract, dead-lettering message_id=%q task_id=%q", job.MessageID, job.TaskID)
		return msg.Nack(false, false)
	}

	if job.Attempt <= 0 {
		job.Attempt = 1
	}

	if p.processedIDs.Exists(job.MessageID) {
		log.Printf("duplicate message ignored: message_id=%s task_id=%s", job.MessageID, job.TaskID)
		return msg.Ack(false)
	}

	log.Printf("processing job: task_id=%s message_id=%s attempt=%d", job.TaskID, job.MessageID, job.Attempt)

	if err := p.process(job); err != nil {
		if job.Attempt >= p.maxAttempts {
			log.Printf(
				"attempt limit reached, dead-lettering: task_id=%s message_id=%s attempt=%d error=%v",
				job.TaskID,
				job.MessageID,
				job.Attempt,
				err,
			)
			return msg.Nack(false, false)
		}

		job.Attempt++
		if republishErr := p.publishRetry(job); republishErr != nil {
			return errors.Join(err, republishErr)
		}

		log.Printf(
			"retry scheduled: task_id=%s message_id=%s next_attempt=%d error=%v",
			job.TaskID,
			job.MessageID,
			job.Attempt,
			err,
		)
		return msg.Ack(false)
	}

	p.processedIDs.Mark(job.MessageID)
	log.Printf("job processed successfully: task_id=%s message_id=%s", job.TaskID, job.MessageID)
	return msg.Ack(false)
}

func (p *jobProcessor) process(job ProcessTaskJob) error {
	delay := p.minDelay
	if p.maxDelay > p.minDelay {
		delay += time.Duration(p.random.Int63n(int64(p.maxDelay - p.minDelay)))
	}

	time.Sleep(delay)

	taskID := strings.ToLower(strings.TrimSpace(job.TaskID))

	switch {
	case taskID == "t_fail":
		return errors.New("simulated permanent failure")
	case taskID == "t_flaky" && job.Attempt < 2:
		return errors.New("simulated transient failure")
	case strings.HasSuffix(taskID, "3"):
		return errors.New("simulated deterministic failure for ids ending with 3")
	default:
		return nil
	}
}

func (p *jobProcessor) publishRetry(job ProcessTaskJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return p.channel.PublishWithContext(
		ctx,
		"",
		p.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			MessageId:    job.MessageID,
			Timestamp:    time.Now().UTC(),
		},
	)
}

func declareTopology(ch *amqp.Channel, queueName, dlxName, dlqName string) error {
	if dlxName != "" {
		if err := ch.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil); err != nil {
			return err
		}
	}

	args := amqp.Table{}
	if dlxName != "" {
		args["x-dead-letter-exchange"] = dlxName
		if dlqName != "" {
			args["x-dead-letter-routing-key"] = dlqName
		}
	}

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, args); err != nil {
		return err
	}

	if dlqName == "" {
		return nil
	}

	q, err := ch.QueueDeclare(dlqName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	if dlxName == "" {
		return nil
	}

	return ch.QueueBind(q.Name, q.Name, dlxName, false, nil)
}

func getEnv(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		log.Printf("invalid %s=%q, using %d", name, raw, fallback)
		return fallback
	}
	return value
}
