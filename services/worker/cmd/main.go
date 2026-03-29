package main

import (
	"log"
	"os"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rabbitURL := getEnv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
	queueName := getEnv("QUEUE_NAME", "task_events")
	prefetch := getEnvInt("WORKER_PREFETCH", 1)

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

	q, err := ch.QueueDeclare(
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

	log.Printf("Worker started (queue=%s, prefetch=%d)", queueName, prefetch)

	for msg := range msgs {
		log.Printf("received: %s", msg.Body)

		// обработка
		err := process(msg.Body)
		if err != nil {
			msg.Nack(false, true)
			continue
		}

		msg.Ack(false)
	}
}

func process(body []byte) error {
	log.Printf("processed: %s", body)
	return nil
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
