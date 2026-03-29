package rabbit

import (
	"context"
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

type TaskEvent struct {
	Event     string `json:"event"`
	TaskID    string `json:"task_id"`
	TS        string `json:"ts"`
	RequestID string `json:"request_id,omitempty"`
	Producer  string `json:"producer,omitempty"`
	Version   string `json:"version,omitempty"`
}

func NewProducer(url, queueName string) (*Producer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	return &Producer{
		conn:    conn,
		channel: ch,
		queue:   q,
	}, nil
}

func (p *Producer) Publish(event any) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return p.channel.PublishWithContext(
		ctx,
		"",
		p.queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now().UTC(),
		},
	)
}

func (p *Producer) Close() error {
	if p == nil {
		return nil
	}
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
