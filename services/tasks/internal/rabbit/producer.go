package rabbit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
}

type TaskEvent struct {
	Event     string `json:"event"`
	TaskID    string `json:"task_id"`
	TS        string `json:"ts"`
	RequestID string `json:"request_id,omitempty"`
	Producer  string `json:"producer,omitempty"`
	Version   string `json:"version,omitempty"`
}

type QueueTopology struct {
	MainQueue string
	DLXName   string
	DLQName   string
}

func NewProducer(url string, topology QueueTopology) (*Producer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if err := declareTopology(ch, topology); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	return &Producer{
		conn:      conn,
		channel:   ch,
		queueName: topology.MainQueue,
	}, nil
}

func declareTopology(ch *amqp.Channel, topology QueueTopology) error {
	if topology.MainQueue == "" {
		return fmt.Errorf("main queue name is required")
	}

	args := amqp.Table{}
	if topology.DLXName != "" {
		if err := ch.ExchangeDeclare(
			topology.DLXName,
			"direct",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return err
		}

		args["x-dead-letter-exchange"] = topology.DLXName
		if topology.DLQName != "" {
			args["x-dead-letter-routing-key"] = topology.DLQName
		}
	}

	_, err := ch.QueueDeclare(
		topology.MainQueue,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return err
	}

	if topology.DLQName == "" {
		return nil
	}

	q, err := ch.QueueDeclare(
		topology.DLQName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	if topology.DLXName == "" {
		return nil
	}

	return ch.QueueBind(
		q.Name,
		q.Name,
		topology.DLXName,
		false,
		nil,
	)
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
		p.queueName,
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
