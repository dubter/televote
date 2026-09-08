package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type VoteMessage struct {
	PollID     uuid.UUID `json:"p"`
	VoterID    string    `json:"v"`
	Choices    []uint8   `json:"c"`
	Net16      string    `json:"n"`
	UAClass    string    `json:"u"`
	ProducedAt time.Time `json:"t"`
}

type Config struct {
	Brokers        []string
	Topic          string
	Linger         time.Duration
	ProduceTimeout time.Duration
	Hooks          []kgo.Hook
}

var (
	ErrNoBrokers = errors.New("producer: no brokers configured")
	ErrNoTopic   = errors.New("producer: topic is not set")
)

const (
	defaultLinger         = 5 * time.Millisecond
	defaultProduceTimeout = 2 * time.Second
)

type Producer struct {
	client  *kgo.Client
	topic   string
	timeout time.Duration
}

func New(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrNoBrokers
	}
	if cfg.Topic == "" {
		return nil, ErrNoTopic
	}
	if cfg.Linger <= 0 {
		cfg.Linger = defaultLinger
	}
	if cfg.ProduceTimeout <= 0 {
		cfg.ProduceTimeout = defaultProduceTimeout
	}

	client, err := kgo.NewClient(
		kgo.WithHooks(cfg.Hooks...),
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(cfg.Linger),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("producer: connect to kafka: %w", err)
	}

	return &Producer{client: client, topic: cfg.Topic, timeout: cfg.ProduceTimeout}, nil
}

func (p *Producer) Send(ctx context.Context, m VoteMessage) error {
	if m.ProducedAt.IsZero() {
		m.ProducedAt = time.Now().UTC()
	}

	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("producer: marshal vote: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(m.VoterID),
		Value: payload,
	}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("producer: send vote: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	if err := p.client.Flush(ctx); err != nil {
		p.client.Close()
		return fmt.Errorf("producer: flush batches: %w", err)
	}
	p.client.Close()
	return nil
}
