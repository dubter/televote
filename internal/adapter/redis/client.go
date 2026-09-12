package redis

//go:generate mockgen -destination=mocks/redis.go -package=mocks github.com/redis/rueidis Client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

var ErrNoAddresses = errors.New("redis: no cluster addresses configured")

const (
	defaultDialTimeout = 2 * time.Second
	keepAlive          = 30 * time.Second
)

type Config struct {
	Addrs       []string
	DialTimeout time.Duration
}

type Client struct {
	raw rueidis.Client
}

func Open(_ context.Context, cfg Config) (*Client, error) {
	addrs := make([]string, 0, len(cfg.Addrs))
	for _, a := range cfg.Addrs {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		return nil, ErrNoAddresses
	}

	dial := cfg.DialTimeout
	if dial <= 0 {
		dial = defaultDialTimeout
	}

	raw, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  addrs,
		Dialer:       net.Dialer{Timeout: dial, KeepAlive: keepAlive},
		ShuffleInit:  true,
		DisableCache: true,
	})
	if err != nil {
		return nil, fmt.Errorf("redis: connect to cluster: %w", err)
	}
	return &Client{raw: raw}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.raw.Do(ctx, c.raw.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("redis: ping: %w", err)
	}
	return nil
}

func (c *Client) Close() {
	c.raw.Close()
}
