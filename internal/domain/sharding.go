package domain

import "github.com/google/uuid"

type Sharding struct {
	PollID     uuid.UUID
	ShardCount uint16
}

func (s Sharding) Valid() bool {
	return s.PollID != uuid.Nil && s.ShardCount > 0
}

func (p *Poll) Sharding() Sharding {
	return Sharding{PollID: p.ID, ShardCount: p.ShardCount}
}
