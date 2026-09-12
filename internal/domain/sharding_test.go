package domain_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dubter/televote/internal/domain"
)

func TestPoll_ShardingCarriesIDAndShardCountTogether(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("6f1c9f2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b")
	p := &domain.Poll{ID: id, ShardCount: 512}

	got := p.Sharding()

	assert.Equal(t, domain.Sharding{PollID: id, ShardCount: 512}, got)
}

func TestSharding_ValidRequiresAtLeastOneShard(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("6f1c9f2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b")

	assert.True(t, domain.Sharding{PollID: id, ShardCount: 1}.Valid())
	assert.False(t, domain.Sharding{PollID: id, ShardCount: 0}.Valid(), "ноль шардов — ключи некуда класть")
	assert.False(t, domain.Sharding{ShardCount: 8}.Valid(), "пустой PollID — ключи не принадлежат опросу")
}
