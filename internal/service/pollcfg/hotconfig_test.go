package pollcfg_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/pollcfg"
)

func TestHotConfig_ShardingMatchesPollRow(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("6f1c9f2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b")
	cfg := &pollcfg.HotConfig{ID: id, ShardCount: 1000}

	assert.Equal(t, domain.Sharding{PollID: id, ShardCount: 1000}, cfg.Sharding())
}
