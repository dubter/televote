package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/adapter/producer"
	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/pollcfg"
	"github.com/dubter/televote/internal/service/vote"
)

var (
	opensAt  = time.Date(2026, 9, 8, 20, 47, 30, 0, time.UTC)
	closesAt = opensAt.Add(time.Minute)
	testSalt = []byte("consumer-test-salt-0123456789abc")
)

type fakeApplier struct {
	mu       sync.Mutex
	seen     map[vote.VoterID]bool
	votes    map[uint8]int64
	ballots  int64
	calls    int
	failWith error
	failFor  int
}

func newFakeApplier() *fakeApplier {
	return &fakeApplier{seen: map[vote.VoterID]bool{}, votes: map[uint8]int64{}}
}

func (f *fakeApplier) Cast(_ context.Context, _ uuid.UUID, _ uint16, v vote.VoterID, choices []uint8) (vote.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if f.failFor > 0 {
		f.failFor--
		return 0, f.failWith
	}

	if f.seen[v] {
		return vote.ResultAlreadyCounted, nil
	}
	f.seen[v] = true
	for _, idx := range choices {
		f.votes[idx]++
	}
	f.ballots++
	return vote.ResultCounted, nil
}

func (f *fakeApplier) snapshot() (map[uint8]int64, int64, int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make(map[uint8]int64, len(f.votes))
	maps.Copy(out, f.votes)
	return out, f.ballots, f.calls
}

type fakeLookup map[uuid.UUID]*pollcfg.HotConfig

func (f fakeLookup) ByID(id uuid.UUID) (*pollcfg.HotConfig, bool) {
	cfg, ok := f[id]
	return cfg, ok
}

type recordingObserver struct {
	mu       sync.Mutex
	counted  []vote.Result
	rejected []string
}

func (o *recordingObserver) VoteCounted(r vote.Result) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counted = append(o.counted, r)
}

func (o *recordingObserver) VoteRejected(reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rejected = append(o.rejected, reason)
}

func (o *recordingObserver) ApplySeconds(float64) {}

func (o *recordingObserver) reasons() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, len(o.rejected))
	copy(out, o.rejected)
	return out
}

func testConfig(t *testing.T) *pollcfg.HotConfig {
	t.Helper()

	p := &domain.Poll{
		ID:         uuid.New(),
		Slug:       "final",
		Type:       domain.PollTypeSingle,
		Options:    []domain.Option{{Idx: 0}, {Idx: 1}, {Idx: 2}},
		Status:     domain.StatusOpen,
		OpensAt:    opensAt,
		ClosesAt:   closesAt,
		ShardCount: 500,
		Salt:       testSalt,
	}
	return &pollcfg.HotConfig{
		ID: p.ID, Slug: p.Slug, Rules: p.ChoiceRules(),
		Window: p.Window(), ShardCount: p.ShardCount, Salt: p.Salt,
	}
}

func newTestCounting(t *testing.T, applier Applier, cfg *pollcfg.HotConfig, obs Observer) *Counting {
	t.Helper()

	c := newCounting(applier, fakeLookup{cfg.ID: cfg}, obs,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config{RetryBudget: 2 * time.Second, LookupBudget: 300 * time.Millisecond})
	return c
}

func record(t *testing.T, cfg *pollcfg.HotConfig, clientID string, choices []uint8, at time.Time) *kgo.Record {
	t.Helper()

	v, err := vote.DeriveVoterID(cfg.Salt, clientID)
	require.NoError(t, err)

	payload, err := json.Marshal(producer.VoteMessage{
		PollID: cfg.ID, VoterID: v.Hex(), Choices: choices, ProducedAt: at,
	})
	require.NoError(t, err)
	return &kgo.Record{Value: payload}
}

func TestCounting_AppliesVote(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	applier := newFakeApplier()
	obs := &recordingObserver{}
	c := newTestCounting(t, applier, cfg, obs)

	c.applyRecord(context.Background(), record(t, cfg, "voter-1", []uint8{1}, opensAt.Add(time.Second)))

	votes, ballots, _ := applier.snapshot()
	assert.Equal(t, map[uint8]int64{1: 1}, votes)
	assert.EqualValues(t, 1, ballots)
	assert.Equal(t, []vote.Result{vote.ResultCounted}, obs.counted)
}

func TestCounting_DuplicateDeliveryDoesNotDoubleCount(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	applier := newFakeApplier()
	obs := &recordingObserver{}
	c := newTestCounting(t, applier, cfg, obs)

	rec := record(t, cfg, "voter-1", []uint8{2}, opensAt.Add(time.Second))
	for range 5 {
		c.applyRecord(context.Background(), rec)
	}

	votes, ballots, _ := applier.snapshot()
	assert.Equal(t, map[uint8]int64{2: 1}, votes, "счётчик вырос от повторной доставки")
	assert.EqualValues(t, 1, ballots)

	assert.Equal(t, vote.ResultCounted, obs.counted[0])
	for _, r := range obs.counted[1:] {
		assert.Equal(t, vote.ResultAlreadyCounted, r)
	}
}

func TestCounting_UsesProducedAtNotProcessingTime(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	applier := newFakeApplier()
	c := newTestCounting(t, applier, cfg, &recordingObserver{})

	c.applyRecord(context.Background(), record(t, cfg, "late", []uint8{0}, closesAt.Add(-time.Second)))

	_, ballots, _ := applier.snapshot()
	assert.EqualValues(t, 1, ballots, "голос из окна обязан быть засчитан на любом дренаже")
}

func TestCounting_RejectsVoteProducedOutsideWindow(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	applier := newFakeApplier()
	obs := &recordingObserver{}
	c := newTestCounting(t, applier, cfg, obs)

	c.applyRecord(context.Background(), record(t, cfg, "early", []uint8{0}, opensAt.Add(-time.Second)))
	c.applyRecord(context.Background(), record(t, cfg, "late", []uint8{0}, closesAt.Add(time.Second)))

	_, ballots, _ := applier.snapshot()
	assert.Zero(t, ballots)
	assert.Equal(t, []string{reasonOutOfWindow, reasonOutOfWindow}, obs.reasons())
}

func TestCounting_RejectsMalformedAndUnknown(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	applier := newFakeApplier()
	obs := &recordingObserver{}
	c := newTestCounting(t, applier, cfg, obs)

	c.applyRecord(context.Background(), &kgo.Record{Value: []byte("не json")})

	foreign, err := json.Marshal(producer.VoteMessage{
		PollID: uuid.New(), VoterID: "0123456789abcdef0123456789abcdef",
		Choices: []uint8{0}, ProducedAt: opensAt.Add(time.Second),
	})
	require.NoError(t, err)
	c.applyRecord(context.Background(), &kgo.Record{Value: foreign})

	badVoter, err := json.Marshal(producer.VoteMessage{
		PollID: cfg.ID, VoterID: "не-hex", Choices: []uint8{0}, ProducedAt: opensAt.Add(time.Second),
	})
	require.NoError(t, err)
	c.applyRecord(context.Background(), &kgo.Record{Value: badVoter})

	_, ballots, _ := applier.snapshot()
	assert.Zero(t, ballots)
	assert.Equal(t, []string{reasonMalformed, reasonUnknownPoll, reasonBadVoterID}, obs.reasons(),
		"набор причин конечен: они уходят в метку метрики")
}

func TestCounting_RetriesTransientRedisError(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	applier := newFakeApplier()
	applier.failWith = errors.New("connection reset by peer")
	applier.failFor = 3

	c := newTestCounting(t, applier, cfg, &recordingObserver{})
	c.applyRecord(context.Background(), record(t, cfg, "voter-1", []uint8{1}, opensAt.Add(time.Second)))

	_, ballots, calls := applier.snapshot()
	assert.EqualValues(t, 1, ballots, "голос обязан примениться после восстановления")
	assert.Equal(t, 4, calls, "три отказа и одно удачное применение")
}

func TestCounting_DoesNotRetryPermanentError(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	applier := newFakeApplier()
	applier.failWith = vote.ErrInvalidArgs
	applier.failFor = 100

	c := newTestCounting(t, applier, cfg, &recordingObserver{})

	start := time.Now()
	c.applyRecord(context.Background(), record(t, cfg, "voter-1", []uint8{1}, opensAt.Add(time.Second)))

	_, _, calls := applier.snapshot()
	assert.Equal(t, 1, calls, "постоянная ошибка не ретраится")
	assert.Less(t, time.Since(start), time.Second)
}

func TestCounting_GivesUpOnGenuinelyUnknownPoll(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	applier := newFakeApplier()
	obs := &recordingObserver{}

	c := newTestCounting(t, applier, cfg, obs)
	c.lookup = fakeLookup{} // опроса нет и не появится

	start := time.Now()
	c.applyRecord(context.Background(), record(t, cfg, "voter-1", []uint8{1}, opensAt.Add(time.Second)))

	assert.Equal(t, []string{reasonUnknownPoll}, obs.reasons())
	assert.Less(t, time.Since(start), 2*time.Second, "консьюмер завис на чужом сообщении")
}
