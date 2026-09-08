package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/platform/metrics"
)

func TestNFR7_MetricsExposeBusinessCounters(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.VoteAccepted()
	m.VoteAccepted()
	m.VoteRejected("invalid_choices")
	m.VoteCounted("counted")
	m.VoteCounted("already_counted")
	m.SetConsumerLag(42)
	m.SetBallots("final", 1000)
	m.HTTPRequest("POST", "/api/v1/polls/{slug}/vote", "2xx", 0.003)
	m.SetBreakerOpen(true)
	m.SetConfigAge(1.5)

	families, err := reg.Gather()
	require.NoError(t, err)

	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	assertCount(t, families, "televote_votes_accepted_total", 2)

	for _, want := range []string{
		"televote_votes_accepted_total",
		"televote_votes_rejected_total",
		"televote_votes_counted_total",
		"televote_produce_duration_seconds",
		"televote_consumer_lag",
		"televote_ballots_total",
		"televote_http_requests_total",
		"televote_redis_breaker_open",
		"televote_poll_config_age_seconds",
	} {
		assert.True(t, names[want], "метрика %s не зарегистрирована", want)
	}
}

func assertCount(t *testing.T, families []*dto.MetricFamily, name string, want float64) {
	t.Helper()

	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		var total float64
		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
		assert.InDelta(t, want, total, 1e-9)
		return
	}
	t.Fatalf("метрика %s не найдена", name)
}
