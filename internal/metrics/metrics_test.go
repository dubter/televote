package metrics_test

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/metrics"
	"github.com/dubter/televote/internal/vote"
)

func TestNFR7_MetricsExposeBusinessCounters(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.VoteAccepted()
	m.VoteAccepted()
	m.VoteRejected("invalid_choices")
	m.VoteCounted(context.Background(), vote.ResultCounted)
	m.VoteCounted(context.Background(), vote.ResultAlreadyCounted)
	m.SetConsumerLag(42)
	m.SetBallots("final", 1000)

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
	} {
		assert.True(t, names[want], "метрика %s не зарегистрирована", want)
	}
}

func TestNFR7_MetricLabelsAreBounded(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.VoteRejected("invalid_choices")
	m.VoteCounted(context.Background(), vote.ResultCounted)

	dump := render(t, reg)

	for _, forbidden := range []string{"voter", "ip=", "addr", "user_agent"} {
		assert.NotContains(t, dump, forbidden, "в метриках не должно быть лейбла %q", forbidden)
	}
	assert.Contains(t, dump, `value:"counted"`)
}

func render(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()

	var b strings.Builder

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		b.WriteString(f.String())
	}
	return b.String()
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
