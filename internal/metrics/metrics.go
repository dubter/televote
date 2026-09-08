package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/dubter/televote/internal/vote"
)

type Metrics struct {
	votesAccepted  prometheus.Counter
	votesRejected  *prometheus.CounterVec
	votesCounted   *prometheus.CounterVec
	produceLatency prometheus.Histogram
	applyLatency   prometheus.Histogram
	consumerLag    prometheus.Gauge
	ballotsTotal   *prometheus.GaugeVec
}

func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)

	return &Metrics{
		votesAccepted: f.NewCounter(prometheus.CounterOpts{
			Name: "televote_votes_accepted_total",
			Help: "Голосов принято на входе (ответ 202).",
		}),
		votesRejected: f.NewCounterVec(prometheus.CounterOpts{
			Name: "televote_votes_rejected_total",
			Help: "Голосов отвергнуто с разбивкой по причине.",
		}, []string{"reason"}),
		votesCounted: f.NewCounterVec(prometheus.CounterOpts{
			Name: "televote_votes_counted_total",
			Help: "Голосов применено консьюмером: counted или already_counted.",
		}, []string{"result"}),
		produceLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "televote_produce_duration_seconds",
			Help:    "Время отправки голоса в Kafka — весь бюджет ответа клиенту.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1},
		}),
		applyLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "televote_apply_duration_seconds",
			Help:    "Время применения голоса в Redis одним EVALSHA.",
			Buckets: []float64{.0002, .0005, .001, .0025, .005, .01, .05, .25, 1},
		}),
		consumerLag: f.NewGauge(prometheus.GaugeOpts{
			Name: "televote_consumer_lag",
			Help: "Сообщений в Kafka, ещё не применённых. Ноль означает конец дренажа.",
		}),
		ballotsTotal: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "televote_ballots_total",
			Help: "Бюллетеней в последнем снимке результата.",
		}, []string{"poll"}),
	}
}

func (m *Metrics) VoteAccepted() { m.votesAccepted.Inc() }

func (m *Metrics) VoteRejected(reason string) { m.votesRejected.WithLabelValues(reason).Inc() }

func (m *Metrics) VoteCounted(_ context.Context, result vote.Result) {
	m.votesCounted.WithLabelValues(result.String()).Inc()
}

func (m *Metrics) VoteRejectedCtx(_ context.Context, reason string) {
	m.VoteRejected(reason)
}

func (m *Metrics) ProduceSeconds(d float64) { m.produceLatency.Observe(d) }

func (m *Metrics) ApplySeconds(d float64) { m.applyLatency.Observe(d) }

func (m *Metrics) SetConsumerLag(n int64) { m.consumerLag.Set(float64(n)) }

func (m *Metrics) SetBallots(pollSlug string, n int64) {
	m.ballotsTotal.WithLabelValues(pollSlug).Set(float64(n))
}
