// Package metrics определяет бизнес-метрики сервиса.
//
// Набор лейблов у каждой метрики конечен и не содержит пользовательских данных:
// slug, voterID или адрес в лейбле взорвали бы кардинальность и положили бы
// Prometheus быстрее, чем сам эфир.
package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/dubter/televote/internal/vote"
)

// Metrics — счётчики приёма, подсчёта и дренажа.
type Metrics struct {
	votesAccepted  prometheus.Counter
	votesRejected  *prometheus.CounterVec
	votesCounted   *prometheus.CounterVec
	produceLatency prometheus.Histogram
	applyLatency   prometheus.Histogram
	consumerLag    prometheus.Gauge
	ballotsTotal   *prometheus.GaugeVec
}

// New регистрирует метрики в переданном регистраторе.
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
			Name: "televote_produce_duration_seconds",
			Help: "Время отправки голоса в Kafka — весь бюджет ответа клиенту.",
			// Приём укладывается в доли миллисекунды; верхние корзины нужны,
			// чтобы увидеть деградацию, а не чтобы измерить норму.
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

// VoteAccepted — голос принят и отправлен в Kafka.
func (m *Metrics) VoteAccepted() { m.votesAccepted.Inc() }

// VoteRejected — голос отвергнут. Причина берётся из конечного набора.
func (m *Metrics) VoteRejected(reason string) { m.votesRejected.WithLabelValues(reason).Inc() }

// VoteCounted — консьюмер применил голос.
func (m *Metrics) VoteCounted(_ context.Context, result vote.Result) {
	m.votesCounted.WithLabelValues(result.String()).Inc()
}

// VoteRejectedCtx — вариант с контекстом для consumer.Observer.
func (m *Metrics) VoteRejectedCtx(_ context.Context, reason string) {
	m.VoteRejected(reason)
}

// ProduceSeconds — время отправки в Kafka.
func (m *Metrics) ProduceSeconds(d float64) { m.produceLatency.Observe(d) }

// ApplySeconds — время применения голоса в Redis.
func (m *Metrics) ApplySeconds(d float64) { m.applyLatency.Observe(d) }

// SetConsumerLag — текущий лаг. По нему KEDA скейлит консьюмеров, а
// снапшотер решает, что дренаж окончен.
func (m *Metrics) SetConsumerLag(n int64) { m.consumerLag.Set(float64(n)) }

// SetBallots — бюллетеней в последнем снимке. Лейбл — slug опроса: их единицы,
// кардинальность ограничена числом активных эфиров.
func (m *Metrics) SetBallots(pollSlug string, n int64) {
	m.ballotsTotal.WithLabelValues(pollSlug).Set(float64(n))
}
