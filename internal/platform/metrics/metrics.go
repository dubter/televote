package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/dubter/televote/internal/service/vote"
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
			Help: "Votes accepted at ingress (202 response).",
		}),
		votesRejected: f.NewCounterVec(prometheus.CounterOpts{
			Name: "televote_votes_rejected_total",
			Help: "Votes rejected, broken down by reason.",
		}, []string{"reason"}),
		votesCounted: f.NewCounterVec(prometheus.CounterOpts{
			Name: "televote_votes_counted_total",
			Help: "Votes applied by the consumer: counted or already_counted.",
		}, []string{"result"}),
		produceLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "televote_produce_duration_seconds",
			Help:    "Time to send a vote to Kafka — the whole client response budget.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1},
		}),
		applyLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "televote_apply_duration_seconds",
			Help:    "Time to apply a vote in Redis with a single EVALSHA.",
			Buckets: []float64{.0002, .0005, .001, .0025, .005, .01, .05, .25, 1},
		}),
		consumerLag: f.NewGauge(prometheus.GaugeOpts{
			Name: "televote_consumer_lag",
			Help: "Messages in Kafka not applied yet. Zero means the drain is over.",
		}),
		ballotsTotal: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "televote_ballots_total",
			Help: "Ballots in the latest result snapshot.",
		}, []string{"poll"}),
	}
}

func (m *Metrics) VoteAccepted() { m.votesAccepted.Inc() }

func (m *Metrics) VoteRejected(reason string) { m.votesRejected.WithLabelValues(reason).Inc() }

func (m *Metrics) VoteCounted(result vote.Result) {
	m.votesCounted.WithLabelValues(result.String()).Inc()
}

func (m *Metrics) ProduceSeconds(d float64) { m.produceLatency.Observe(d) }

func (m *Metrics) ApplySeconds(d float64) { m.applyLatency.Observe(d) }

func (m *Metrics) SetConsumerLag(n int64) { m.consumerLag.Set(float64(n)) }

func (m *Metrics) SetBallots(pollSlug string, n int64) {
	m.ballotsTotal.WithLabelValues(pollSlug).Set(float64(n))
}
