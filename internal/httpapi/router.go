package httpapi

import (
	"net/http"
	"net/netip"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/riandyrn/otelchi"
	"github.com/rs/cors"

	"github.com/dubter/televote/pkg/httpx"
)

type RouterConfig struct {
	TrustedProxies   []netip.Prefix
	DatacenterRanges []netip.Prefix
	VoteRateLimit    int
	RateWindow       time.Duration
	AllowedOrigins   []string
	ServiceName      string
}

func NewRouter(public *PublicHandler, admin *AdminHandler, static http.Handler, cfg RouterConfig) http.Handler {
	r := chi.NewRouter()

	r.Use(otelchi.Middleware(cfg.ServiceName, otelchi.WithChiRoutes(r)))
	r.Use(httpx.Recovery(nil))
	r.Use(httpx.SecurityHeaders)
	r.Use(httpx.ClientIP(cfg.TrustedProxies))

	if len(cfg.AllowedOrigins) > 0 {
		r.Use(cors.New(cors.Options{
			AllowedOrigins: cfg.AllowedOrigins,
			AllowedMethods: []string{http.MethodGet, http.MethodPost},
			AllowedHeaders: []string{"Content-Type", "Authorization"},
			MaxAge:         300,
		}).Handler)
	}

	r.Route("/api/v1", func(api chi.Router) {
		api.Group(func(vote chi.Router) {
			vote.Use(httpx.BlockDatacenterASN(cfg.DatacenterRanges))
			vote.Use(httpx.RateLimit(cfg.VoteRateLimit, cfg.RateWindow))
			vote.Mount("/", public.Routes())
		})

		if admin != nil {
			api.Mount("/admin", admin.Routes())
		}
	})

	if static != nil {
		r.Mount("/", static)
	}
	return r
}
