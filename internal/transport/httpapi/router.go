package httpapi

import (
	"log/slog"
	"net/http"
	"net/http/pprof" //nolint:gosec // G108: pprof is served by DebugRouter on its own listener, DefaultServeMux is not exposed
	"net/netip"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riandyrn/otelchi"
	"github.com/rs/cors"

	"github.com/dubter/televote/internal/platform/health"
	"github.com/dubter/televote/internal/platform/httpx"
	"github.com/dubter/televote/internal/service/auth"
)

type RouterConfig struct {
	TrustedProxies  []netip.Prefix
	VoteRateLimit   int
	AdminRateLimit  int
	Logger          *slog.Logger
	RequestObserver httpx.RequestObserver
	RateWindow      time.Duration
	AllowedOrigins  []string
	ServiceName     string
}

func APIRouter(probes *health.Probes, public *PublicHandler, admin *AdminHandler, pages Pages, cfg RouterConfig) http.Handler {
	r := operational(probes)

	r.Group(func(app chi.Router) {
		app.Use(otelchi.Middleware(cfg.ServiceName, otelchi.WithChiRoutes(r)))
		app.Use(httpx.Recovery(cfg.Logger))
		app.Use(httpx.Metrics(cfg.RequestObserver))
		app.Use(httpx.SecurityHeaders)
		app.Use(httpx.ClientIP(cfg.TrustedProxies))

		if len(cfg.AllowedOrigins) > 0 {
			app.Use(cors.New(cors.Options{
				AllowedOrigins: cfg.AllowedOrigins,
				AllowedMethods: []string{http.MethodGet, http.MethodPost},
				AllowedHeaders: []string{"Content-Type", "Authorization"},
				MaxAge:         300,
			}).Handler)
		}

		app.Route("/api/v1", func(api chi.Router) {
			api.Group(func(vote chi.Router) {
				vote.Use(httpx.RateLimit(cfg.VoteRateLimit, cfg.RateWindow))
				vote.Mount("/", PublicRoutes(public))
			})

			api.Group(func(protected chi.Router) {
				protected.Use(httpx.RateLimit(cfg.AdminRateLimit, cfg.RateWindow))
				protected.Mount("/admin", AdminRoutes(admin))
			})
		})

		app.Mount("/", PageRoutes(pages))
	})

	return r
}

func PublicRoutes(h *PublicHandler) chi.Router {
	r := chi.NewRouter()
	r.Get("/time", h.serverTime)
	r.Get("/polls/{slug}", h.pollConfig)
	r.Post("/polls/{slug}/vote", h.castVote)
	return r
}

func AdminRoutes(h *AdminHandler) chi.Router {
	r := chi.NewRouter()
	r.Post("/login", h.login)

	r.Group(func(viewer chi.Router) {
		viewer.Use(h.requireRole(auth.RoleViewer))
		viewer.Get("/polls", h.listPolls)
		viewer.Get("/polls/{slug}/results", h.pollResults)
	})

	r.Group(func(editor chi.Router) {
		editor.Use(h.requireRole(auth.RoleEditor))
		editor.Post("/polls", h.createPoll)
		editor.Post("/polls/{slug}/open", h.openPoll)
		editor.Post("/polls/{slug}/close", h.closePoll)
	})
	return r
}

func PageRoutes(p Pages) chi.Router {
	r := chi.NewRouter()
	r.Get("/p/{slug}", p.vote)
	r.Get("/p/{slug}/qr.png", p.qr)
	r.Get("/admin", p.admin)
	r.Get("/favicon.ico", noFavicon)
	return r
}

func ConsumerRouter(probes *health.Probes) http.Handler {
	return operational(probes)
}

func SnapshotRouter(probes *health.Probes, advisor CapacityAdvisor) http.Handler {
	r := operational(probes)
	r.Get("/internal/capacity", capacityHandler(advisor))
	return r
}

func DebugRouter() http.Handler {
	r := chi.NewRouter()
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return r
}

func operational(probes *health.Probes) chi.Router {
	r := chi.NewRouter()
	r.HandleFunc("/livez", probes.Live)
	r.HandleFunc("/readyz", probes.Ready)
	r.Handle("/metrics", promhttp.Handler())
	return r
}
