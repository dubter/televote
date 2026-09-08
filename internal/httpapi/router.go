package httpapi

import (
	"net/http"
	"net/netip"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	"github.com/rs/cors"
)

// RouterConfig — то, что роутеру нужно снаружи.
type RouterConfig struct {
	// TrustedProxies — сети, чьему X-Forwarded-For можно верить.
	TrustedProxies []netip.Prefix
	// DatacenterRanges — датацентровые диапазоны: зритель ТВ оттуда не голосует.
	DatacenterRanges []netip.Prefix
	// VoteRateLimit — частота на ключ лимита за окно.
	VoteRateLimit int
	RateWindow    time.Duration
	// AllowedOrigins — источники для CORS. Пустой список означает same-origin.
	AllowedOrigins []string
	ServiceName    string
}

// NewRouter собирает публичный и админский маршруты в один сервер.
//
// Порядок middleware важен: сначала выясняем адрес клиента, потом лимитируем и
// фильтруем по нему. Обратный порядок лимитировал бы по адресу прокси, то есть
// по одному ключу на весь трафик.
func NewRouter(public *PublicHandler, admin *AdminHandler, static http.Handler, cfg RouterConfig) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RealIP)
	r.Use(otelchi.Middleware(cfg.ServiceName, otelchi.WithChiRoutes(r)))
	r.Use(Recovery(nil))
	r.Use(SecurityHeaders)
	r.Use(ClientIP(cfg.TrustedProxies))

	if len(cfg.AllowedOrigins) > 0 {
		r.Use(cors.New(cors.Options{
			AllowedOrigins: cfg.AllowedOrigins,
			AllowedMethods: []string{http.MethodGet, http.MethodPost},
			AllowedHeaders: []string{"Content-Type", "Authorization"},
			MaxAge:         300,
		}).Handler)
	}

	// Публичный приём. Лимит и ASN-фильтр только здесь: админку защищает
	// аутентификация, а голосующий анонимен.
	r.Route("/api/v1", func(api chi.Router) {
		api.Group(func(vote chi.Router) {
			vote.Use(BlockDatacenterASN(cfg.DatacenterRanges))
			vote.Use(RateLimit(cfg.VoteRateLimit, cfg.RateWindow))
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
