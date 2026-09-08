package httpapi

import (
	"embed"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	qrcode "github.com/skip2/go-qrcode"
)

//go:embed web/vote.html web/admin.html
var webFS embed.FS

func StaticRoutes(publicBaseURL string) chi.Router {
	r := chi.NewRouter()

	votePage := mustRead("web/vote.html")
	adminPage := mustRead("web/admin.html")

	r.Get("/p/{slug}", servePage(votePage))
	r.Get("/admin", servePage(adminPage))
	r.Get("/p/{slug}/qr.png", qrHandler(publicBaseURL))
	return r
}

func mustRead(name string) []byte {
	data, err := webFS.ReadFile(name)
	if err != nil {
		panic(fmt.Sprintf("httpapi: встроенный файл %s: %v", name, err))
	}
	return data
}

func servePage(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=300")
		_, _ = w.Write(body) //nolint:errcheck // обрыв соединения на отдаче страницы — событие клиента
	}
}

func qrHandler(baseURL string) http.HandlerFunc {
	base := strings.TrimSuffix(baseURL, "/")

	return func(w http.ResponseWriter, r *http.Request) {
		slug := chi.URLParam(r, "slug")
		png, err := qrcode.Encode(base+"/p/"+slug, qrcode.Medium, 512)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(png) //nolint:errcheck,gosec // PNG сгенерирован нами, XSS невозможен
	}
}
