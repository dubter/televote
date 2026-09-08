package httpapi_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/httpapi"
)

func fetchPage(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()

	w := httptest.NewRecorder()
	httpapi.StaticRoutes("https://vote.example").ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestFR2_ShortLinkServesVotingPage(t *testing.T) {
	t.Parallel()

	w := fetchPage(t, "/p/final")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
	assert.Contains(t, w.Body.String(), "<noscript>", "страница обязана быть осмысленной без JS")
}

// 30 млн загрузок означают, что лишний килобайт — это 30 ГБ трафика за минуту,
// а CDN дороже всего вычислительного вместе взятого.
func TestVotePage_UnderEightKilobytesGzipped(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := io.Copy(zw, bytes.NewReader(fetchPage(t, "/p/final").Body.Bytes()))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	assert.Less(t, buf.Len(), 8*1024, "страница голосования раздулась: %d байт gzip", buf.Len())
}

// sessionStorage живёт до закрытия вкладки: F5 в той же вкладке прошёл бы,
// а новая вкладка дала бы второй голос. Разработчик, проверяющий обновлением
// страницы, этого не увидит — поэтому проверяет тест.
func TestVotePage_UsesLocalStorageNotSession(t *testing.T) {
	t.Parallel()

	body := fetchPage(t, "/p/final").Body.String()

	// Ищем обращение к API, а не упоминание: в комментарии рядом с кодом
	// имя sessionStorage стоит законно — оно объясняет, почему его тут нет.
	assert.Contains(t, body, "localStorage.getItem")
	assert.NotContains(t, body, "sessionStorage.",
		"идентификатор голосующего обязан переживать закрытие вкладки")
}

// Внешний ресурс — это лишний round-trip на мобильной сети и лишняя точка
// отказа ровно в ту минуту, когда всё решается.
func TestVotePage_HasNoExternalResources(t *testing.T) {
	t.Parallel()

	body := fetchPage(t, "/p/final").Body.String()

	for _, marker := range []string{"src=\"http", "href=\"http", "//cdn", "googleapis"} {
		assert.NotContains(t, body, marker, "страница тянет внешний ресурс: %s", marker)
	}
}

func TestFR2_QREncodesPublicVotingURL(t *testing.T) {
	t.Parallel()

	w := fetchPage(t, "/p/final/qr.png")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/png", w.Header().Get("Content-Type"))
	assert.True(t, bytes.HasPrefix(w.Body.Bytes(), []byte("\x89PNG")), "ответ не PNG")
	assert.NotEmpty(t, w.Header().Get("Cache-Control"), "QR неизменен и обязан кэшироваться")
}

func TestAdminPage_IsServedAndNotIndexed(t *testing.T) {
	t.Parallel()

	w := fetchPage(t, "/admin")

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "noindex"),
		"админка не должна попадать в поисковый индекс")
}
