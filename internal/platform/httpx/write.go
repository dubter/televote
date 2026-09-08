package httpx

import (
	"encoding/json"
	"net/http"
)

func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	//nolint:errcheck,errchkjson // status and headers are already on the wire, nothing left to report
	_ = json.NewEncoder(w).Encode(body)
}

func WriteBytes(w http.ResponseWriter, contentType, cacheControl string, body []byte) {
	w.Header().Set("Content-Type", contentType)
	if cacheControl != "" {
		w.Header().Set("Cache-Control", cacheControl)
	}
	//nolint:errcheck // a dropped connection mid-body is the client's business
	_, _ = w.Write(body)
}
