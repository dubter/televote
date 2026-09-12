package httpapi

//go:generate mockgen -source=capacity.go -destination=mocks/capacity.go -package=mocks

import (
	"context"
	"net/http"

	"github.com/dubter/televote/internal/service/capacity"
)

type CapacityAdvisor interface {
	Advise(ctx context.Context) (capacity.Advice, error)
}

func capacityHandler(advisor CapacityAdvisor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		advice, err := advisor.Advise(r.Context())
		if err != nil {
			WriteError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, advice)
	}
}
