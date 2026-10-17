package dashboard

import (
	"time"

	"github.com/aditya1808/distributed-rate-limiter/schemas/quota"
)

type ActiveTenantsQuery struct {
	Limit int `form:"limit" binding:"omitempty,min=1,max=50"`
}

type ActiveTenant struct {
	Tenant    string                   `json:"tenant"`
	Namespace string                   `json:"namespace"`
	LastSeen  time.Time                `json:"last_seen"`
	Quota     *quota.QuotaResponseBody `json:"quota,omitempty"`
}

type ActiveTenantsResponseBody struct {
	Items []ActiveTenant `json:"items"`
	Count int            `json:"count"`
}
