package feedback

import "time"

type FeedbackRequestBody struct {
	Tenant    string `json:"tenant" binding:"required,identifier"`
	Namespace string `json:"namespace" binding:"required,identifier"`
	// ErrorRate is the downstream error ratio in [0, 1].
	ErrorRate *float64 `json:"error_rate" binding:"required,gte=0,lte=1"`
	// TTLSeconds defaults to 300.
	TTLSeconds int `json:"ttl_seconds" binding:"omitempty,min=30,max=3600"`
}

type FeedbackResponseBody struct {
	Tenant             string     `json:"tenant"`
	Namespace          string     `json:"namespace"`
	AdaptiveMultiplier float64    `json:"adaptive_multiplier"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
}
