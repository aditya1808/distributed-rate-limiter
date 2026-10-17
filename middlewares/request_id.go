package middlewares

import (
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/aditya1808/distributed-rate-limiter/constants"
)

var validRequestID = regexp.MustCompile(`^[a-zA-Z0-9._:-]{1,128}$`)

// RequestID propagates a caller-supplied X-Request-ID (if well-formed) or
// generates one, so every log line and error can be correlated.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(constants.HeaderRequestID)
		if !validRequestID.MatchString(id) {
			id = uuid.NewString()
		}
		c.Set(constants.ContextKeyRequestID, id)
		c.Header(constants.HeaderRequestID, id)
		c.Next()
	}
}

// InstanceID tags responses with the serving instance, which is how
// cross-instance behaviour is observed behind a load balancer.
func InstanceID(id string, expose bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if expose {
			c.Header(constants.HeaderInstanceID, id)
		}
		c.Next()
	}
}
