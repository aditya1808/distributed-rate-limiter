package middlewares

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/controllers"
)

// Recovery turns a panic into a 500 problem so one bad request cannot take
// the instance down.
func Recovery(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("[RECOVERY] panic",
					"panic", rec,
					"request_id", c.GetString(constants.ContextKeyRequestID),
					"stack", string(debug.Stack()))
				controllers.WriteProblem(c, http.StatusInternalServerError, "internal", "Internal server error", "", nil)
			}
		}()
		c.Next()
	}
}
