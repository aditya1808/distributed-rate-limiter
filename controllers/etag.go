package controllers

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

// SetETag exposes a resource version as a strong ETag, e.g. "3".
func SetETag(c *gin.Context, version int64) {
	c.Header("ETag", strconv.Quote(strconv.FormatInt(version, 10)))
}

// IfMatch returns the version a client expects from its If-Match header. It
// writes 428 when the header is missing and 400 when it is not a version
// ETag, and then returns false.
func IfMatch(c *gin.Context) (int64, bool) {
	v := c.GetHeader("If-Match")
	if v == "" {
		WriteProblem(c, http.StatusPreconditionRequired, "precondition-required", "If-Match header required",
			`send If-Match with the resource's current ETag, e.g. If-Match: "3"`, nil)
		return 0, false
	}
	v = strings.Trim(strings.TrimPrefix(strings.TrimSpace(v), "W/"), `"`)
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		Validation(c, []types.FieldError{{Field: "If-Match", Message: `must be an ETag such as "3"`}})
		return 0, false
	}
	return n, true
}
