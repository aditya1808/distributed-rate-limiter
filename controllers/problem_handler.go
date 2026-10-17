package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

const ProblemContentType = "application/problem+json"

// Problem is an RFC 7807 error body. Every error response uses it.
type Problem struct {
	Type     string             `json:"type"`
	Title    string             `json:"title"`
	Status   int                `json:"status"`
	Detail   string             `json:"detail,omitempty"`
	Instance string             `json:"instance,omitempty"`
	Errors   []types.FieldError `json:"errors,omitempty"`
}

func WriteProblem(c *gin.Context, status int, slug, title, detail string, fields []types.FieldError) {
	p := Problem{
		Type:     "urn:ratelimiter:problem:" + slug,
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: c.GetString(constants.ContextKeyRequestID),
		Errors:   fields,
	}
	body, _ := json.Marshal(p)
	c.Data(status, ProblemContentType, body)
	c.Abort()
}

func Validation(c *gin.Context, fields []types.FieldError) {
	WriteProblem(c, http.StatusBadRequest, "validation", "Validation failed",
		fmt.Sprintf("%d invalid field(s)", len(fields)), fields)
}

// BindError turns request binding failures into a 400 (or 413) problem.
func BindError(c *gin.Context, err error) {
	var (
		verrs     validator.ValidationErrors
		syntaxErr *json.SyntaxError
		typeErr   *json.UnmarshalTypeError
		maxErr    *http.MaxBytesError
	)
	switch {
	case errors.As(err, &verrs):
		fields := make([]types.FieldError, 0, len(verrs))
		for _, fe := range verrs {
			fields = append(fields, types.FieldError{Field: fe.Field(), Message: fieldMessage(fe)})
		}
		Validation(c, fields)
	case errors.As(err, &maxErr):
		WriteProblem(c, http.StatusRequestEntityTooLarge, "payload-too-large", "Request body too large", "", nil)
	case errors.As(err, &typeErr):
		Validation(c, []types.FieldError{{Field: typeErr.Field, Message: "must be of type " + typeErr.Type.String()}})
	case errors.As(err, &syntaxErr), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		WriteProblem(c, http.StatusBadRequest, "malformed-body", "Malformed request body", "body must be a valid JSON object", nil)
	default:
		WriteProblem(c, http.StatusBadRequest, "bad-request", "Bad request", err.Error(), nil)
	}
}

// HandleError maps domain errors to HTTP problems.
func HandleError(c *gin.Context, err error) {
	var verr *types.ValidationError
	switch {
	case errors.As(err, &verr):
		Validation(c, verr.Fields)
	case errors.Is(err, types.ErrNotFound):
		WriteProblem(c, http.StatusNotFound, "not-found", "Resource not found", "", nil)
	case errors.Is(err, types.ErrNoRule):
		WriteProblem(c, http.StatusNotFound, "no-rule", "No rule matches", err.Error(), nil)
	case errors.Is(err, types.ErrRuleInUse):
		WriteProblem(c, http.StatusConflict, "rule-in-use", "Rule is assigned",
			"delete or reassign every assignment that uses this rule first", nil)
	case errors.Is(err, types.ErrAssignmentExists):
		WriteProblem(c, http.StatusConflict, "assignment-exists", "Assignment exists",
			"this tenant and namespace already have an assignment; update it instead", nil)
	case errors.Is(err, types.ErrUnknownRule):
		Validation(c, []types.FieldError{{Field: "rule_id", Message: "no rule with this id"}})
	case errors.Is(err, types.ErrVersionMismatch):
		WriteProblem(c, http.StatusPreconditionFailed, "version-mismatch", "Version mismatch",
			"the resource was modified; fetch it again and retry with the new ETag", nil)
	case errors.Is(err, types.ErrStoreUnavailable):
		WriteProblem(c, http.StatusServiceUnavailable, "store-unavailable", "State store unavailable", "", nil)
	case errors.Is(err, context.Canceled):
		WriteProblem(c, http.StatusServiceUnavailable, "cancelled", "Request cancelled", "", nil)
	case errors.Is(err, context.DeadlineExceeded):
		WriteProblem(c, http.StatusServiceUnavailable, "timeout", "Request timed out", "", nil)
	default:
		slog.Error("[CONTROLLER] unhandled error", "error", err, "request_id", c.GetString(constants.ContextKeyRequestID))
		WriteProblem(c, http.StatusInternalServerError, "internal", "Internal server error", "", nil)
	}
}

func fieldMessage(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "is required"
	case "identifier":
		return "must match " + types.IdentifierPattern.String()
	case "tenant_selector":
		return `must be "*" or match ` + types.IdentifierPattern.String()
	case "oneof":
		return "must be one of: " + fe.Param()
	case "min", "gte":
		return "must be at least " + fe.Param()
	case "max", "lte":
		return "must be at most " + fe.Param()
	}
	return "is invalid (" + fe.Tag() + ")"
}
