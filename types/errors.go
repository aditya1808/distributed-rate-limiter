package types

import (
	"errors"
	"strings"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrVersionMismatch  = errors.New("version mismatch")
	ErrNoRule           = errors.New("no rule matches tenant and namespace")
	ErrStoreUnavailable = errors.New("state store unavailable")

	// ErrRuleInUse means a rule cannot be deleted while assignments use it.
	ErrRuleInUse = errors.New("rule is assigned")
	// ErrUnknownRule means an assignment names a rule that does not exist.
	ErrUnknownRule = errors.New("rule does not exist")
)

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for _, f := range e.Fields {
		parts = append(parts, f.Field+": "+f.Message)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func NewValidationError(fields ...FieldError) *ValidationError {
	return &ValidationError{Fields: fields}
}
