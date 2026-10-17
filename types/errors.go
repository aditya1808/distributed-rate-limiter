package types

import (
	"errors"
	"strings"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrConflict         = errors.New("conflict")
	ErrVersionMismatch  = errors.New("version mismatch")
	ErrNoRule           = errors.New("no rule matches tenant and namespace")
	ErrStoreUnavailable = errors.New("state store unavailable")

	// ErrUnsupportedSchema means a stored record was written by a newer build.
	ErrUnsupportedSchema = errors.New("unsupported schema version")
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
