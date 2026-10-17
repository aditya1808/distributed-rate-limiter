package types

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type Algorithm string

const (
	AlgorithmTokenBucket   Algorithm = "token_bucket"
	AlgorithmSlidingWindow Algorithm = "sliding_window"
)

func (a Algorithm) Valid() bool {
	return a == AlgorithmTokenBucket || a == AlgorithmSlidingWindow
}

const (
	MinPeriod      = time.Second
	MaxPeriod      = 24 * time.Hour
	MinBurstWindow = 100 * time.Millisecond
	MaxLimit       = 1_000_000
	MaxCost        = 1_000
	MaxRuleName    = 128

	// MaxSlidingWindowLimit bounds memory: the sliding window log stores one
	// sorted-set entry per admitted request.
	MaxSlidingWindowLimit = 10_000
)

// IdentifierPattern restricts tenants and namespaces to characters that are
// safe inside Redis keys and hash tags.
var IdentifierPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

// Rule is a reusable quota definition. It applies to traffic only through
// assignments, which bind it to (tenant, namespace) pairs.
//
// Limit/Period is the sustained rate. Burst means:
//   - token bucket: bucket capacity (defaults to Limit)
//   - sliding window: max requests within BurstWindow (0 disables the burst cap)
type Rule struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Algorithm   Algorithm     `json:"algorithm"`
	Limit       int64         `json:"limit"`
	Period      time.Duration `json:"period"`
	Burst       int64         `json:"burst"`
	BurstWindow time.Duration `json:"burst_window"`
	FailOpen    bool          `json:"fail_open"`
	Version     int64         `json:"version"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

// Capacity is the largest cost a single request can ever be granted.
func (r Rule) Capacity() int64 {
	switch r.Algorithm {
	case AlgorithmTokenBucket:
		if r.Burst > 0 {
			return r.Burst
		}
		return r.Limit
	case AlgorithmSlidingWindow:
		if r.Burst > 0 && r.Burst < r.Limit {
			return r.Burst
		}
		return r.Limit
	}
	return 0
}

func (r Rule) Validate() error {
	var errs []FieldError
	add := func(field, msg string, args ...any) {
		errs = append(errs, FieldError{Field: field, Message: fmt.Sprintf(msg, args...)})
	}

	if strings.TrimSpace(r.Name) == "" || utf8.RuneCountInString(r.Name) > MaxRuleName {
		add("name", "must be 1 to %d characters and not blank", MaxRuleName)
	}
	if !r.Algorithm.Valid() {
		add("algorithm", "must be one of: %s, %s", AlgorithmTokenBucket, AlgorithmSlidingWindow)
	}
	if r.Limit < 1 || r.Limit > MaxLimit {
		add("limit", "must be between 1 and %d", MaxLimit)
	}
	if r.Period < MinPeriod || r.Period > MaxPeriod {
		add("period", "must be between %s and %s", MinPeriod, MaxPeriod)
	}
	if r.Period%time.Millisecond != 0 {
		add("period", "must be a whole number of milliseconds")
	}
	if r.Burst < 0 || r.Burst > MaxLimit {
		add("burst", "must be between 0 and %d", MaxLimit)
	}

	switch r.Algorithm {
	case AlgorithmTokenBucket:
		if r.BurstWindow != 0 {
			add("burst_window", "is only valid for %s", AlgorithmSlidingWindow)
		}
	case AlgorithmSlidingWindow:
		if r.Limit > MaxSlidingWindowLimit {
			add("limit", "must be at most %d for %s; use %s for higher limits",
				MaxSlidingWindowLimit, AlgorithmSlidingWindow, AlgorithmTokenBucket)
		}
		if r.Burst > 0 {
			if r.BurstWindow < MinBurstWindow || r.BurstWindow >= r.Period {
				add("burst_window", "must be at least %s and shorter than period when burst is set", MinBurstWindow)
			}
			if r.BurstWindow%time.Millisecond != 0 {
				add("burst_window", "must be a whole number of milliseconds")
			}
		} else if r.BurstWindow != 0 {
			add("burst_window", "requires burst to be set")
		}
	}

	if len(errs) > 0 {
		return NewValidationError(errs...)
	}
	return nil
}
