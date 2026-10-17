package types

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validRule() Rule {
	return Rule{Algorithm: AlgorithmTokenBucket, Limit: 300, Period: time.Minute, Burst: 20}
}

func fieldsOf(t *testing.T, err error) []string {
	t.Helper()
	var verr *ValidationError
	require.True(t, errors.As(err, &verr), "expected ValidationError, got %v", err)
	var out []string
	for _, f := range verr.Fields {
		out = append(out, f.Field)
	}
	return out
}

func TestRuleValidate(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(*Rule)
		wantFields []string
	}{
		{"valid token bucket", func(*Rule) {}, nil},
		{"valid sliding window with burst", func(r *Rule) {
			r.Algorithm, r.Limit, r.Burst, r.BurstWindow = AlgorithmSlidingWindow, 1000, 10, time.Second
		}, nil},
		{"unknown algorithm", func(r *Rule) { r.Algorithm = "leaky" }, []string{"algorithm"}},
		{"zero limit", func(r *Rule) { r.Limit = 0 }, []string{"limit"}},
		{"period too short", func(r *Rule) { r.Period = 500 * time.Millisecond }, []string{"period"}},
		{"period too long", func(r *Rule) { r.Period = 48 * time.Hour }, []string{"period"}},
		{"negative burst", func(r *Rule) { r.Burst = -1 }, []string{"burst"}},
		{"burst window on token bucket", func(r *Rule) { r.BurstWindow = time.Second }, []string{"burst_window"}},
		{"sliding window limit above cap", func(r *Rule) {
			r.Algorithm, r.Burst, r.Limit = AlgorithmSlidingWindow, 0, MaxSlidingWindowLimit+1
		}, []string{"limit"}},
		{"sliding window burst without window", func(r *Rule) {
			r.Algorithm, r.Burst, r.BurstWindow = AlgorithmSlidingWindow, 5, 0
		}, []string{"burst_window"}},
		{"sliding window burst window not shorter than period", func(r *Rule) {
			r.Algorithm, r.Burst, r.BurstWindow = AlgorithmSlidingWindow, 5, time.Minute
		}, []string{"burst_window"}},
		{"sliding window window without burst", func(r *Rule) {
			r.Algorithm, r.Burst, r.BurstWindow = AlgorithmSlidingWindow, 0, time.Second
		}, []string{"burst_window"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := validRule()
			tc.mutate(&r)
			err := r.Validate()
			if tc.wantFields == nil {
				assert.NoError(t, err)
				return
			}
			assert.Equal(t, tc.wantFields, fieldsOf(t, err))
		})
	}
}

func TestRuleCapacity(t *testing.T) {
	assert.EqualValues(t, 20, Rule{Algorithm: AlgorithmTokenBucket, Limit: 300, Burst: 20}.Capacity())
	assert.EqualValues(t, 300, Rule{Algorithm: AlgorithmTokenBucket, Limit: 300}.Capacity(), "burst defaults to limit")
	assert.EqualValues(t, 10, Rule{Algorithm: AlgorithmSlidingWindow, Limit: 100, Burst: 10}.Capacity())
	assert.EqualValues(t, 100, Rule{Algorithm: AlgorithmSlidingWindow, Limit: 100}.Capacity())
	assert.EqualValues(t, 5, Rule{Algorithm: AlgorithmSlidingWindow, Limit: 5, Burst: 10}.Capacity(), "burst above limit cannot raise capacity")
}

func TestAssignmentValidate(t *testing.T) {
	valid := Assignment{Tenant: "acme", Namespace: "orders", RuleID: "r_1"}
	assert.NoError(t, valid.Validate())

	def := valid
	def.Tenant = "*"
	assert.NoError(t, def.Validate(), "* is the namespace default")

	cases := []struct {
		name   string
		mutate func(*Assignment)
		want   []string
	}{
		{"tenant with key separator", func(a *Assignment) { a.Tenant = "ac:me" }, []string{"tenant"}},
		{"tenant with hash tag brace", func(a *Assignment) { a.Tenant = "{acme}" }, []string{"tenant"}},
		{"empty namespace", func(a *Assignment) { a.Namespace = "" }, []string{"namespace"}},
		{"wildcard namespace", func(a *Assignment) { a.Namespace = "*" }, []string{"namespace"}},
		{"missing rule", func(a *Assignment) { a.RuleID = "" }, []string{"rule_id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := valid
			tc.mutate(&a)
			assert.Equal(t, tc.want, fieldsOf(t, a.Validate()))
		})
	}
}
