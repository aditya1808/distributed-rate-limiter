package constants

import "fmt"

const (
	DefaultKeyPrefix = "rl"

	// DefaultTenant is the tenant selector for a namespace-wide default rule.
	DefaultTenant = "*"

	HeaderRequestID  = "X-Request-ID"
	HeaderInstanceID = "X-Instance-ID"

	ContextKeyRequestID = "request_id"
	ContextKeyTenant    = "tenant"
)

// Keyspace builds every Redis key the service uses. Keys touched by the same
// Lua script share a hash tag ({...}) so the service stays Redis Cluster safe.
type Keyspace struct {
	prefix string
}

func NewKeyspace(prefix string) Keyspace {
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	return Keyspace{prefix: prefix}
}

func (k Keyspace) Prefix() string { return k.prefix }

func (k Keyspace) RulesData() string    { return k.prefix + ":{rules}:data" }
func (k Keyspace) RulesIndex() string   { return k.prefix + ":{rules}:index" }
func (k Keyspace) RulesChannel() string { return k.prefix + ":rules:changed" }
func (k Keyspace) Activity() string     { return k.prefix + ":activity" }

// State is versioned by rule so that editing a rule never reinterprets state
// written under the previous settings or algorithm.
func (k Keyspace) State(tenant, namespace, ruleID string, version int64) string {
	return fmt.Sprintf("%s:{%s}:%s:v%d", k.prefix, PairKey(tenant, namespace), ruleID, version)
}

func (k Keyspace) Seq(stateKey string) string { return stateKey + ":seq" }

func (k Keyspace) Adaptive(tenant, namespace string) string {
	return fmt.Sprintf("%s:{%s}:adaptive", k.prefix, PairKey(tenant, namespace))
}

func PairKey(tenant, namespace string) string { return tenant + "|" + namespace }
