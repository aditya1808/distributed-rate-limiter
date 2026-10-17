package redisstore_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence/redisstore"
	"github.com/aditya1808/distributed-rate-limiter/testutils"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

func newRawStore(t *testing.T) (*redisstore.RuleStore, redis.UniversalClient, constants.Keyspace) {
	t.Helper()
	rdb := testutils.RedisClient(t)
	keys := constants.NewKeyspace(testutils.UniquePrefix(t, rdb))
	return redisstore.NewRuleStore(rdb, keys), rdb, keys
}

// rewriteRecord stores the rule's JSON with schema_version replaced
// (or removed when version < 0), simulating another build's writes.
func rewriteRecord(t *testing.T, rdb redis.UniversalClient, keys constants.Keyspace, id string, version int) {
	t.Helper()
	ctx := context.Background()
	raw, err := rdb.HGet(ctx, keys.RulesData(), id).Bytes()
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	if version < 0 {
		delete(doc, "schema_version")
	} else {
		doc["schema_version"] = version
	}
	data, _ := json.Marshal(doc)
	require.NoError(t, rdb.HSet(ctx, keys.RulesData(), id, data).Err())
}

func TestRuleStore_WritesSchemaVersion(t *testing.T) {
	s, rdb, keys := newRawStore(t)
	require.NoError(t, s.Create(context.Background(), rule("r1", "acme", 1)))

	raw, err := rdb.HGet(context.Background(), keys.RulesData(), "r1").Bytes()
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	assert.EqualValues(t, redisstore.RuleSchemaVersion, doc["schema_version"])
	assert.Equal(t, "acme", doc["tenant"], "rule fields stay top-level so the Lua scripts can read them")
}

func TestRuleStore_ReadsLegacyRecordWithoutSchemaVersion(t *testing.T) {
	s, rdb, keys := newRawStore(t)
	ctx := context.Background()
	require.NoError(t, s.Create(ctx, rule("r1", "acme", 1)))
	rewriteRecord(t, rdb, keys, "r1", -1)

	got, err := s.Get(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, "acme", got.Tenant)

	updated := rule("r1", "acme", 2)
	require.NoError(t, s.Update(ctx, updated, 1), "legacy records can be updated, which stamps the current version")
}

func TestRuleStore_RefusesNewerSchema(t *testing.T) {
	s, rdb, keys := newRawStore(t)
	ctx := context.Background()
	require.NoError(t, s.Create(ctx, rule("r1", "acme", 1)))
	rewriteRecord(t, rdb, keys, "r1", redisstore.RuleSchemaVersion+1)

	_, err := s.Get(ctx, "r1")
	assert.ErrorIs(t, err, types.ErrUnsupportedSchema)

	_, err = s.List(ctx)
	assert.ErrorIs(t, err, types.ErrUnsupportedSchema, "list fails so the cache keeps its last good snapshot")

	assert.ErrorIs(t, s.Update(ctx, rule("r1", "acme", 2), 1), types.ErrUnsupportedSchema,
		"an older build must never overwrite a newer record")

	raw, _ := rdb.HGet(ctx, keys.RulesData(), "r1").Bytes()
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	assert.EqualValues(t, redisstore.RuleSchemaVersion+1, doc["schema_version"], "record left untouched")

	require.NoError(t, s.Delete(ctx, "r1"), "delete only needs the stable fields and still works")
}
