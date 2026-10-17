package core_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func TestNew_WarmsRedisPoolBeforeReturning(t *testing.T) {
	app := testutils.NewTestApp(t, testutils.WithMinIdleConns(12))

	stats := app.App.Redis.PoolStats()
	assert.GreaterOrEqual(t, stats.TotalConns, uint32(12), "pool is dialed before the app can serve traffic")
	assert.GreaterOrEqual(t, stats.IdleConns, uint32(10), "warm connections are returned to the pool as idle")
}
