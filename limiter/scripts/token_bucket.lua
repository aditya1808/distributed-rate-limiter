-- Token bucket. Runs atomically inside Redis.
--
-- KEYS[1] state hash {tokens, ts}
-- KEYS[2] adaptive multiplier (string, optional)
-- ARGV[1] capacity (burst)
-- ARGV[2] refill rate in tokens per millisecond
-- ARGV[3] cost
-- ARGV[4] "1" to peek (compute without writing)
--
-- Returns {allowed, remaining, limit, retry_after_ms, reset_after_ms, now_ms, multiplier, used}

local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local peek = ARGV[4] == "1"

local mult = tonumber(redis.call("GET", KEYS[2])) or 1
capacity = math.max(1, math.floor(capacity * mult))
rate = rate * mult

-- Redis is the single clock for every instance.
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  -- A missing key is a full bucket; the TTL below relies on this.
  tokens = capacity
  ts = now
end

-- Guard against the clock moving backwards after a failover.
if now > ts then
  tokens = tokens + (now - ts) * rate
end
if tokens > capacity then
  tokens = capacity
end
tokens = tonumber(string.format("%.6f", tokens))

local allowed = 0
if tokens + 1e-9 >= cost then
  allowed = 1
  if not peek then
    tokens = tokens - cost
  end
end
if tokens < 0 then
  tokens = 0
end

local retry_after = 0
if allowed == 0 then
  retry_after = math.ceil((cost - tokens) / rate)
end
local reset_after = math.ceil((capacity - tokens) / rate)

if not peek then
  redis.call("HSET", KEYS[1], "tokens", string.format("%.6f", tokens), "ts", now)
  -- Expire once the bucket would be full again: nothing is lost.
  redis.call("PEXPIRE", KEYS[1], reset_after + 1000)
end

local remaining = math.floor(tokens + 1e-9)
return {allowed, remaining, capacity, retry_after, reset_after, now, tostring(mult), capacity - remaining}
