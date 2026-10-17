-- Sliding window log with an optional burst sub-window. Runs atomically inside Redis.
--
-- KEYS[1] sorted set of admitted requests (score = ms timestamp)
-- KEYS[2] adaptive multiplier (string, optional)
-- KEYS[3] sequence counter that keeps sorted-set members unique
-- ARGV[1] sustained limit
-- ARGV[2] window in ms
-- ARGV[3] burst limit (0 = no burst cap)
-- ARGV[4] burst window in ms
-- ARGV[5] cost
-- ARGV[6] "1" to peek (compute without writing)
--
-- Returns {allowed, remaining, limit, retry_after_ms, reset_after_ms, now_ms, multiplier, used}

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local burst_window = tonumber(ARGV[4])
local cost = tonumber(ARGV[5])
local peek = ARGV[6] == "1"

local mult = tonumber(redis.call("GET", KEYS[2])) or 1
limit = math.max(1, math.floor(limit * mult))
if burst > 0 then
  burst = math.max(1, math.floor(burst * mult))
end

local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window_start = now - window

if not peek then
  redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", window_start)
end

-- In peek mode expired entries are still present, so ranks are offset by them.
local expired = redis.call("ZCOUNT", KEYS[1], "-inf", window_start)
local used = redis.call("ZCOUNT", KEYS[1], "(" .. window_start, "+inf")
local burst_used = 0
if burst > 0 then
  burst_used = redis.call("ZCOUNT", KEYS[1], "(" .. (now - burst_window), "+inf")
end

local allowed = 0
if used + cost <= limit and (burst == 0 or burst_used + cost <= burst) then
  allowed = 1
end

local function score_at(rank)
  local entry = redis.call("ZRANGE", KEYS[1], rank, rank, "WITHSCORES")
  if entry[2] == nil then
    return nil
  end
  return tonumber(entry[2])
end

local retry_after = 0
if allowed == 0 then
  if used + cost > limit then
    -- The (used + cost - limit)-th oldest live entry must leave the window.
    local s = score_at(expired + used + cost - limit - 1)
    if s then
      retry_after = math.max(retry_after, s + window - now)
    else
      retry_after = math.max(retry_after, window)
    end
  end
  if burst > 0 and burst_used + cost > burst then
    local first_in_burst = expired + used - burst_used
    local s = score_at(first_in_burst + burst_used + cost - burst - 1)
    if s then
      retry_after = math.max(retry_after, s + burst_window - now)
    else
      retry_after = math.max(retry_after, burst_window)
    end
  end
end

if allowed == 1 and not peek then
  for _ = 1, cost do
    local seq = redis.call("INCR", KEYS[3])
    redis.call("ZADD", KEYS[1], now, now .. ":" .. seq)
  end
  used = used + cost
  burst_used = burst_used + cost
  redis.call("PEXPIRE", KEYS[1], window)
  redis.call("PEXPIRE", KEYS[3], window)
end

local remaining = limit - used
if burst > 0 and burst - burst_used < remaining then
  remaining = burst - burst_used
end
if remaining < 0 then
  remaining = 0
end

local reset_after = 0
local newest = redis.call("ZRANGE", KEYS[1], -1, -1, "WITHSCORES")
if newest[2] ~= nil then
  reset_after = math.max(0, tonumber(newest[2]) + window - now)
end

return {allowed, remaining, limit, retry_after, reset_after, now, tostring(mult), used}
