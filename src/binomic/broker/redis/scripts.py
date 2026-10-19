_REPLY_ABOUT = """\
local function reply_ok(result)
    return cjson.encode({
        ok = true,
        result = result,
        error = cjson.null,
    })
end

local function reply_error(error)
    return cjson.encode({
        ok = false,
        result = cjson.null,
        error = error,
    })
end
"""

_KEYS_GEN = """\
local function stream_key(namespace, queue)
    return namespace .. ":" .. queue .. ":stream"
end

local function hash_key(namespace, queue)
    return namespace .. ":" .. queue .. ":hash"
end

local function dlq_stream_key(namespace, queue)
    return namespace .. ":" .. queue .. ":dlq:stream"
end

local function dlq_hash_key(namespace, queue)
    return namespace .. ":" .. queue .. ":dlq:hash"
end
"""

_COMMON = _REPLY_ABOUT + _KEYS_GEN

_ENQUEUE = f"""\
{_COMMON}

local namespace = ARGV[1]
local queue = ARGV[2]
local message_json = ARGV[3]
local enqueued_at = tonumber(ARGV[4])

if not enqueued_at then
    return reply_error("Invalid `enqueued_at` value")
end

local ok, message = pcall(cjson.decode, message_json)
if not ok or type(message) ~= "table" or not message.id then
    return reply_error("Invalid JSON message or missing `message.id`")
end

local streamkey = stream_key(namespace, queue)
local hk = hash_key(namespace, queue)

local res = redis.pcall("XADD", streamkey, "*", "id", tostring(message.id))
if type(res) == "table" and res.err then
    return reply_error("Failed to add message to stream: " .. res.err)
end

local msid = tostring(res)
local entry = {{
    id = msid,
    queue = queue,
    message = message,
    enqueued_at = enqueued_at,
}}
local stored = redis.call(
    "HSETNX", hk,
    msid, cjson.encode(entry)
)

if stored == 0 then
    redis.call("XDEL", streamkey, msid)
    return reply_error("Failed to store message in hash")
end

return reply_ok(entry)
"""

_ACQUIRE = f"""\
{_COMMON}

local namespace = ARGV[1]
local consumer = ARGV[2]
local count = tonumber(ARGV[3])

if not count or count < 1 then
    return reply_error("Invalid `count` value")
end

local stream_keys = {{}}
local queue_by_stream = {{}}
for i = 4, #ARGV do
    local sk = stream_key(namespace, ARGV[i])
    stream_keys[#stream_keys + 1] = sk
    queue_by_stream[sk] = ARGV[i]
end

if #stream_keys == 0 then
    return reply_ok(cjson.empty_array)
end

local args = {{
    "XREADGROUP", "GROUP", namespace, consumer,
    "COUNT", tostring(count), "STREAMS",
}}
for _, sk in ipairs(stream_keys) do
    args[#args + 1] = sk
end
for _ = 1, #stream_keys do
    args[#args + 1] = ">"
end

local result = redis.pcall(unpack(args))

if type(result) == "table" and result.err then
    if string.find(tostring(result.err), "NOGROUP") then
        return reply_ok(cjson.empty_array)
    end
    return reply_error("Failed to read messages from streams: " .. result.err)
end

if not result then
    return reply_ok(cjson.empty_array)
end

local items = {{}}
for _, stream_data in ipairs(result) do
    local sk = stream_data[1]
    local entries = stream_data[2]
    local queue = queue_by_stream[sk]

    if queue and entries then
        local hk = hash_key(namespace, queue)
        for _, entry in ipairs(entries) do
            local msid = entry[1]
            local body = redis.call("HGET", hk, msid)
            if body then
                local ok, decoded = pcall(cjson.decode, body)
                if ok and type(decoded) == "table" then
                    items[#items + 1] = {{
                        id = msid,
                        queue = queue,
                        message = decoded.message,
                        enqueued_at = decoded.enqueued_at,
                    }}
                else
                    redis.call("XACK", sk, namespace, msid)
                    redis.call("XDEL", sk, msid)
                    redis.call("HDEL", hk, msid)
                end
            else
                redis.call("XACK", sk, namespace, msid)
                redis.call("XDEL", sk, msid)
            end
        end
    end
end

if #items == 0 then
    return reply_ok(cjson.empty_array)
end

return reply_ok(items)
"""

_ACK = f"""\
{_COMMON}

local namespace = ARGV[1]
local queue = ARGV[2]
local entry_id = ARGV[3]

local hk = hash_key(namespace, queue)
local sk = stream_key(namespace, queue)

redis.call("HDEL", hk, entry_id)
local acked = redis.call("XACK", sk, namespace, entry_id)
redis.call("XDEL", sk, entry_id)

return reply_ok(acked)
"""

_DEAD = f"""\
{_COMMON}

local namespace = ARGV[1]
local queue = ARGV[2]
local entry_id = ARGV[3]

local hk = hash_key(namespace, queue)
local sk = stream_key(namespace, queue)
local dlq_sk = dlq_stream_key(namespace, queue)
local dlq_hk = dlq_hash_key(namespace, queue)

local body = redis.call("HGET", hk, entry_id)
if not body then
    redis.call("XACK", sk, namespace, entry_id)
    redis.call("XDEL", sk, entry_id)
    redis.call("HDEL", hk, entry_id)
    return reply_ok(0)
end

local ok, decoded = pcall(cjson.decode, body)
if not ok or type(decoded) ~= "table"
    or type(decoded.message) ~= "table" or not decoded.message.id then
    redis.call("XACK", sk, namespace, entry_id)
    redis.call("XDEL", sk, entry_id)
    redis.call("HDEL", hk, entry_id)
    return reply_ok(0)
end

local res = redis.pcall(
    "XADD", dlq_sk,
    "*", "id", tostring(decoded.message.id)
)

if type(res) == "table" and res.err then
    return reply_error("Failed to move message to DLQ: " .. res.err)
end

local new_entry_id = tostring(res)
decoded.id = new_entry_id

local set_ok = redis.call(
    "HSETNX", dlq_hk,
    new_entry_id, cjson.encode(decoded)
)

redis.call("XACK", sk, namespace, entry_id)
redis.call("XDEL", sk, entry_id)
redis.call("HDEL", hk, entry_id)

return reply_ok(tonumber(set_ok))
"""

_RETRY = f"""\
{_COMMON}

local namespace = ARGV[1]
local queue = ARGV[2]
local entry_id = ARGV[3]
local enqueued_at = tonumber(ARGV[4])

if not enqueued_at then
    return reply_error("Invalid `enqueued_at` value")
end

local hk = hash_key(namespace, queue)
local sk = stream_key(namespace, queue)

local body = redis.call("HGET", hk, entry_id)
if not body then
    redis.call("XACK", sk, namespace, entry_id)
    redis.call("XDEL", sk, entry_id)
    return reply_ok(0)
end

local ok, decoded = pcall(cjson.decode, body)
if not ok or type(decoded) ~= "table"
    or type(decoded.message) ~= "table" or not decoded.message.id then
    redis.call("HDEL", hk, entry_id)
    redis.call("XACK", sk, namespace, entry_id)
    redis.call("XDEL", sk, entry_id)
    return reply_ok(0)
end

decoded.message.attempt = (decoded.message.attempt or 0) + 1
decoded.enqueued_at = enqueued_at

local res = redis.pcall(
    "XADD", sk, "*",
    "id", tostring(decoded.message.id)
)
if type(res) == "table" and res.err then
    return reply_error("Failed to re-enqueue message to stream: " .. res.err)
end

local new_ms_id = tostring(res)
decoded.id = new_ms_id

local set_ok = redis.call(
    "HSETNX", hk,
    new_ms_id, cjson.encode(decoded)
)

redis.call("HDEL", hk, entry_id)
redis.call("XACK", sk, namespace, entry_id)
redis.call("XDEL", sk, entry_id)

return reply_ok(tonumber(set_ok))
"""

_RECLAIM = f"""\
{_COMMON}

local namespace = ARGV[1]
local consumer = ARGV[2]
local min_idle_ms = tonumber(ARGV[3])
local count = tonumber(ARGV[4])
local enqueued_at = tonumber(ARGV[5])
local gc_idle_ms = tonumber(ARGV[6])

if not min_idle_ms or not count or count < 1 or not enqueued_at
    or not gc_idle_ms then
    return reply_error("Invalid arguments")
end

local total_reclaimed = 0
local total_dropped = 0

for i = 7, #ARGV do
    local queue = ARGV[i]
    local hk = hash_key(namespace, queue)
    local sk = stream_key(namespace, queue)

    local cursor = "0-0"
    local max_steps = 20
    while max_steps > 0 do
        max_steps = max_steps - 1

        local result = redis.pcall(
            "XAUTOCLAIM", sk, namespace, consumer,
            min_idle_ms, cursor, "COUNT", count
        )

        if type(result) == "table" and result.err then
            if string.find(tostring(result.err), "NOGROUP") then
                break
            end
            return reply_error("Failed to reclaim messages from streams: " .. result.err)
        end

        local next_cursor = result[1]
        local entries = result[2] or {{}}

        for _, entry in ipairs(entries) do
            local msid = entry[1]
            local body = redis.call("HGET", hk, msid)
            if not body then
                redis.call("XACK", sk, namespace, msid)
                redis.call("XDEL", sk, msid)
                total_dropped = total_dropped + 1
            else
                local ok, decoded = pcall(cjson.decode, body)
                if not ok or type(decoded) ~= "table"
                    or type(decoded.message) ~= "table" then
                    redis.call("HDEL", hk, msid)
                    redis.call("XACK", sk, namespace, msid)
                    redis.call("XDEL", sk, msid)
                    total_dropped = total_dropped + 1
                else
                    local msg_id = decoded.message.id
                    if not msg_id then
                        redis.call("HDEL", hk, msid)
                        redis.call("XACK", sk, namespace, msid)
                        redis.call("XDEL", sk, msid)
                        total_dropped = total_dropped + 1
                    else
                        decoded.message.attempt = (decoded.message.attempt or 0) + 1

                        local new_ms_id = redis.call(
                            "XADD", sk, "*",
                            "id", tostring(decoded.message.id)
                        )
                        decoded.id = new_ms_id
                        decoded.enqueued_at = enqueued_at
                        redis.call(
                            "HSETNX", hk, new_ms_id,
                            cjson.encode(decoded)
                        )

                        redis.call("HDEL", hk, msid)
                        redis.call("XACK", sk, namespace, msid)
                        redis.call("XDEL", sk, msid)

                        total_reclaimed = total_reclaimed + 1
                    end
                end
            end
        end

        if next_cursor == "0-0" then
            break
        end
        cursor = next_cursor
    end

    -- Consumer metadata hygiene: instances come and go, and Redis never GCs
    -- consumer group members. Drop consumers whose PEL is empty and whose
    -- idle time exceeds gc_idle_ms. Pending entries make a consumer
    -- untouchable: DELCONSUMER destroys the PEL and silently orphans those
    -- messages past any claim. XINFO sits behind pcall so environments
    -- without it (test doubles) degrade to a no-op sweep.
    local consumers = redis.pcall("XINFO", "CONSUMERS", sk, namespace)
    if type(consumers) == "table" and not consumers.err then
        for _, item in ipairs(consumers) do
            local name, pending, idle
            if type(item) == "table" then
                for j = 1, #item, 2 do
                    local field = item[j]
                    if field == "name" then
                        name = item[j + 1]
                    elseif field == "pending" then
                        pending = tonumber(item[j + 1])
                    elseif field == "idle" then
                        idle = tonumber(item[j + 1])
                    end
                end
            end

            if name and pending == 0 and idle and idle > gc_idle_ms then
                redis.call("XGROUP", "DELCONSUMER", sk, namespace, name)
            end
        end
    end
end

return reply_ok(total_reclaimed)
"""
