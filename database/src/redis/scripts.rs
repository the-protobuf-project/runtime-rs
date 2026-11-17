//! The writes, each one Lua script so the record, its index entry and its unique reservations
//! change together or not at all.
//!
//! Go claims each key with its own `SET NX` and rolls back by hand on a conflict, which its
//! own docs name as the gap: a process dying mid-create leaves a reservation with no record,
//! and the next create of that value fails naming a key that does not exist. They also name
//! the fix, a Lua script, which is what these are. The keys are Go's, so the layout is shared.
//!
//! Each script is sent as `EVALSHA`, falling back to `EVAL` on a cold server, and declares
//! every key it touches in `KEYS`. Replies are integers: [`DONE`], [`MISSING`], [`TAKEN`] or
//! [`MOVED`].

use std::sync::LazyLock;

use redis::Script;

/// The write happened.
pub(super) const DONE: i64 = 1;
/// The record is not there (update, delete), or already is (create).
pub(super) const MISSING: i64 = 0;
/// Another record holds a unique value.
pub(super) const TAKEN: i64 = 2;
/// The record changed since it was read; read it again and retry.
pub(super) const MOVED: i64 = 3;

/// Creates a record. KEYS: record, ids, then each unique reservation. ARGV: body, primary
/// key. A reservation already held by this key is accepted, as Go's `claimUnique` accepts it.
pub(super) static CREATE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
for i = 3, #KEYS do
  local owner = redis.call('GET', KEYS[i])
  if owner and owner ~= ARGV[2] then return 2 end
end
redis.call('SET', KEYS[1], ARGV[1])
for i = 3, #KEYS do redis.call('SET', KEYS[i], ARGV[2]) end
redis.call('ZADD', KEYS[2], 0, ARGV[2])
return 1
",
    )
});

/// Overwrites a record read as ARGV[1], moving its reservations. KEYS: record, then ARGV[4]
/// reservations to claim, then the stale ones to release. ARGV: the body read, the new body,
/// primary key, the claim count. A stale reservation is released only while this key holds
/// it, so a value another record has since claimed stays claimed.
pub(super) static UPDATE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
if current ~= ARGV[1] then return 3 end
local claims = tonumber(ARGV[4])
for i = 2, claims + 1 do
  local owner = redis.call('GET', KEYS[i])
  if owner and owner ~= ARGV[3] then return 2 end
end
for i = 2, claims + 1 do redis.call('SET', KEYS[i], ARGV[3]) end
for i = claims + 2, #KEYS do
  if redis.call('GET', KEYS[i]) == ARGV[3] then redis.call('DEL', KEYS[i]) end
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
",
    )
});

/// Deletes a record read as ARGV[1] and releases what it reserved. KEYS: record, ids, then
/// its reservations. ARGV: the body read, primary key.
pub(super) static DELETE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
if current ~= ARGV[1] then return 3 end
redis.call('DEL', KEYS[1])
for i = 3, #KEYS do
  if redis.call('GET', KEYS[i]) == ARGV[2] then redis.call('DEL', KEYS[i]) end
end
redis.call('ZREM', KEYS[2], ARGV[2])
return 1
",
    )
});
