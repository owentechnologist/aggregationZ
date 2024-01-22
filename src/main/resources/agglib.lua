#!lua name=agglib
-- this function library executes using a 5 min lookback and returns
-- the count of instances of entries in the key within that window
-- as well as the sum of the values stored in the key within that window
-- it expects a SortedSet as the key to be used for this result set
-- Save it to your file system and then load it into redis using:
-- cat /path/to/agglib.lua | redis-cli -x FUNCTION LOAD REPLACE
-- The function that is expected to be called is
-- update_and_get_count_and_sum_for_key which has an alias registered of agg_result
-- example: Here we pass in the name of the interesting SortedSet key 'z1' and the value to add to the Set:
-- the results are for the last 5 minutes of activity as seen by that SortedSet key
-- FCALL agg_result 1 z1 12.87
-- 1) (integer) 4
-- 2) (integer) 800

local function is_not_sorted_set(key_name)
  local errorVal = nil
  if redis.call('TYPE', key_name)['ok'] ~= 'zset' then
    errorVal = 'The key ' .. key_name .. ' is not a SortedSet.'
    redis.call('publish','function_log','errorVal: '..errorVal)
  end
  redis.call('publish','function_log','errorVal: is nil')
  return errorVal
end

-- in order to allow duplicate numeric values we need to add some thing unique
-- a SortedSet does not allow duplicate entries as it thinks its keeping score not timestamping
local function uniquify_value(value,throwaway_suffix)
  return value..':'..throwaway_suffix
end


local function get_time_for_score()
  local t = redis.call('time')
  local tSec = t[1]
  local tMic = t[2]
  local ft = (tonumber(tSec..string.format('%06d', tMic)))
  return ft
end

local function get_sum_for_window(key_name,loop_count,window_start,window_end)
  local sum = 0.0
  local values = redis.call('zrange',key_name,window_start,window_end,'byscore')
  local numeric_val = 0.0
  for sumup=1,loop_count do
    local tstring = values[sumup]
    redis.call('publish','function_log','tstring: '..tstring..' numeric_val: '..numeric_val..' loop_count: '..loop_count)
    local dsot = string.find(tstring, ':')

    redis.call('publish','function_log','debug line 44 ...  dsot: '..dsot)
    numeric_val = string.sub(tstring,1,dsot-1)
    redis.call('publish','function_log','tstring: '..tstring..' numeric_val: '..numeric_val)
    sum = sum+tonumber(numeric_val)
  end
  return sum
end
local function get_count_for_window(key_name,window_start,window_end)
  return redis.call('zcount',key_name,window_start,window_end)
end
-- this function accepts the name of the interesting SortedSet
-- it also accepts the value to be stored now
-- example invocation: FCALL update_and_get_count_and_sum_for_key 1 z1 12.87
local function update_and_get_count_and_sum_for_key(keys,args)
  local sortedSet = keys[1]
  local newVal = args[1]
  local fiveMin = 300000000
  local now = get_time_for_score()
  local lookBack = now-fiveMin
  local exists = redis.call('exists',sortedSet)
  if exists==1 then
    redis.call('publish','function_log','Key '..sortedSet..' exists in redis')
    local type_error = is_not_sorted_set(sortedSet)
    if type_error ~= nil then
      return redis.error_reply(type_error)
    end
  end
  redis.call('ZADD',sortedSet,now,uniquify_value(newVal,now))
  local loop_count = get_count_for_window(sortedSet,lookBack,now)
  redis.call('publish','function_log','now: '..now..' lookBack: '..lookBack..' loop_count: '..loop_count)
  return {loop_count,get_sum_for_window(sortedSet,loop_count,lookBack,now)}
end
redis.register_function('agg_result', update_and_get_count_and_sum_for_key)