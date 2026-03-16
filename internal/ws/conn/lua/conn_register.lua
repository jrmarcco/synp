redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])

if redis.call("PEXPIRE", KEYS[1], ARGV[3]) == 1 then
	return 1
end

return 0
