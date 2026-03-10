redis.call("SADD", KEYS[1], ARGV[1])

if redis.call("PEXPIRE", KEYS[1], ARGV[2]) == 1 then
	return "ok"
end

return "failed"
