redis.call("SADD", KEYS[1], ARGV[1])

if redis.call("PEXPIRE", KEYS[1], ARGV[2]) == 0 then
	"ok"
end

return ""
