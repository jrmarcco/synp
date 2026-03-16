redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
return redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], "+inf")
