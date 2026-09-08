if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) == false then
  return 2
end

for i = 2, #ARGV do
  redis.call('HINCRBY', KEYS[2], ARGV[i], 1)
end
redis.call('HINCRBY', KEYS[2], 'b', 1)

return 1
