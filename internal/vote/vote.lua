-- Применение одного голоса: дедуп и инкремент в одной атомарной операции.
--
-- KEYS[1] = v:{p:<pollID>:s<shard>}:<voterID>   маркер дедупа
-- KEYS[2] = c:{p:<pollID>:s<shard>}             хэш счётчиков
-- ARGV[1] = ttl в секундах
-- ARGV[2..] = индексы выбранных опций
--
-- Возврат: 1 — голос учтён, 2 — этот голосующий уже учтён.
--
-- Оба ключа обязаны делить hash tag, иначе Redis Cluster отвечает CROSSSLOT.
-- Дедуп-маркер здесь же служит ключом идемпотентности: Kafka доставляет
-- at-least-once, и без этого свойства каждый ребаланс группы завышал бы счёт.
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) == false then
  return 2
end

for i = 2, #ARGV do
  redis.call('HINCRBY', KEYS[2], ARGV[i], 1)
end
redis.call('HINCRBY', KEYS[2], 'b', 1)

return 1
