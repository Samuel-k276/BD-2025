--ex1

SELECT
  --o least e o greatest sao usados para garantir sempre a mesma ordem alfabetica 
  LEAST(cidade_partida, cidade_chegada) AS cidade1, 
  GREATEST(cidade_partida, cidade_chegada) AS cidade2,
  SUM(passageiros_1c + passageiros_2c)::float / SUM(assentos_1c + assentos_2c) AS taxa_ocupacao
FROM estatisticas_voos
WHERE hora_partida >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY cidade1, cidade2
HAVING SUM(assentos_1c + assentos_2c) > 0
ORDER BY taxa_ocupacao DESC
LIMIT 1;

--ex2

SELECT
    LEAST(cidade_partida, cidade_chegada) AS cidade1,
    GREATEST(cidade_partida, cidade_chegada) AS cidade2
FROM estatisticas_voos ev1
WHERE hora_partida >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY
    LEAST(cidade_partida, cidade_chegada),
    GREATEST(cidade_partida, cidade_chegada)
HAVING COUNT(DISTINCT no_serie) = (
    SELECT COUNT(DISTINCT ev2.no_serie)
    FROM estatisticas_voos ev2
    WHERE ev2.hora_partida >= CURRENT_DATE - INTERVAL '3 months'
)


