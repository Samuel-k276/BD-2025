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

--ex3

SELECT
  pais_partida,
  cidade_partida,
  pais_chegada,
  cidade_chegada,
  ano,
  mes,
  dia_do_mes,
  SUM(vendas_1c) AS total_vendas_1c,
  SUM(vendas_2c) AS total_vendas_2c,
  SUM(vendas_1c + vendas_2c) AS total_vendas
FROM estatisticas_voos
GROUP BY ROLLUP(
  pais_partida, cidade_partida,
  pais_chegada, cidade_chegada,
  ano, mes, dia_do_mes
)
ORDER BY
  pais_partida NULLS LAST,
  cidade_partida NULLS LAST,
  pais_chegada NULLS LAST,
  cidade_chegada NULLS LAST,
  ano NULLS LAST,
  mes NULLS LAST,
  dia_do_mes NULLS LAST;

