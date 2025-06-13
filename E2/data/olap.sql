--ex1
SELECT cidade1, cidade2, taxa_ocupacao
FROM (
  SELECT
    LEAST(cidade_partida, cidade_chegada) AS cidade1,
    GREATEST(cidade_partida, cidade_chegada) AS cidade2,
    AVG((passageiros_1c + passageiros_2c)::float / (assentos_1c + assentos_2c)) AS taxa_ocupacao,
    RANK() OVER (ORDER BY AVG((passageiros_1c + passageiros_2c)::float / (assentos_1c + assentos_2c)) DESC) AS ranking
  FROM estatisticas_voos
  WHERE hora_partida >= CURRENT_DATE - INTERVAL '1 year'
  GROUP BY 
    LEAST(cidade_partida, cidade_chegada),
    GREATEST(cidade_partida, cidade_chegada)
) AS rotas_ranking
WHERE ranking = 1;

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
  (pais_partida, pais_chegada), (cidade_partida, cidade_chegada),
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


--ex4
SELECT
  pais_partida,
  cidade_partida,
  pais_chegada,
  cidade_chegada,
  dia_da_semana,
  SUM(passageiros_1c) AS total_1c,
  SUM(passageiros_2c) AS total_2c,
  ROUND(
    SUM(passageiros_1c)::numeric / NULLIF(SUM(passageiros_2c), 0),
    2
  ) AS ratio_1c_2c

FROM estatisticas_voos

GROUP BY
  ROLLUP((pais_partida, pais_chegada), (cidade_partida, cidade_chegada)),
  dia_da_semana

ORDER BY
  pais_partida NULLS LAST,
  cidade_partida NULLS LAST,
  pais_chegada NULLS LAST,
  cidade_chegada NULLS LAST,
  POSITION(dia_da_semana IN 'Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday');