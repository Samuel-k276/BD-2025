
DROP INDEX IF EXISTS idx_estatisticas_rollup_ordenado; 

CREATE INDEX idx_estatisticas_rollup_ordenado
ON estatisticas_voos (
    pais_partida,
    pais_chegada,
    cidade_partida,
    cidade_chegada,
    ano,
    mes,
    dia_do_mes, vendas_1c, vendas_2c
);