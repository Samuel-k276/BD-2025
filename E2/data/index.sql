
DROP INDEX IF EXISTS idx_estatisticas_rollup_ordenado;
DROP INDEX IF EXISTS idx_hora_partida; 

CREATE INDEX idx_hora_partida 
ON estatisticas_voos (hora_partida);

CREATE INDEX idx_estatisticas_rollup_ordenado
ON estatisticas_voos (
    pais_partida,
    pais_chegada,
    cidade_partida,
    cidade_chegada,
    ano,
    mes,
    dia_do_mes, 
    vendas_1c, 
    vendas_2c
);

--Justificação teórica:
--Para otimizar o desempenho da vista materializada "estatisticas_voos", foram criados dois índices:

-- 1. "idx_hora_partida": Este índice melhora a eficiência das consultas que filtram ou ordenam por 
--"hora_partida", uma coluna frequentemente utilizada, nomeadamente na consulta 5.2 e 5.3. No caso,
-- se a base de dados tivesse sido inicializada de maneira diferente, seria possível ver também uma
-- maior eficiência na consulta 5.1. Esta B-tree index, que é o tipo padrão de para colunas do tipo "timestamp",
--e funciona bem para consultas que envolvem intervalos de tempo.

--2. "idx_estatisticas_rollup_ordenado": índice composto que é útil para acelerar as operações de agregação (SUMS)
-- e agrupamento com ROLLUP, como na consulta 5.3. Para além disto, facilita o ORDER BY porque as colunas ficam ordenadas 
-- no índice, evitando operações adicionais de ordenação.
