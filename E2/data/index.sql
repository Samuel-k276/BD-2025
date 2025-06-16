
DROP INDEX IF EXISTS idx_hora_partida; 
DROP INDEX IF EXISTS idx_estatisticas_rollup_ordenado;
DROP INDEX IF EXISTS idx_estatisticas_dia_semana_rollup;

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
    dia_do_mes
) INCLUDE (
    vendas_1c, 
    vendas_2c
);

CREATE INDEX idx_estatisticas_dia_semana_rollup
ON estatisticas_voos (
    dia_da_semana,
    pais_partida,
    pais_chegada,
    cidade_partida,
    cidade_chegada
) INCLUDE (
    passageiros_1c,
    passageiros_2c
);

-- Justificação teórica:
-- Para otimizar o desempenho da vista materializada "estatisticas_voos", foram criados tres índices:

-- 1. "idx_hora_partida": Este índice melhora a eficiência das consultas que filtram ou ordenam por 
-- "hora_partida", uma coluna frequentemente utilizada, nomeadamente na consulta 5.1 e 5.2. 
-- Esta B-tree index, que é o tipo padrão de para colunas do tipo "timestamp",
-- e funciona bem para consultas que envolvem intervalos de tempo.q
-- Antes de criar este índice, as consultas 5.1 e 5.2 tinham custo de 6469 e 14089, respectivamente 
-- e demoravam respetivamente 27ms e 50ms. Depois de criar o índice, o custo das mesmas consultas
-- foi reduzido para 5160 e 10039, com tempos de execução de 13ms e 19ms, mostrando uma melhoria significativa, na ordem dos 60%.

-- 2. "idx_estatisticas_rollup_ordenado": índice composto que é útil para acelerar as operações de agregação (SUMS)
-- e agrupamento com ROLLUP, como na consulta 5.3. Para além disto, facilita o ORDER BY porque as colunas ficam ordenadas
-- no índice, evitando operações adicionais de ordenação. Antes de criar este índice, a consulta 5.3
-- tinha um custo de 43919 e demorava 1558ms. Depois de criar o índice, o custo da consulta foi reduzido para 25343,
-- com um tempo de execução de 918ms, mostrando uma melhoria significativa, na ordem dos 40%, especialmente devido ao
-- evitar por completo a ordenação dos resultados.

-- 3. "idx_estatisticas_dia_semana_rollup": índice composto que melhora o desempenho de consultas que agregam dados
-- por dia da semana, como na consulta 5.4. Este índice é particularmente útil para consultas que envolvem
-- agregações e filtragens por dia da semana, país e cidade de partida e chegada. Antes de criar este índice,
-- a consulta 5.4 tinha um custo de 34061 e demorava 324ms. Depois de criar o índice, o custo da consulta foi reduzido para
-- 15637, com um tempo de execução de 67ms, mostrando uma melhoria significativa, na ordem dos 80%.