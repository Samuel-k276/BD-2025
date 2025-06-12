%%sql

DROP MATERIALIZED VIEW IF EXISTS estatisticas_voos;

CREATE MATERIALIZED VIEW estatisticas_voos AS
SELECT
    v.no_serie,
    v.hora_partida,
    ap.cidade AS cidade_partida,
    ap.pais AS pais_partida,
    ac.cidade AS cidade_chegada,
    ac.pais AS pais_chegada,
    EXTRACT(YEAR FROM v.hora_partida) AS ano,
    EXTRACT(MONTH FROM v.hora_partida) AS mes,
    EXTRACT(DAY FROM v.hora_partida) AS dia_do_mes,
    TRIM(TO_CHAR(v.hora_partida, 'Day')) AS dia_da_semana,
    
    COUNT(CASE WHEN b.prim_classe = TRUE THEN 1 END) AS passageiros_1c,
    COUNT(CASE WHEN b.prim_classe = FALSE THEN 1 END) AS passageiros_2c,
    
    (SELECT COUNT(*) FROM assento a WHERE a.no_serie = v.no_serie AND a.prim_classe = TRUE) AS assentos_1c,
    (SELECT COUNT(*) FROM assento a WHERE a.no_serie = v.no_serie AND a.prim_classe = FALSE) AS assentos_2c,
    
    SUM(CASE WHEN b.prim_classe = TRUE THEN b.preco ELSE 0 END) AS vendas_1c,
    SUM(CASE WHEN b.prim_classe = FALSE THEN b.preco ELSE 0 END) AS vendas_2c
    
FROM voo v
JOIN aeroporto ap ON v.partida = ap.codigo
JOIN aeroporto ac ON v.chegada = ac.codigo
LEFT JOIN bilhete b ON v.id = b.voo_id
GROUP BY v.no_serie, v.hora_partida, ap.cidade, ap.pais, ac.cidade, ac.pais;