DROP TABLE IF EXISTS aeroporto CASCADE;
DROP TABLE IF EXISTS aviao CASCADE;
DROP TABLE IF EXISTS assento CASCADE;
DROP TABLE IF EXISTS voo CASCADE;
DROP TABLE IF EXISTS venda CASCADE;
DROP TABLE IF EXISTS bilhete CASCADE;

CREATE TABLE aeroporto(
	codigo CHAR(3) PRIMARY KEY CHECK (codigo ~ '^[A-Z]$'),
	nome VARCHAR(80) NOT NULL,
	cidade VARCHAR(255) NOT NULL,
	pais VARCHAR(255) NOT NULL,
	UNIQUE (nome, cidade)
);

CREATE TABLE aviao(
	no_serie VARCHAR(80) PRIMARY KEY,
	modelo VARCHAR(80) NOT NULL
);

CREATE TABLE assento (
	lugar VARCHAR(3) CHECK (lugar ~ '^[0-9]{1,2}[A-Z]$'),
	no_serie VARCHAR(80) REFERENCES aviao,
	prim_classe BOOLEAN NOT NULL DEFAULT FALSE,
	PRIMARY KEY (lugar, no_serie)
);

CREATE TABLE voo (
	id SERIAL PRIMARY KEY,
	no_serie VARCHAR(80) REFERENCES aviao,
	hora_partida TIMESTAMP,
	hora_chegada TIMESTAMP, 
	partida CHAR(3) REFERENCES aeroporto(codigo),
	chegada CHAR(3) REFERENCES aeroporto(codigo),
	UNIQUE (no_serie, hora_partida),
	UNIQUE (no_serie, hora_chegada),
	UNIQUE (hora_partida, partida, chegada),
	UNIQUE (hora_chegada, partida, chegada),
	CHECK (partida!=chegada),
	CHECK (hora_partida<=hora_chegada)
);

CREATE TABLE venda (
	codigo_reserva SERIAL PRIMARY KEY,
	nif_cliente CHAR(9) NOT NULL,
	balcao CHAR(3) REFERENCES aeroporto(codigo),
	hora TIMESTAMP

	CONSTRAINT check_hora_venda CHECK (
		hora < (SELECT MIN(hora_partida) FROM bilhete WHERE bilhete.codigo_reserva = venda.codigo_reserva JOIN voo ON voo.id = bilhete.voo_id)
	)
);

CREATE TABLE bilhete (
	id SERIAL PRIMARY KEY,
	voo_id INTEGER REFERENCES voo,
	codigo_reserva INTEGER REFERENCES venda,
	nome_passegeiro VARCHAR(80),
	preco NUMERIC(7,2) NOT NULL,
	prim_classe BOOLEAN NOT NULL DEFAULT FALSE,
	lugar VARCHAR(3),
	no_serie VARCHAR(80),
	UNIQUE (voo_id, codigo_reserva, nome_passegeiro),
	FOREIGN KEY (lugar, no_serie) REFERENCES assento

   CONSTRAINT check_classe_assento CHECK (
		(lugar IS NULL AND no_serie IS NULL) OR
		(lugar IS NOT NULL AND no_serie IS NOT NULL AND
		 prim_classe = (SELECT prim_classe FROM assento WHERE lugar = bilhete.lugar AND no_serie = bilhete.no_serie))
	)

	CONSTRAINT check_no_serie CHECK (
		(lugar IS NULL AND no_serie IS NULL) OR
		(lugar IS NOT NULL AND no_serie IS NOT NULL AND
		 no_serie = (SELECT no_serie FROM voo WHERE voo_id = voo.id))
	)

	CONSTRAINT check_capacidade_classe CHECK (
		((SELECT COUNT(*) FROM bilhete WHERE voo_id = voo.id AND prim_classe = TRUE) <=
		(SELECT COUNT(*) FROM assento WHERE no_serie = voo.no_serie AND prim_classe = TRUE)) AND 
		((SELECT COUNT(*) FROM bilhete WHERE voo_id = voo.id AND prim_classe = FALSE) <=
		(SELECT COUNT(*) FROM assento WHERE no_serie = voo.no_serie AND prim_classe = FALSE))
	)
);