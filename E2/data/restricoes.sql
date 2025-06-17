-- This script creates triggers and functions to enforce business rules on the database.
-- These rules ensure data integrity and consistency in the aviation database

-- RI-1
-- Aquando do check-in (i.e. quando se define o assento em bilhete) a classe do bilhete tem de corresponder à classe do assento e o aviao do assento tem de corresponder ao aviao do voo
CREATE OR REPLACE FUNCTION check_classe_assento() 
RETURNS trigger AS $$
BEGIN
   IF NEW.lugar IS NULL OR NEW.no_serie IS NULL THEN
      RETURN NEW;
   END IF;
   IF NOT EXISTS (
      SELECT 1 FROM assento
      WHERE lugar = NEW.lugar AND no_serie = NEW.no_serie AND prim_classe = NEW.prim_classe
   ) THEN
      RAISE EXCEPTION 'Classe do bilhete não corresponde à do assento';
   END IF;

   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_classe_assento
   BEFORE INSERT OR UPDATE OF lugar ON bilhete
   FOR EACH ROW
   EXECUTE FUNCTION check_classe_assento();

-- RI-2
-- O número de bilhetes de cada classe vendidos para cada voo não pode exceder a capacidade (i.e., número de assentos) do avião para essa classe
CREATE OR REPLACE FUNCTION check_capacidade_classe()
   RETURNS trigger AS 
$$
DECLARE
   capacidade INT;
   bilhetes_reservados INT;
BEGIN
   IF NEW.prim_classe THEN
      SELECT COUNT(*) INTO capacidade
      FROM assento
      WHERE no_serie = (SELECT no_serie FROM voo WHERE id = NEW.voo_id)
         AND prim_classe = TRUE;

      SELECT COUNT(*) INTO bilhetes_reservados
      FROM bilhete
      WHERE voo_id = NEW.voo_id AND prim_classe = TRUE;

      IF bilhetes_reservados >= capacidade THEN
         RAISE EXCEPTION 'Capacidade da primeira classe excedida para o voo';
      END IF;

   ELSE
      SELECT COUNT(*) INTO capacidade
      FROM assento
      WHERE no_serie = (SELECT no_serie FROM voo WHERE id = NEW.voo_id)
         AND prim_classe = FALSE;

      SELECT COUNT(*) INTO bilhetes_reservados
      FROM bilhete
      WHERE voo_id = NEW.voo_id AND prim_classe = FALSE;

      IF bilhetes_reservados >= capacidade THEN
         RAISE EXCEPTION 'Capacidade da segunda classe excedida.';
      END IF;
   END IF;

   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_capacidade_classe
  BEFORE INSERT OR UPDATE OF voo_id, prim_classe ON bilhete
  FOR EACH ROW
  EXECUTE FUNCTION check_capacidade_classe();

-- RI-3
-- A hora da venda tem de ser anterior à hora de partida de todos os voos para os quais foram comprados bilhetes na venda
CREATE OR REPLACE FUNCTION check_hora_venda()
   RETURNS trigger AS 
$$
DECLARE
   min_hora_partida TIMESTAMP;
   hora_venda TIMESTAMP;
BEGIN   
   SELECT voo.hora_partida INTO min_hora_partida
   FROM voo
   WHERE voo.id = NEW.voo_id;

   SELECT venda.hora INTO hora_venda
   FROM venda
   WHERE venda.codigo_reserva = NEW.codigo_reserva;
    
   IF min_hora_partida IS NOT NULL AND hora_venda IS NOT NULL AND hora_venda >= min_hora_partida THEN
      RAISE EXCEPTION 'Hora da venda deve ser anterior à menor hora de partida dos bilhetes.';
   END IF;

   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_hora_venda
   BEFORE INSERT OR UPDATE OF voo_id, codigo_reserva ON bilhete
   FOR EACH ROW
   EXECUTE FUNCTION check_hora_venda();