%%sql

-- populate.sql
BEGIN;

-- 1. Insert airports (≥10 European international airports, with 2 cities having 2 airports)
INSERT INTO aeroporto (codigo, nome, cidade, pais) VALUES
   -- London (2 airports)
   ('LHR', 'Heathrow Airport', 'London', 'United Kingdom'),
   ('LGW', 'Gatwick Airport', 'London', 'United Kingdom'),
   
   -- Paris (2 airports)
   ('CDG', 'Charles de Gaulle Airport', 'Paris', 'France'),
   ('ORY', 'Orly Airport', 'Paris', 'France'),
   
   -- Other major European airports
   ('FRA', 'Frankfurt Airport', 'Frankfurt', 'Germany'),
   ('AMS', 'Amsterdam Schiphol Airport', 'Amsterdam', 'Netherlands'),
   ('MAD', 'Adolfo Suárez Madrid-Barajas Airport', 'Madrid', 'Spain'),
   ('FCO', 'Leonardo da Vinci-Fiumicino Airport', 'Rome', 'Italy'),
   ('LIS', 'Humberto Delgado Airport', 'Lisbon', 'Portugal'),
   ('ZRH', 'Zurich Airport', 'Zurich', 'Switzerland'),
   ('CPH', 'Copenhagen Airport', 'Copenhagen', 'Denmark'),
   ('VIE', 'Vienna International Airport', 'Vienna', 'Austria');

-- 2. Insert airplanes (≥10 planes of ≥3 distinct models)
INSERT INTO aviao (no_serie, modelo) VALUES
   ('B737-001', 'Boeing 737-800'),
   ('B737-002', 'Boeing 737-800'),
   ('B737-003', 'Boeing 737-800'),
   ('B737-004', 'Boeing 737-800'),
   ('A320-001', 'Airbus A320'),
   ('A320-002', 'Airbus A320'),
   ('A320-003', 'Airbus A320'),
   ('A320-004', 'Airbus A320'),
   ('B787-001', 'Boeing 787'),
   ('B787-002', 'Boeing 787'),
   ('E195-001', 'Embraer E195');

-- 3. Insert seats (first ~10% rows are first class)
INSERT INTO assento (lugar, no_serie, prim_classe)
SELECT
   row_num::text || letter AS lugar,
   a.no_serie,
   row_num <= 2 AS prim_classe
FROM
   aviao a
   CROSS JOIN generate_series(1, 20) AS row_num
   CROSS JOIN (SELECT chr(n) AS letter FROM generate_series(65, 70) n) letters;

-- 4. Insert flights (≥5 flights per day Jan-Jul 2025, round trips, proper airplane routing)
DO $$
DECLARE
   flight_date DATE;
   departure_time TIMESTAMP;
   arrival_time TIMESTAMP;
   flight_id INTEGER;
   route RECORD;
   airplane_no VARCHAR;
   airplane_idx INTEGER := 0;
   route_num INTEGER := 0;
   airplane_count INTEGER := (SELECT COUNT(*) FROM aviao);
   route_count INTEGER := (SELECT COUNT(*) FROM aeroporto a1 CROSS JOIN aeroporto a2 WHERE a1.codigo != a2.codigo);
BEGIN
   FOR flight_date IN SELECT generate_series(
      '2025-01-01'::DATE, 
      '2025-07-31'::DATE, 
      '1 day'::INTERVAL
   )
   LOOP
      FOR i IN 1..15 LOOP
         SELECT a1.codigo, a2.codigo AS codigo2
         INTO route
         FROM aeroporto a1 CROSS JOIN aeroporto a2
         WHERE a1.codigo != a2.codigo AND a1.cidade != a2.cidade
         OFFSET ( route_num % route_count )
         LIMIT 1;

         route_num := route_num + 1;

         SELECT no_serie INTO airplane_no
         FROM aviao
         ORDER BY no_serie
         LIMIT 1 OFFSET airplane_idx;

         airplane_idx := (airplane_idx + 1) % airplane_count;

         departure_time := flight_date + TIME '06:00' + (i * INTERVAL '2 hours');
         arrival_time := departure_time + INTERVAL '2 hours';

         INSERT INTO voo (no_serie, hora_partida, hora_chegada, partida, chegada)
         VALUES (airplane_no, departure_time, arrival_time, route.codigo, route.codigo2)
         RETURNING id INTO flight_id;

         departure_time := arrival_time + INTERVAL '1 hour';
         arrival_time := departure_time + INTERVAL '2 hours';

         INSERT INTO voo (no_serie, hora_partida, hora_chegada, partida, chegada)
         VALUES (airplane_no, departure_time, arrival_time, route.codigo2, route.codigo);
      END LOOP;
   END LOOP;
END $$;


-- 5. Insert sales and tickets (≥30,000 tickets in ≥10,000 sales)
DO $$
DECLARE
   sale_id INTEGER;
   flight RECORD;
   seat RECORD;
   ticket_count INTEGER := 0;
   sale_count INTEGER := 0;
   passenger_num INTEGER;
   flight_date DATE;
BEGIN
   -- For each flight that has already occurred (before current date)
   FOR flight IN SELECT id, hora_partida, no_serie FROM voo 
   WHERE hora_partida < NOW() ORDER BY hora_partida
   LOOP
      -- Create 1-13 sales per flight
      FOR s IN 1..(1 + random() * 13)::INTEGER LOOP
         sale_count := sale_count + 1;
         
         INSERT INTO venda (nif_cliente, balcao, hora)
         VALUES (
            LPAD((random() * 999999999)::INTEGER::TEXT, 9, '0'),
            (SELECT codigo FROM aeroporto ORDER BY random() LIMIT 1),
            flight.hora_partida - INTERVAL '1 day' + (random() * INTERVAL '23 hours')
         )
         RETURNING codigo_reserva INTO sale_id;
         
         -- Create 1-6 tickets per sale
         passenger_num := 0;
         FOR t IN 1..(1 + random() * 6)::INTEGER LOOP
            ticket_count := ticket_count + 1;
            passenger_num := passenger_num + 1;
             
            -- Select an available seat (alternating between first and economy class)
            SELECT a.lugar, a.no_serie, a.prim_classe INTO seat
            FROM assento a
            WHERE a.no_serie = flight.no_serie
            AND a.prim_classe = (t % 5 = 4)
            AND NOT EXISTS (
               SELECT 1 FROM bilhete b 
               WHERE b.voo_id = flight.id 
               AND b.lugar = a.lugar 
               AND b.no_serie = a.no_serie
            )
            LIMIT 1;
            
            IF seat IS NOT NULL THEN
               INSERT INTO bilhete (
                  voo_id, codigo_reserva, nome_passegeiro, 
                  preco, prim_classe, lugar, no_serie
               ) VALUES (
                  flight.id, sale_id, 
                  'Passenger ' || passenger_num || ' of Sale ' || sale_id,
                  CASE WHEN seat.prim_classe THEN 500 + (random() * 1000) 
                     ELSE 100 + (random() * 400) END,
                     seat.prim_classe, seat.lugar, seat.no_serie
               );
            END IF;
         END LOOP;
      END LOOP;
   END LOOP;
    
   RAISE NOTICE 'Created % sales with % total tickets', sale_count, ticket_count;
END $$;

COMMIT;