import os
from datetime import datetime, timedelta
from random import random
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Lisbon")

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@postgres/postgres")

pool = ConnectionPool(
   conninfo=DATABASE_URL,
   kwargs={
      "autocommit": False, # Use transactions
      "row_factory": namedtuple_row,
   },
   min_size=4,
   max_size=10,
   open=True,
   # check=ConnectionPool.check_connection,
   name="postgres_pool",
   timeout=10,
)


def get_time():
   """
   Retorna a hora atual no fuso horário de Lisboa.
   """
   return datetime.now(TZ)

app = Flask(__name__)
log = app.logger


@app.route('/', methods=['GET'])
def airports():
   """
   Retorna a lista de aeroportos.
   """
   with pool.connection() as conn:
      with conn.cursor() as cur:
         try:
            airports = cur.execute(
               """
               SELECT nome, cidade 
               FROM aeroporto 
               ORDER BY nome
               """
            ).fetchall()
            log.info(f"Retrieved {len(airports)} airports from the database.")
            return jsonify([{"nome": row[0], "cidade": row[1]} for row in airports]), 200
         except Exception as e:
            log.error(f"Database error: {e}")
            return jsonify({"error": str(e)}), 500


@app.route('/voos/<partida>', methods=['GET'])
def list_flights_from_departure(partida):
   """
   Retorna os voos que partem de um aeroporto específico, até 12h após o momento da consulta.
   """

   if not isinstance(partida, str) or not len(partida) == 3 or not partida.isalpha():
      return jsonify({"error": "Aeroporto de partida inválido, deve ter 3 letras"}), 400

   with pool.connection() as conn:
      with conn.cursor() as cur:
         try:
            now = get_time() + timedelta(hours=12)
            rows = cur.execute(
               """
               SELECT no_serie, hora_partida, partida, chegada 
               FROM voo
               WHERE partida = %s AND hora_partida <= %s
               ORDER BY hora_partida ASC;
               """, 
               (partida, now)
            ).fetchall()
            voos = [{"número de série": row[0],"hora de partida": row[1].isoformat(),"Aeroporto de chegada": row[3]} for row in rows]
            return jsonify(voos), 200
         except Exception as e:
            log.error(f"Database error: {e}")
            return jsonify({"error": str(e)}), 500


@app.route('/voos/<partida>/<chegada>', methods=['GET'])
def list_flights(partida, chegada):
   """
   Retorna os próximos 3 voos entre dois aeroportos específicos, para os quais ainda há bilhetes disponíveis.
   """
   if (not (isinstance(partida, str) and isinstance(chegada, str)) or
      not (len(partida) == 3 and len(chegada) == 3) or
      not (partida.isalpha() and chegada.isalpha())):
      return jsonify({"error": "Aeroportos inválidos, devem ter 3 letras"}), 400

   now = get_time()
   with pool.connection() as conn:
      with conn.cursor() as cur:
         try:
            cur.execute(
               """
               SELECT v.no_serie, v.hora_partida
               FROM voo v
               WHERE v.partida = %s 
                  AND v.chegada = %s 
                  AND v.hora_partida > %s
                  AND EXISTS (
                     SELECT 1
                     FROM assento a
                     WHERE a.no_serie = v.no_serie
                        AND NOT EXISTS (
                           SELECT 1 FROM bilhete b
                           WHERE b.voo_id = v.id
                              AND b.no_serie = a.no_serie
                              AND b.lugar = a.lugar
                        )  
                  )
               ORDER BY v.hora_partida ASC
               LIMIT 3;    
               """, 
               (partida.upper(), chegada.upper(), now)
            )
            rows = cur.fetchall()
            results = [
               {"no_serie": row[0], "hora_partida": row[1].isoformat()}
               for row in rows
            ]
            return jsonify(results), 200
         except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route('/compra/<voo>', methods=['POST'])
def make_purchase(voo):
   """
   Registra a compra de bilhetes para um voo específico.
   Recebe no corpo da request um JSON com o NIF do cliente e uma lista de bilhetes.
   """
   data = request.get_json()
   nif_cliente = data.get("nif_cliente")
   bilhetes = data.get("bilhetes")

   if not (nif_cliente and bilhetes):
      return jsonify({"error": "Nif e bilhetes são obrigatórios"}), 400
   
   if len(nif_cliente) != 9 or not nif_cliente.isdigit():
      return jsonify({"error": "NIF inválido, deve ter 9 dígitos"}), 400

   with pool.connection() as conn:
      with conn.cursor() as cur:
         try:
            no_serie = cur.execute(
               """
               SELECT no_serie 
               FROM voo 
               WHERE id = %s
               """, 
               (voo,)
            ).fetchone()
            
            if not no_serie:
               return jsonify({"error": "Voo não encontrado"}), 404
            no_serie = no_serie[0]

            now = get_time()
            codigo_reserva = cur.execute(
               """
               INSERT INTO venda (nif_cliente, hora)
               VALUES (%s, %s)
               RETURNING codigo_reserva;
               """, 
               (nif_cliente, now)
            ).fetchone()[0]

            return_bilhetes = []
            for b in bilhetes:
               if not isinstance(b, dict) or "nome" not in b or "classe" not in b:
                  return jsonify({"error": "Cada bilhete deve ser um objeto com 'nome' e 'classe'"}), 400
               
               if not isinstance(b["nome"], str) or not b["nome"].strip():
                  return jsonify({"error": "Nome do passageiro inválido"}), 400
               
               if b["classe"] not in [1, 2]:
                  return jsonify({"error": "Classe inválida, deve ser 1 ou 2"}), 400

               prim_classe = b["classe"] == 1
               preco = 500 + (random() * 1000) if prim_classe else 100 + (random() * 400)
               cur.execute(
                  """
                  INSERT INTO bilhete (voo_id, codigo_reserva, nome_passegeiro, preco, prim_classe, no_serie)
                  VALUES (%s, %s, %s, %s, %s, %s)
                  RETURNING id, preco;
                  """, 
                  (voo, codigo_reserva, b["nome"], preco, prim_classe, no_serie)
               )
               bilhete_id, preco = cur.fetchone()
               return_bilhetes.append({
                  "id": bilhete_id,
                  "nome": b["nome"],
                  "classe": b["classe"],
                  "preco": preco
               })
            
            conn.commit()
            
            return jsonify({
               "message": f"Compra realizada para o voo {voo}",
               "codigo_reserva": codigo_reserva,
               "nif_cliente": nif_cliente,
               "bilhetes": return_bilhetes
               }), 200
         
         except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


@app.route('/checkin/<bilhete>', methods=['POST'])
def check_in(bilhete):
   """
   Realiza o check-in de um bilhete, atribuindo um lugar disponível.
   Retorna o lugar atribuído ou um erro se não houver lugares disponíveis.
   """
   try:
      bilhete_id = int(bilhete)
   except ValueError:
      return jsonify({"error": "ID do bilhete inválido, deve ser um número inteiro"}), 400

   with pool.connection() as conn:
      with conn.cursor() as cur:
         try:
            bilhete = cur.execute(
               """
               SELECT b.id, b.voo_id, b.no_serie, b.prim_classe, b.lugar
               FROM bilhete b
               WHERE b.id = %s
               """,
               (bilhete_id,)
            ).fetchone()

            if bilhete is None:
               return jsonify({"error": "Bilhete não encontrado"}), 404

            if bilhete[4] is not None:
               return jsonify({"error": "Check-in já feito"}), 409

            voo_id = bilhete[1]
            no_serie = bilhete[2]
            prim_classe = bilhete[3]

            # Procurar um assento disponível da classe correspondente
            assento_livre = cur.execute(
               """
               SELECT a.lugar
               FROM assento a
               WHERE a.no_serie = %s
                  AND a.prim_classe = %s
                  AND NOT EXISTS (
                     SELECT 1 FROM bilhete b
                     WHERE b.no_serie = a.no_serie
                        AND b.lugar = a.lugar
                        AND b.voo_id = %s
                  )
               LIMIT 1
               """,
               (no_serie, prim_classe, voo_id)
            ).fetchone()

            lugar_atribuido = assento_livre[0]

            # Atualizar o bilhete com o lugar atribuído
            cur.execute(
               """
               UPDATE bilhete
               SET lugar = %s
               WHERE id = %s
               """,
               (lugar_atribuido, bilhete_id)
            )

            conn.commit()
            
            return jsonify({
               "message": "Check-in efetuado com sucesso",
               "lugar_atribuido": lugar_atribuido
            }), 200

         except Exception as e:
            conn.rollback()
            log.error(f"Database error: {e}")
            return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
   app.run(debug=True, host='0.0.0.0')
