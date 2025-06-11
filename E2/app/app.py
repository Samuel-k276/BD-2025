import os
from datetime import datetime, timedelta
from logging.config import dictConfig
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from zoneinfo import ZoneInfo
import psycopg2


app = Flask(__name__)


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
    timeout=5,
)

DB_CONFIG = {
    'dbname': 'Aviacao',
    'user': 'teu_user',
    'password': 'tua_password',
    'host': 'localhost',
    'port': 5432
}

def get_db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


@app.route('/', methods=['GET'])
def list_all_airports():

   with pool.connections() as conn:
      with conn.cursor() as cur:
         try:
            cur.execute("""SELECT nome, cidade FROM aeroporto ORDER BY nome""")
            airports = cur.fetchall()
            return jsonify([{"nome": row[0], "cidade": row[1]} for row in airports])
         except Exception as e:
            return jsonify({"error": str(e)}), 500
         


@app.route('/voos/<partida>', methods=['GET'])
def list_flights_from_departure(partida):
   return jsonify(f"List of flights from {partida}")

@app.route('/voos/<partida>/<chegada>', methods=['GET'])
def list_flights(partida, chegada):
   try:
      now = datetime.now()
      with pool.connection as conn:
         with conn.cursor() as cur:
            cur.execute("""
                     SELECT v.no_serie, v.hora_partida
                     from voo v
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
                  """, (partida.upper(), chegada.upper(), now))
            rows = cur.fetchall()
            results = [
               {"no_serie": row[0], "hora_partida": row[1].isoformat()}
               for row in rows
            ]
            return jsonify(results), 200
   except Exception as e:
      return jsonify({"error": str(e)}), 500

            
                     
   


@app.route('/compra/<voo>', methods=['GET'])
def make_purchhase(voo):
   """
   json = {
      nif_cliente: 123456789,`
      bilhetes: [{nome: "John Doe", classe: 1}, {nome: "Jane Doe", classe: 2}]`
   }
   """
   data = request.get_json()
   nif_cliente = data.get('nif_cliente')
   bilhetes = data.get('bilhetes')

   if not nif_cliente or not bilhetes:
      return jsonify({"error": "Invalid data"}), 400

   return jsonify({
      "message": f"Purchase made for flight {voo}",
      "nif_cliente": nif_cliente,
      "bilhetes": bilhetes
   }), 200
   


@app.route('/checkin/<bilhete>', methods=['GET'])
def check_in(bilhete):
   return jsonify(f"Check-in for ticket {bilhete} successful"	)


if __name__ == '__main__':
   app.run(debug=True, port=5000)
