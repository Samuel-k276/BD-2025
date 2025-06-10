from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/', methods=['GET'])
def list_all_airports():
   return jsonify("List of all airports")

@app.route('/voos/<partida>', methods=['GET'])
def list_flights_from_departure(partida):
   return f"List of flights from {partida}"

@app.route('/voos/<partida>/<chegada>', methods=['GET'])
def list_flights(partida, chegada):
   return jsonify(f"List of flights from {partida} to {chegada}")


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
   return f"Check-in for ticket {bilhete} successful"	


if __name__ == '__main__':
   app.run(debug=True, port=5000)
