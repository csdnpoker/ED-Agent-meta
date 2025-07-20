from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/ping', methods=['POST'])
def ping():
    data = request.get_json()
    print(f"Received: {data}")
    return jsonify({'response': f"Hello from {request.host}!"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050)