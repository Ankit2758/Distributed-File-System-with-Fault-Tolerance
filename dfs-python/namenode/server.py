from flask import Flask, request, jsonify
import time

app = Flask(__name__)

# filename -> list of node URLs that have (some or all) chunks
metadata = {}

# node_url -> last heartbeat timestamp
node_status = {}


@app.route("/register", methods=["POST"])
def register_chunk():
    data = request.json
    filename = data["filename"]
    nodes = data["nodes"]  # IMPORTANT: list of nodes

    existing = metadata.get(filename, [])
    for n in nodes:
        if n not in existing:
            existing.append(n)

    metadata[filename] = existing
    return jsonify({"status": "ok"})


@app.route("/locations/<filename>", methods=["GET"])
def get_locations(filename):
    return jsonify(metadata.get(filename, [])), 200

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.json
    node = data["node"]
    node_status[node] = time.time()
    return jsonify({"status": "ok"})


@app.route("/nodes", methods=["GET"])
def get_nodes():
    now = time.time()
    timeout = 15  # seconds
    alive = [node for node, ts in node_status.items() if now - ts < timeout]
    return jsonify(alive)


def run_namenode(port=5000):
    app.run(port=port)


if __name__ == "__main__":
    run_namenode()
