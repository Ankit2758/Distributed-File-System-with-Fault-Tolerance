from flask import Flask, request, jsonify
import threading
import time
import requests

app = Flask(__name__)

NAMENODE_URL = "http://127.0.0.1:5000"

# (filename, index) -> bytes
chunks = {}


def heartbeat_loop(node_url):
    while True:
        try:
            requests.post(
                NAMENODE_URL + "/heartbeat",
                json={"node": node_url},
                timeout=1,
            )
        except Exception:
            pass
        time.sleep(5)


@app.route("/store", methods=["POST"])
def store_chunk():
    filename = request.args.get("filename")
    index = request.args.get("index", type=int)

    if filename is None or index is None:
        return jsonify({"error": "missing filename or index"}), 400

    chunks[(filename, index)] = request.data
    return jsonify({"status": "stored"})


@app.route("/chunk", methods=["GET"])
def get_chunk():
    filename = request.args.get("filename")
    index = request.args.get("index", type=int)

    key = (filename, index)
    if key not in chunks:
        return "", 404

    # return raw bytes
    return chunks[key]


def run_datanode(port):
    node_url = f"http://127.0.0.1:{port}"
    t = threading.Thread(target=heartbeat_loop, args=(node_url,), daemon=True)
    t.start()
    app.run(port=port)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        p = 6000
    else:
        p = int(sys.argv[1])

    run_datanode(p)
