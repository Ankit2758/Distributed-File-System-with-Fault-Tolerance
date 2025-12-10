import os
import requests

NAMENODE_URL = "http://127.0.0.1:5000"
DATANODE_URLS = [
    "http://127.0.0.1:6000",
    "http://127.0.0.1:6001",
]

CHUNK_SIZE = 1024 * 64  # 64KB


def upload_file(path):
    filename = os.path.basename(path)
    print(f"Uploading {filename}...")

    with open(path, "rb") as f:
        index = 0
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break

            successful_nodes = []

            for node in DATANODE_URLS:
                try:
                    r = requests.post(
                        node + "/store",
                        params={"filename": filename, "index": index},
                        data=chunk,
                        timeout=2,
                    )
                    if r.status_code == 200:
                        successful_nodes.append(node)
                except requests.exceptions.RequestException:
                    print(f"[WARN] Could not reach {node} for chunk {index}")

            if successful_nodes:
                # register chunk locations
                requests.post(
                    NAMENODE_URL + "/register",
                    json={"filename": filename, "nodes": successful_nodes},
                    timeout=2,
                )
            else:
                print(f"[ERROR] No node stored chunk {index}")

            index += 1

    print("Upload complete")


def get_locations(filename):
    if not filename:
        print("No filename")
        return []
    r = requests.get(NAMENODE_URL + "/locations/" + filename, timeout=2)
    if r.status_code != 200:
        print("File not found")
        return []
    try:
        locs = r.json()
    except ValueError:
        print("Invalid JSON from namenode")
        return []
    print("Locations:", locs)
    return locs


def download_file(filename, output_path):
    if not filename:
        print("No filename for download")
        return

    nodes = get_locations(filename)
    if not nodes:
        print("No locations known for file")
        return

    # try nodes one by one until one works
    for node in nodes:
        print(f"Trying node {node} for download...")
        try:
            index = 0
            wrote_any = False
            with open(output_path, "wb") as out:
                while True:
                    r = requests.get(
                        node + "/chunk",
                        params={"filename": filename, "index": index},
                        timeout=2,
                    )
                    if r.status_code != 200 or not r.content:
                        break
                    out.write(r.content)
                    wrote_any = True
                    index += 1

            if wrote_any:
                print(f"Downloaded {filename} from {node} to {output_path}")
                return
            else:
                print(f"No chunks found on {node}")
        except requests.exceptions.RequestException:
            print(f"[WARN] Node {node} unreachable during download")

    print("[ERROR] Could not download file from any node")
