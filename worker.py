from flask import Flask, request, jsonify
import subprocess
import requests
import socket
import os

app = Flask(__name__)

MASTER_URL = "http://172.16.15.224:5000"
BLENDER_PATH = r"C:\Program Files\Blender Foundation\Blender 4.5\blender.exe"


@app.route("/run", methods=["POST"])
def run():
    start = request.json["start"]
    end = request.json["end"]
    file_path = request.json["file"]

    cmd = [
        BLENDER_PATH,
        "-b", file_path,
        "-s", str(start),
        "-e", str(end),
        "-a"
    ]

    subprocess.run(cmd)

    return jsonify({"status": "done"})


@app.route("/health")
def health():
    return jsonify({"status": "alive"})


def register():
    worker_ip = socket.gethostname()
    try:
        requests.post(f"{MASTER_URL}/register", json={"worker": worker_ip})
    except:
        pass


if __name__ == "__main__":
    register()
    app.run(host="0.0.0.0", port=5001)
