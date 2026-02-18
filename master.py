from flask import Flask, request, jsonify
import requests
import threading

app = Flask(__name__)
workers = []


@app.route("/register", methods=["POST"])
def register():
    worker = request.json["worker"]
    if worker not in workers:
        workers.append(worker)
    return jsonify({"registered": worker})


@app.route("/workers")
def get_workers():
    return jsonify(workers)


@app.route("/submit", methods=["POST"])
def submit():
    total_frames = request.json["frames"]

    if not workers:
        return jsonify({"error": "No workers available"}), 400

    chunk = total_frames // len(workers)

    threads = []

    start = 1
    for worker in workers:
        end = start + chunk - 1
        t = threading.Thread(
            target=send_job,
            args=(worker, start, end)
        )
        t.start()
        threads.append(t)
        start = end + 1

    return jsonify({"status": "job dispatched"})


def send_job(worker, start, end):
    requests.post(
        f"http://{worker}:5001/run",
        json={"start": start, "end": end, "file": "/shared/scene.blend"}
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
