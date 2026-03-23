import requests
import subprocess
import os
import socket
import time
import zipfile


MASTER_IP = "192.168.1.2:5000"
WORKER_NAME = socket.gethostname()


def start_worker():
    # 1. Register
    requests.post(
        f"http://{MASTER_IP}/register",
        json={"name": WORKER_NAME}
    )

    # 2. Get Assignment
    task = requests.get(
        f"http://{MASTER_IP}/get_task/{WORKER_NAME}"
    ).json()
    print(f"Assigning frames {task['start']} to {task['end']}")

    # 3. Download blend file
    r = requests.get(task["file_url"])
    with open("job.blend", "wb") as f:
        f.write(r.content)

    # 4. Render frames & update master
    for frame in range(task["start"], task["end"] + 1):
        # Use simple padded index: out_1.png, out_2.png, ...
        index = frame - task["start"] + 1
        subprocess.run([
            "blender",
            "-b", "job.blend",
            "-o", f"//out_{index:04d}",   # Blender writes out_0001.png, out_0002.png, ...
            "-f", str(frame)
        ])
        progress = frame - task["start"] + 1
        requests.post(
            f"http://{MASTER_IP}/update_progress",
            json={"name": WORKER_NAME, "count": progress}
        )

    # 5. Upload results (zip)
    print("📤 Sending results back...")
    zip_path = "results.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        for idx in range(1, (task["end"] - task["start"] + 2)):
            png_name = f"out_{idx:04d}.png"
            if os.path.exists(png_name):
                z.write(png_name)

    with open(zip_path, "rb") as f:
        requests.post(
            f"http://{MASTER_IP}/upload_result",
            data={"worker_name": WORKER_NAME},
            files={"file": f}
        )

    print("Done!")


if __name__ == "__main__":
    start_worker()
