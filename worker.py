import requests, subprocess, os, socket, time, zipfile

MASTER_IP = "192.168.1.2:5000" 
WORKER_NAME = socket.gethostname()

def start_worker():
    # 1. Register
    requests.post(f"http://{MASTER_IP}:5000/register", json={"name": WORKER_NAME})
    
    # 2. Get Assignment
    task = requests.get(f"http://{MASTER_IP}:5000/get_task/{WORKER_NAME}").json()
    print(f"Assigning frames {task['start']} to {task['end']}")

    # 3. Download
    r = requests.get(task['file_url'])
    with open("job.blend", "wb") as f: f.write(r.content)

    # 4. Render & Update Master
    
    for frame in range(task['start'], task['end'] + 1):
        subprocess.run(["blender", "-b", "job.blend", "-o", f"//out_{frame}", "-f", str(frame)])
        requests.post(f"http://{MASTER_IP}:5000/update_progress", 
                     json={"name": WORKER_NAME, "count": frame - task['start'] + 1})

    # 5. Upload (simplified)
    print(f"📤 Sending results back...")
    with zipfile.ZipFile("results.zip", "w") as z:
        for frame in range(task['start'], task['end'] + 1):
            z.write(f"out_{frame:04d}.png")
    with open("results.zip", "rb") as f:
        requests.post(f"{MASTER_URL}/upload_result", 
                      data={'worker_name': WORKER_NAME}, 
                      files={'file': f})
    
    print("Done!")

if __name__ == "__main__":
    start_worker()
