"""
worker.py  -  Orion Render Worker  (v2 — uses /can_start polling flow)

Usage:
    python3 worker.py

Environment variables:
    MASTER_IP    IP:port of the master  (default: 127.0.0.1:5000)
    WORKER_NAME  Override hostname      (default: socket.gethostname())

Flow:
    1. Register with master (/register)
    2. Poll /can_start/<name> every 5 s until master signals go
    3. Receive explicit list of frame numbers to render
    4. Render each frame with Blender → frame_NNNNNN.png (global numbering)
    5. Zip sorted PNGs and upload (/upload_result)
    6. Poll /get_remaining for any missed frames and re-render (recovery loop)
"""

import requests, subprocess, os, socket, zipfile, time, tempfile

WORKER_VERSION = "2.0"
MASTER_IP      = os.environ.get("MASTER_IP",   "127.0.0.1:5000")
WORKER_NAME    = os.environ.get("WORKER_NAME", socket.gethostname())
POLL_INTERVAL  = 5    # seconds between /can_start polls
MAX_RETRIES    = 5
RETRY_DELAY    = 3


# ── network helpers ───────────────────────────────────────────────────────────

def _req(method, url, ok_statuses=(200,), **kwargs):
    """Retry wrapper. Returns response on success, raises RuntimeError on total failure."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.request(method, url, timeout=60, **kwargs)
            if r.status_code in ok_statuses:
                return r
            # Non-retryable client errors
            if r.status_code in (400, 401, 403):
                raise RuntimeError(f"HTTP {r.status_code} from {url}: {r.text[:200]}")
            # 404/5xx — log and retry
            print(f"[worker] {method} {url} → {r.status_code} "
                  f"(attempt {attempt+1}/{MAX_RETRIES}): {r.text[:120]}")
        except requests.RequestException as e:
            print(f"[worker] {method} {url} network error "
                  f"(attempt {attempt+1}/{MAX_RETRIES}): {e}")
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)
    raise RuntimeError(f"{method} {url} failed after {MAX_RETRIES} attempts")


# ── rendering ─────────────────────────────────────────────────────────────────

def render_frames(frame_list, blend_path, render_dir):
    """
    Render every frame in frame_list with Blender.
    Output files: render_dir/frame_NNNNNN.png  (global frame number).
    Returns (frames_done_count, elapsed_seconds).
    """
    os.makedirs(render_dir, exist_ok=True)
    start_time  = time.time()
    frames_done = 0

    for frame in sorted(frame_list):
        # Template WITHOUT extension — Blender appends .png automatically.
        # Result: render_dir/frame_000042.png
        out_template = os.path.join(render_dir, f"frame_{frame:06d}")

        print(f"[worker] Rendering frame {frame} ...")
        result = subprocess.run(
            ["blender", "-b", blend_path,
             "-o", out_template,
             "-F", "PNG",
             "-f", str(frame)],
            capture_output=True, text=True,
        )

        expected_png = out_template + ".png"
        if result.returncode == 0 and os.path.exists(expected_png):
            frames_done += 1
        else:
            print(f"[worker] ⚠ Frame {frame} failed "
                  f"(returncode={result.returncode})")
            if result.stderr:
                print(result.stderr[-300:])

        # Report progress to master after each frame
        elapsed = time.time() - start_time
        try:
            _req("POST", f"http://{MASTER_IP}/update_progress",
                 ok_statuses=(200,),
                 json={"name": WORKER_NAME,
                       "count": frames_done,
                       "elapsed_seconds": elapsed})
        except RuntimeError:
            pass  # progress updates are non-fatal

    return frames_done, time.time() - start_time


def zip_and_upload(render_dir, label="results"):
    """Zip frame_*.png files (sorted by name) and upload to master."""
    png_files = sorted(
        f for f in os.listdir(render_dir)
        if f.startswith("frame_") and f.endswith(".png")
    )
    if not png_files:
        print("[worker] No PNGs to upload — skipping.")
        return []

    zip_path = f"{WORKER_NAME}_{label}.zip"
    print(f"[worker] Zipping {len(png_files)} frames → {zip_path}")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for png in png_files:
            z.write(os.path.join(render_dir, png), arcname=png)

    print(f"[worker] Uploading {zip_path} ...")
    with open(zip_path, "rb") as f:
        resp = _req("POST", f"http://{MASTER_IP}/upload_result",
                    ok_statuses=(200,),
                    data={"worker_name": WORKER_NAME},
                    files={"file": (zip_path, f, "application/zip")})

    return resp.json().get("missing_frames", [])


# ── main worker loop ──────────────────────────────────────────────────────────

def start_worker():
    base = f"http://{MASTER_IP}"

    print(f"[worker] Orion worker v{WORKER_VERSION}")
    print(f"[worker] Name : {WORKER_NAME}")
    print(f"[worker] Master: {MASTER_IP}")

    # ── 1. Register ───────────────────────────────────────────────────────────
    print(f"[worker] Registering ...")
    _req("POST", f"{base}/register",
         ok_statuses=(200,),
         json={"name": WORKER_NAME})
    print(f"[worker] Registered as '{WORKER_NAME}'")

    # ── 2. Poll /can_start until master signals go ────────────────────────────
    print(f"[worker] Waiting for Start Rendering signal "
          f"(polling every {POLL_INTERVAL}s) ...")
    task_data = None
    while True:
        try:
            r = _req("GET", f"{base}/can_start/{WORKER_NAME}",
                     ok_statuses=(200,))
            data = r.json()
            if data.get("go"):
                task_data = data
                break
            reason = data.get("reason", "waiting")
            print(f"[worker] Not started ({reason}) — retrying in {POLL_INTERVAL}s ...")
        except RuntimeError as e:
            print(f"[worker] Poll error: {e} — retrying in {POLL_INTERVAL}s ...")
        time.sleep(POLL_INTERVAL)

    frame_list = task_data.get("frames", [])
    file_url   = task_data.get("file_url")

    if not frame_list:
        print("[worker] Received empty frame list — nothing to render. Exiting.")
        return

    print(f"[worker] ✅ GO! Assigned {len(frame_list)} frames: "
          f"{frame_list[:8]}{'...' if len(frame_list) > 8 else ''}")

    # ── 3. Download blend file ────────────────────────────────────────────────
    print(f"[worker] Downloading blend file from {file_url} ...")
    r = _req("GET", file_url, ok_statuses=(200,))
    blend_path = "job.blend"
    with open(blend_path, "wb") as f:
        f.write(r.content)
    print(f"[worker] Blend file saved ({len(r.content)//1024} KB)")

    # ── 4. Render primary assignment ──────────────────────────────────────────
    render_dir = tempfile.mkdtemp(prefix="orion_")
    done, elapsed = render_frames(frame_list, blend_path, render_dir)
    print(f"[worker] Primary render complete: "
          f"{done}/{len(frame_list)} frames in {elapsed:.1f}s "
          f"({done/elapsed:.2f} fps)" if elapsed > 0 else "")

    # ── 5. Upload primary results ─────────────────────────────────────────────
    zip_and_upload(render_dir, label="primary")

    # ── 6. Recovery loop — render any frames still missing on master ──────────
    for attempt in range(10):
        try:
            r = _req("GET", f"{base}/get_remaining/{WORKER_NAME}",
                     ok_statuses=(200,))
            data = r.json()
        except RuntimeError:
            break

        remaining = data.get("frames", [])
        if not remaining:
            print("[worker] ✅ All frames accounted for. Done!")
            break

        print(f"[worker] Recovery pass {attempt + 1}: "
              f"rendering {len(remaining)} missing frames {remaining[:5]}...")
        rec_dir = tempfile.mkdtemp(prefix="orion_rec_")
        render_frames(remaining, blend_path, rec_dir)
        zip_and_upload(rec_dir, label=f"recovery_{attempt + 1}")

    print("[worker] Worker finished.")


if __name__ == "__main__":
    start_worker()import requests
import subprocess
import os
import socket
import zipfile


MASTER_IP = "192.168.1.2:5000"
WORKER_NAME = socket.gethostname()


def start_worker():
    # 1. Register
    requests.post(
        f"http://{MASTER_IP}/register",
        json={"name": WORKER_NAME},
    )

    # 2. Get assignment
    task = requests.get(f"http://{MASTER_IP}/get_task/{WORKER_NAME}").json()
    print(f"Assigning frames {task['start']} to {task['end']}")

    # 3. Download blend file
    r = requests.get(task["file_url"])
    with open("job.blend", "wb") as f:
        f.write(r.content)

    # 4. Render: use index 1,2,3,... (not original frame numbers)
    for frame in range(task["start"], task["end"] + 1):
        idx = frame - task["start"] + 1   # 1, 2, 3, ...
        subprocess.run([
            "blender",
            "-b", "job.blend",
            "-o", f"//out_{idx:04d}",      # writes out_0001.png, out_0002.png, ...
            "-f", str(frame)
        ])

    # 5. Upload results (zip in nice order)
    zip_path = "results.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        for idx in range(1, task["end"] - task["start"] + 2):
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
