"""
master.py  -  Orion Render Master (complete rewrite)

Key design:
  - render_started flag: workers poll /can_start and wait until dashboard
    operator clicks "Start Rendering".
  - Assignments locked in once at render start; never recomputed mid-run.
  - /missing_frames  returns list of frame numbers not yet on disk.
  - /get_remaining/<worker>  reassigns any missing frames to a requesting worker.
  - GA reads historical scores from cloud_state.json on startup so past
    performance data informs future scheduling.
  - _all_done checks actual PNG files on disk, not just frames_done counter
    (survives worker crashes).
  - Video assembly triggered when all frames present on disk.
"""

from flask import Flask, request, send_file, jsonify
import os, json, glob, threading
from scheduler import generate_frame_schedule

app        = Flask(__name__)
STATUS_FILE = "cloud_state.json"
RESULTS_DIR = "rendered_results"

state = {
    "workers":          {},   # name -> {status, frames_done, score, fps_history}
    "assignments":      {},   # name -> {start, end}
    "total_frames":     250,
    "frame_start":      1,    # first frame index in the .blend timeline
    "blend_file":       "project.blend",
    "finished_workers": [],
    "render_started":   False,
    "video_assembled":  False,
    "missing_frames":   [],   # frames not yet on disk
}

_lock = threading.Lock()


# ── persistence ───────────────────────────────────────────────────────────────

def save_state():
    with open(STATUS_FILE, "w") as f:
        json.dump(state, f, indent=2)

def load_state():
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE) as f:
            saved = json.load(f)
        # Merge saved workers so historical scores survive restarts.
        # Never load "master" — it is not a render worker.
        for k, v in saved.items():
            if k == "workers":
                for wname, wdata in v.items():
                    if wname == "master":
                        continue          # strip legacy master entries
                    if wname not in state["workers"]:
                        state["workers"][wname] = wdata
                    else:
                        # keep best historical score
                        state["workers"][wname]["score"] = max(
                            state["workers"][wname].get("score", 1.0),
                            wdata.get("score", 1.0),
                        )
                        hist = wdata.get("fps_history", [])
                        state["workers"][wname].setdefault("fps_history", [])
                        state["workers"][wname]["fps_history"].extend(hist)
            elif k not in ("render_started", "video_assembled", "finished_workers",
                           "assignments", "missing_frames"):
                state[k] = v


# ── helpers ───────────────────────────────────────────────────────────────────

def _frames_on_disk():
    """Return sorted list of global frame numbers present as PNGs on disk."""
    pngs = glob.glob(os.path.join(RESULTS_DIR, "frame_*.png"))
    nums = []
    for p in pngs:
        base = os.path.splitext(os.path.basename(p))[0]  # "frame_000042"
        try:
            nums.append(int(base.split("_")[1]))
        except (IndexError, ValueError):
            pass
    return sorted(nums)

def _all_frames():
    """Set of all expected frame numbers."""
    fs = state["frame_start"]
    return set(range(fs, fs + state["total_frames"]))

def _missing():
    """Frame numbers expected but not yet on disk."""
    return sorted(_all_frames() - set(_frames_on_disk()))

def _build_history():
    """Return {worker: score} using fps_history average if available."""
    hist = {}
    for wname, wdata in state["workers"].items():
        fps_list = wdata.get("fps_history", [])
        if fps_list:
            hist[wname] = sum(fps_list) / len(fps_list)
        else:
            hist[wname] = wdata.get("score", 1.0)
    return hist

def _recompute_assignments(frames_to_assign=None):
    """Run GA over given frame list (defaults to all frames).
    Must be called while holding _lock."""
    worker_names = [w for w in state["workers"] if w != "master"]
    if not worker_names:
        return
    if frames_to_assign is None:
        frames_to_assign = list(_all_frames())
    history = _build_history()
    state["assignments"] = generate_frame_schedule(
        workers=worker_names,
        history=history,
        frames=frames_to_assign,
        pop_size=30,
        generations=60,
    )
    # Caller is responsible for save_state()

def _check_and_assemble():
    """If all frames are on disk and video not yet made, assemble it."""
    missing = _missing()
    if not missing and not state["video_assembled"]:
        state["missing_frames"] = []
        save_state()
        threading.Thread(target=_assemble_video, daemon=True).start()
    else:
        state["missing_frames"] = missing
        save_state()


# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/register", methods=["POST"])
def register():
    data  = request.json
    wname = data["name"]
    with _lock:
        if wname not in state["workers"]:
            state["workers"][wname] = {
                "status":      "Online",
                "frames_done": 0,
                "score":       float(data.get("score", 1.0)),
                "fps_history": [],
            }
        else:
            state["workers"][wname]["status"] = "Online"
        save_state()
    return jsonify({"status": "registered"})


@app.route("/can_start/<worker_name>", methods=["GET"])
def can_start(worker_name):
    """Workers poll this. Always returns 200.
    {go: false} while waiting; {go: true, frames: [...]} once started.
    Auto-registers the worker if it isn't known yet so polling before
    /register never causes a 404.
    """
    with _lock:
        # Only auto-register unknown workers BEFORE render starts.
        # After start, unknown devices get go=False — prevents ghost workers
        # from polluting the list after GA assignments are locked.
        if worker_name not in state["workers"]:
            if state["render_started"]:
                return jsonify({"go": False,
                                "reason": "render already started; please register before start"})
            state["workers"][worker_name] = {
                "status":      "Online",
                "frames_done": 0,
                "score":       1.0,
                "fps_history": [],
            }
            save_state()

        if not state["render_started"]:
            return jsonify({"go": False, "reason": "waiting for start"})

        asgn = state["assignments"].get(worker_name)
        if not asgn:
            remaining = _missing()
            if remaining:
                _recompute_assignments(frames_to_assign=remaining)
                asgn = state["assignments"].get(worker_name)
            if not asgn:
                return jsonify({"go": False, "reason": "no frames left to assign"})

    return jsonify({
        "go":       True,
        "file_url": f"http://{request.host}/download_blend",
        "frames":   asgn["frames"],
    })



@app.route("/get_task/<worker_name>", methods=["GET"])
def get_task_compat(worker_name):
    """Compatibility shim for old worker.py that calls /get_task/.
    Redirects to the /can_start flow so old and new workers both work.
    """
    with _lock:
        # Auto-register if needed
        if worker_name not in state["workers"]:
            if not state["render_started"]:
                state["workers"][worker_name] = {
                    "status": "Online", "frames_done": 0,
                    "score": 1.0, "fps_history": [],
                }
                save_state()
            else:
                return jsonify({"error": "render already started, register first"}), 400

        if not state["render_started"]:
            # Old worker expects a task immediately — tell it to wait and retry
            return jsonify({
                "waiting": True,
                "message": "Render not started yet. Update to new worker.py which polls /can_start."
            }), 202

        asgn = state["assignments"].get(worker_name)
        if not asgn:
            return jsonify({"error": "no frames assigned to this worker"}), 204

    # Return in old {start, end} format AND new {frames} format
    frames = asgn.get("frames", [])
    return jsonify({
        "file_url": f"http://{request.host}/download_blend",
        "start":    frames[0] if frames else 1,
        "end":      frames[-1] if frames else 1,
        "frames":   frames,
    })


@app.route("/start_render", methods=["POST"])
def start_render():
    """Called by the dashboard Start button.
    Returns explicit error JSON on every failure path so the dashboard
    can display a meaningful message instead of 'Failed: {}'.
    """
    with _lock:
        if state["render_started"]:
            return jsonify({"status": "already started",
                            "assignments": state["assignments"]})

        # Guard: refuse to start with no render workers registered
        real_workers = [w for w in state["workers"] if w != "master"]
        if not real_workers:
            return jsonify({
                "status": "error",
                "error":  (f"No render workers registered. "
                           f"Run worker.py on each render node BEFORE clicking Start. "
                           f"Registered now: {list(state['workers'].keys())}")
            }), 400

        try:
            _recompute_assignments()
        except Exception as exc:
            return jsonify({
                "status": "error",
                "error":  f"GA scheduler failed: {exc}"
            }), 500

        if not state["assignments"]:
            return jsonify({
                "status": "error",
                "error":  "Scheduler returned empty assignments — check total_frames and workers."
            }), 500

        state["render_started"] = True
        try:
            save_state()
        except Exception as exc:
            # State is set; warn but don't abort the render
            print(f"[master] WARNING: save_state failed: {exc}")

    return jsonify({"status": "started", "assignments": state["assignments"]})


@app.route("/update_progress", methods=["POST"])
def update_progress():
    data    = request.json
    wname   = data["name"]
    count   = int(data["count"])
    elapsed = float(data.get("elapsed_seconds", 0))
    with _lock:
        if wname not in state["workers"]:
            return jsonify({"error": "unknown worker"}), 404
        state["workers"][wname]["frames_done"] = count
        if elapsed > 0 and count > 0:
            fps = count / elapsed
            state["workers"][wname]["score"] = fps
            state["workers"][wname].setdefault("fps_history", []).append(fps)
            # keep last 20 samples
            state["workers"][wname]["fps_history"] = \
                state["workers"][wname]["fps_history"][-20:]
        save_state()
    return jsonify({"status": "ok"})


@app.route("/download_blend")
def download_blend():
    blend = state["blend_file"]
    if not os.path.exists(blend):
        return jsonify({"error": "blend file not found"}), 404
    return send_file(blend)


@app.route("/upload_result", methods=["POST"])
def upload_result():
    import zipfile
    file        = request.files.get("file")
    worker_name = request.form.get("worker_name", "unknown")
    if not file:
        return jsonify({"error": "no file"}), 400

    save_path = os.path.join(RESULTS_DIR, f"{worker_name}_results.zip")
    file.save(save_path)
    with zipfile.ZipFile(save_path) as z:
        z.extractall(RESULTS_DIR)

    with _lock:
        if worker_name not in state["finished_workers"]:
            state["finished_workers"].append(worker_name)
        _check_and_assemble()

    return jsonify({
        "status":         "received",
        "missing_frames": state["missing_frames"],
    })


@app.route("/missing_frames", methods=["GET"])
def missing_frames_endpoint():
    with _lock:
        m = _missing()
        state["missing_frames"] = m
    return jsonify({"missing": m, "count": len(m)})


@app.route("/get_remaining/<worker_name>", methods=["GET"])
def get_remaining(worker_name):
    """A worker that finished early (or is recovering) calls this to get
    any frames that haven't landed on disk yet."""
    with _lock:
        if not state["render_started"]:
            return jsonify({"frames": [], "reason": "not started"})
        remaining = _missing()
        if not remaining:
            _check_and_assemble()
            return jsonify({"frames": [], "reason": "all done"})
        # Give this worker all remaining frames (it's the only one asking)
        state["assignments"][worker_name] = {"frames": remaining}
        save_state()
    return jsonify({
        "frames":   remaining,
        "file_url": f"http://{request.host}/download_blend",
    })


@app.route("/status")
def status():
    with _lock:
        enriched = dict(state)
        enriched["frames_on_disk"]  = len(_frames_on_disk())
        enriched["missing_count"]   = len(_missing())
    return jsonify(enriched)


@app.route("/reset", methods=["POST"])
def reset():
    """Hard reset - clears assignments and render_started flag."""
    with _lock:
        state["render_started"]   = False
        state["video_assembled"]  = False
        state["finished_workers"] = []
        state["assignments"]      = {}
        state["missing_frames"]   = []
        for w in state["workers"]:
            state["workers"][w]["frames_done"] = 0
            state["workers"][w]["status"]      = "Online"
        save_state()
    return jsonify({"status": "reset"})


# ── video assembly ─────────────────────────────────────────────────────────────

def _assemble_video():
    import subprocess
    frames = sorted(glob.glob(os.path.join(RESULTS_DIR, "frame_*.png")))
    if not frames:
        print("[master] No frames to assemble.")
        return

    list_path = os.path.join(RESULTS_DIR, "frame_list.txt")
    fps       = 24
    with open(list_path, "w") as f:
        for fp in frames:
            f.write(f"file '{os.path.abspath(fp)}'\n")
            f.write(f"duration {1/fps:.6f}\n")

    output = "final_render.mp4"
    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", list_path,
        "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-crf", "18", "-r", str(fps),
        output,
    ]
    print(f"[master] Assembling {len(frames)} frames -> {output}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[master] ✅ Video ready: {output}")
        with _lock:
            state["video_assembled"] = True
            save_state()
    else:
        print(f"[master] FFmpeg error:\n{result.stderr[-1000:]}")


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs(RESULTS_DIR, exist_ok=True)
    load_state()
    # Remove "master" from workers — it is not a render node
    with _lock:
        state["workers"].pop("master", None)
        save_state()
    print(f"[master] Starting. total_frames={state['total_frames']}, "
          f"blend={state['blend_file']}")
    print(f"[master] Known workers from history: {list(state['workers'].keys())}")
    app.run(host="0.0.0.0", port=5000, threaded=True)
