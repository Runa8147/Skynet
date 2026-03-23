from flask import Flask, request, send_file, jsonify
import os
import json

app = Flask(__name__)
STATUS_FILE = "cloud_state.json"

# Internal State
state = {
    "workers": {}, 
    "total_frames": 50,
    "blend_file": "project.blend"
}

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    name = data['name']
    if name not in state["workers"]:
        state["workers"][name] = {"status": "Online", "frames_done": 0, "score": 1.0}
    save_state()
    return "OK"

@app.route('/get_task/<name>', methods=['GET'])
def get_task(name):
    # Logic to trigger scheduler would go here
    # For now, worker calls this to get their assigned chunk
    from scheduler import GAScheduler
    
    worker_names = list(state["workers"].keys())
    history = {w: state["workers"][w]["score"] for w in worker_names}
    
    gen_ai = GAScheduler(worker_names, state["total_frames"])
    assignment = gen_ai.generate_schedule(history)
    
    return jsonify({
        "file_url": f"http://{request.host}/download_blend",
        "start": assignment[name]["start"],
        "end": assignment[name]["end"]
    })

@app.route('/update_progress', methods=['POST'])
def update():
    data = request.json
    state["workers"][data['name']]["frames_done"] = data['count']
    save_state()
    return "OK"

@app.route('/download_blend')
def download():
    return send_file(state["blend_file"])

@app.route('/upload_result', methods=['POST'])
def upload():
    file = request.files['file']
    file.save(f"rendered_results/{file.filename}")
    return "OK"

def save_state():
    with open(STATUS_FILE, 'w') as f:
        json.dump(state, f)

if __name__ == '__main__':
    os.makedirs("rendered_results", exist_ok=True)
    save_state()
    app.run(host='0.0.0.0', port=5000)