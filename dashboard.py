"""
dashboard.py  -  Orion Render Dashboard (Streamlit)

Features:
  - "Start Rendering" button calls /start_render on master.
  - "Reset" button clears state for a fresh run.
  - Shows per-worker progress bars using actual assignment data.
  - Shows missing frames count and which frames are missing.
  - Shows GA-derived assignments (frame lists per worker).
  - Historical score (fps) per worker from cloud_state.json.
  - Auto-refreshes every 2 s.
  - Video download link when assembly is complete.
"""

import streamlit as st
import json, time, requests, os
import pandas as pd

MASTER_URL  = "http://127.0.0.1:5000"
STATE_FILE  = "cloud_state.json"

st.set_page_config(page_title="Orion Render Cloud", layout="wide", page_icon="🌌")
st.title("🌌 Orion: GA-Optimised Distributed Render Cloud")


# ── helpers ───────────────────────────────────────────────────────────────────

def get_data():
    """Try live API first, fall back to JSON file."""
    try:
        r = requests.get(f"{MASTER_URL}/status", timeout=2)
        if r.ok:
            return r.json(), True
    except Exception:
        pass
    try:
        with open(STATE_FILE) as f:
            return json.load(f), False
    except Exception:
        return None, False


def post_action(endpoint, json_body=None):
    try:
        r = requests.post(f"{MASTER_URL}/{endpoint}", json=json_body, timeout=10)
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text[:300]}
        return r.ok, body
    except requests.exceptions.ConnectionError:
        return False, {"error": f"Cannot reach master at {MASTER_URL} — is master.py running?"}
    except requests.exceptions.Timeout:
        return False, {"error": "Master timed out — GA may be overloaded"}
    except Exception as e:
        return False, {"error": str(e)}


# ── sidebar controls ──────────────────────────────────────────────────────────

with st.sidebar:
    st.header("⚙️ Controls")

    if st.button("🚀 Start Rendering", type="primary", width="stretch"):
        ok, resp = post_action("start_render")
        status_val = resp.get("status", "")
        if status_val == "already started":
            st.warning("⚠️ Render already running. Use Reset to restart.")
        elif ok and status_val == "started":
            assignments = resp.get("assignments", {})
            if not assignments:
                st.error("❌ Started but no workers were assigned — are any workers registered?")
            else:
                st.success(f"✅ Render started! {len(assignments)} worker(s) assigned.")
                for w, asgn in assignments.items():
                    frames = asgn.get("frames", [])
                    st.write(f"**{w}**: {len(frames)} frames "
                             f"({frames[0] if frames else '?'}–{frames[-1] if frames else '?'})")
        else:
            err = resp.get("error") or resp.get("raw") or str(resp)
            st.error(f"❌ Start failed: {err}")

    st.divider()

    if st.button("🔄 Reset", type="secondary", width="stretch"):
        ok, resp = post_action("reset")
        if ok:
            st.success("Reset done.")
        else:
            st.error(f"Failed: {resp}")

    st.divider()
    st.caption("Auto-refreshes every 2 s")

    # Config section - read from state file
    st.subheader("📁 Config")
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            cfg = json.load(f)
        st.write(f"Blend file: `{cfg.get('blend_file','?')}`")
        st.write(f"Total frames: `{cfg.get('total_frames','?')}`")
        st.write(f"Frame start: `{cfg.get('frame_start', 1)}`")
    else:
        st.write("_Master not started yet_")


# ── main panel ────────────────────────────────────────────────────────────────

placeholder = st.empty()

while True:
    data, live = get_data()

    with placeholder.container():
        conn_badge = "🟢 Live" if live else "🔴 File fallback"
        st.caption(f"Connection: {conn_badge}  |  Last refresh: {time.strftime('%H:%M:%S')}")

        if not data:
            st.warning("⏳ Waiting for master to start...")
            time.sleep(2)
            continue

        workers       = data.get("workers", {})
        assignments   = data.get("assignments", {})
        total_frames  = data.get("total_frames", 1)
        render_started = data.get("render_started", False)
        video_done    = data.get("video_assembled", False)
        missing_count  = data.get("missing_count", len(data.get("missing_frames", [])))
        frames_on_disk = data.get("frames_on_disk", 0)

        # ── status banner ──────────────────────────────────────────────────
        if video_done:
            st.success("🎬 Render complete! Video assembled: `final_render.mp4`")
            if os.path.exists("final_render.mp4"):
                with open("final_render.mp4", "rb") as vf:
                    st.download_button("⬇️ Download Video", vf, "final_render.mp4", "video/mp4")
        elif not render_started:
            st.info("⏸️ Render not started — click **Start Rendering** in the sidebar.")
        elif missing_count > 0:
            st.warning(f"🔄 Rendering in progress... {frames_on_disk}/{total_frames} frames on disk "
                       f"({missing_count} missing)")
        else:
            st.success(f"✅ All {total_frames} frames on disk — assembling video...")

        # ── top metrics ────────────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Workers", len(workers))
        c2.metric("Total Frames", total_frames)
        c3.metric("On Disk", frames_on_disk)
        c4.metric("Missing", missing_count,
                  delta=None if missing_count == 0 else f"-{missing_count}",
                  delta_color="inverse")

        # ── worker table ────────────────────────────────────────────────────
        st.subheader("Worker Status")
        rows = []
        for name, info in workers.items():
            asgn        = assignments.get(name, {})
            frame_list  = asgn.get("frames", [])
            assigned    = len(frame_list)
            done        = info.get("frames_done", 0)
            fps         = info.get("score", 1.0)
            fps_history = info.get("fps_history", [])
            avg_fps     = sum(fps_history)/len(fps_history) if fps_history else fps
            rows.append({
                "Worker":       name,
                "Status":       info.get("status", "?"),
                "Assigned":     assigned,
                "Done":         done,
                "Current FPS":  f"{fps:.2f}",
                "Avg FPS (history)": f"{avg_fps:.2f}",
                "Frame Range":  f"{frame_list[0]}–{frame_list[-1]}" if frame_list else "—",
            })
        if rows:
            df = pd.DataFrame(rows).set_index("Worker")
            st.dataframe(df, width="stretch")

        # ── progress bars ──────────────────────────────────────────────────
        st.subheader("Per-Worker Progress")
        for name, info in workers.items():
            asgn       = assignments.get(name, {})
            frame_list = asgn.get("frames", [])
            assigned   = len(frame_list)
            done       = info.get("frames_done", 0)
            ratio      = done / assigned if assigned > 0 else 0.0
            pct        = min(ratio * 100, 100)
            col_a, col_b = st.columns([3, 1])
            col_a.write(f"**{name}**")
            col_b.write(f"{done}/{assigned} ({pct:.0f}%)")
            st.progress(min(ratio, 1.0))

        # ── missing frames detail ───────────────────────────────────────────
        missing_list = data.get("missing_frames", [])
        if missing_list:
            with st.expander(f"⚠️ {len(missing_list)} missing frame(s)"):
                st.write(missing_list[:100])
                if len(missing_list) > 100:
                    st.caption(f"... and {len(missing_list)-100} more")

        # ── GA assignment detail ────────────────────────────────────────────
        if assignments and render_started:
            with st.expander("📊 GA Frame Assignments"):
                for wname, asgn in assignments.items():
                    flist = asgn.get("frames", [])
                    st.write(f"**{wname}**: {len(flist)} frames — "
                             f"{flist[:10]}{'...' if len(flist)>10 else ''}")

    time.sleep(2)
