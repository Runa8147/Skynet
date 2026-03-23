import streamlit as st
import json
import time
import pandas as pd

st.set_page_config(page_title="Orion Cloud Render", layout="wide")
st.title("🌌 Orion: GA-Optimized Render Cloud")

def get_data():
    try:
        with open("cloud_state.json", "r") as f:
            return json.load(f)
    except: return None

placeholder = st.empty()

while True:
    data = get_data()
    if data:
        with placeholder.container():
            col1, col2 = st.columns(2)
            col1.metric("Active Workers", len(data['workers']))
            col2.metric("Target Frames", data['total_frames'])

            st.subheader("Worker Status")
            df = pd.DataFrame.from_dict(data['workers'], orient='index')
            st.table(df)

            for name, info in data['workers'].items():
                st.write(f"**{name}** Progress")
                # Visual progress bar
                prog = info['frames_done'] / (data['total_frames']/len(data['workers']))
                st.progress(min(prog, 1.0))
                
    time.sleep(2)