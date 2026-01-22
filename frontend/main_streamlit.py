# frontend/main_streamlit.py
import streamlit as st

from ui._00_tab_authentication import render_tab_auth
from ui._01_tab_excel_upload import render_tab_ingestion
from ui._02_tab_cleaning import render_tab_cleaning
from ui._03_tab_signals import render_tab_signals
from ui._04_tab_visualization import render_tab_visualization
from ui._05_save_all_files import render_tab_saved_datasets

st.set_page_config(page_title="4CAST", layout="wide")
st.title("4CAST: Multi-Agent Excel Pipeline")
st.caption("Ingestion → Cleaning → Signals → Visualization")

if "auth_token" not in st.session_state:
    st.session_state.auth_token = None

if "runs_store" not in st.session_state:
    st.session_state.runs_store = {}


def is_authed() -> bool:
    return bool(st.session_state.get("auth_token"))


def get_active_dataset():

    dataset_id = st.session_state.get("active_dataset_id")
    sheet_meta = st.session_state.get("active_sheet_meta")
    selected_file = st.session_state.get("active_file_name")
    if not dataset_id or not sheet_meta or not selected_file:
        return None, None, None
    return selected_file, sheet_meta, dataset_id


tab0, tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "0) Auth",
    "1) Ingestion & Sheets",
    "2) Data Cleaning",
    "3) Signal Generation",
    "4) Visualization",
    "5) Download All Cleaned Files",
])

with tab0:
    render_tab_auth()

with tab1:
    if not is_authed():
        st.info("Please login in tab 0) Auth to use the app.")
    else:
        render_tab_ingestion()

selected_file, sheet_meta, dataset_id = get_active_dataset()

with tab2:
    if not is_authed():
        st.info("Please login first.")
    elif not dataset_id:
        st.info("Go to tab 1) Ingestion & Sheets and select a dataset first.")
    else:
        run_id, report = render_tab_cleaning(selected_file, sheet_meta, dataset_id=dataset_id)
        if run_id and report:
            st.session_state.runs_store[dataset_id] = {
                "file_name": selected_file,
                "sheet_name": sheet_meta.get("sheet_name"),
                "run_id": run_id,
                "report": report,
            }
            st.success("Dataset cleaned and saved")

with tab3:
    if not is_authed():
        st.info("Please login first.")
    elif not dataset_id:
        st.info("Select a dataset in tab 1 first.")
    else:
        item = st.session_state.runs_store.get(dataset_id)
        if not item:
            st.info("Clean the dataset first (tab 2).")
        else:
            render_tab_signals(item["report"])

with tab4:
    if not is_authed():
        st.info("Please login first.")
    elif not dataset_id:
        st.info("Select a dataset in tab 1 first.")
    else:
        item = st.session_state.runs_store.get(dataset_id)
        if not item:
            st.info("Clean the dataset first (tab 2).")
        else:
            render_tab_visualization()

with tab5:
    if not is_authed():
        st.info("Please login first.")
    else:
        render_tab_saved_datasets()