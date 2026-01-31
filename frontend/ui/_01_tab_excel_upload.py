# frontend/ui/_01_tab_excel_upload.py
from __future__ import annotations

import io
import re
import pandas as pd
import streamlit as st

from ui.components import sheets_summary_table
from ui.data_access import cached_upload, get_preview, run_cleaning, get_run_report, download_dataset, suggest_policy


def _is_excel_file(file_name: str) -> bool:
    f = (file_name or "").lower()
    return f.endswith(".xlsx") or f.endswith(".xls")


def _safe_sheet_name(name: str, fallback: str = "Sheet") -> str:
    n = (name or "").strip() or fallback
    n = re.sub(r"[:\\/?*\[\]]", "_", n)
    n = n[:31].strip()
    return n or fallback


def render_tab_ingestion() -> None:
    st.subheader("Upload files (.xlsx, .xls, .csv)")

    token = st.session_state.get("auth_token")
    if not token:
        st.info("Login first in tab 0) Auth")
        return

    uploaded_files = st.file_uploader(
        "Upload files",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="uploader_tab1",
    )

    if not uploaded_files:
        st.info("Upload one or more files to start")
        return

    if "files_registry" not in st.session_state:
        st.session_state.files_registry = {}

    for up in uploaded_files:
        if up.name not in st.session_state.files_registry:
            st.session_state.files_registry[up.name] = {
                "bytes": up.getvalue(),
                "datasets": None,
            }

    file_names = list(st.session_state.files_registry.keys())
    selected_file = st.selectbox("Select file to work with", file_names, key="selected_file_tab1")
    file_bytes = st.session_state.files_registry[selected_file]["bytes"]

    try:
        datasets_meta = cached_upload(file_bytes, selected_file, token_cache_key=token)
        st.session_state.files_registry[selected_file]["datasets"] = datasets_meta
    except Exception as e:
        st.error(f"Failed to upload/ingest file via API: {e}")
        return

    if not datasets_meta:
        st.error("No sheets/datasets returned from API.")
        return

    sheet_names = [d.get("sheet_name") for d in datasets_meta]
    selected_sheet_name = st.selectbox("Select sheet", sheet_names, key="selected_sheet_tab1")

    sheet_meta = next(d for d in datasets_meta if d.get("sheet_name") == selected_sheet_name)
    dataset_id = sheet_meta["dataset_id"]

    st.session_state["active_file_name"] = selected_file
    st.session_state["active_datasets_meta"] = datasets_meta
    st.session_state["active_sheet_meta"] = sheet_meta
    st.session_state["active_dataset_id"] = dataset_id

    st.session_state["datasets_meta"] = datasets_meta

    st.divider()
    st.subheader("Sheets summary")
    st.dataframe(sheets_summary_table(datasets_meta), width="stretch", hide_index=True)

    st.subheader(f"Raw preview — {sheet_meta['sheet_name']}")
    preview = get_preview(dataset_id, rows=30)
    st.dataframe(pd.DataFrame(preview["rows"]), width="stretch")

    st.subheader("Raw dtypes")
    dtypes = sheet_meta.get("dtypes", {}) or {}
    st.dataframe(
        pd.DataFrame([{"column": k, "dtype": v} for k, v in dtypes.items()]),
        width="stretch",
        hide_index=True,
    )