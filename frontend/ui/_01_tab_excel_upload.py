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
    # Excel: max 31 chars, cannot contain : \ / ? * [ ]
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

    # 1) Upload files (только внутри Tab 1)
    uploaded_files = st.file_uploader(
        "Upload files",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="uploader_tab1",
    )

    if not uploaded_files:
        st.info("Upload one or more files to start")
        return

    # 2) Registry
    if "files_registry" not in st.session_state:
        st.session_state.files_registry = {}

    for up in uploaded_files:
        if up.name not in st.session_state.files_registry:
            st.session_state.files_registry[up.name] = {
                "bytes": up.getvalue(),
                "datasets": None,
            }

    # 3) Select file
    file_names = list(st.session_state.files_registry.keys())
    selected_file = st.selectbox("Select file to work with", file_names, key="selected_file_tab1")
    file_bytes = st.session_state.files_registry[selected_file]["bytes"]

    # 4) Upload/ingest via API (с учетом токена)
    try:
        datasets_meta = cached_upload(file_bytes, selected_file, token_cache_key=token)
        st.session_state.files_registry[selected_file]["datasets"] = datasets_meta
    except Exception as e:
        st.error(f"Failed to upload/ingest file via API: {e}")
        return

    if not datasets_meta:
        st.error("No sheets/datasets returned from API.")
        return

    # 5) Select sheet
    sheet_names = [d.get("sheet_name") for d in datasets_meta]
    selected_sheet_name = st.selectbox("Select sheet", sheet_names, key="selected_sheet_tab1")

    sheet_meta = next(d for d in datasets_meta if d.get("sheet_name") == selected_sheet_name)
    dataset_id = sheet_meta["dataset_id"]

    # 6) Save active selection to session_state for other tabs
    st.session_state["active_file_name"] = selected_file
    st.session_state["active_datasets_meta"] = datasets_meta
    st.session_state["active_sheet_meta"] = sheet_meta
    st.session_state["active_dataset_id"] = dataset_id

    # -------------------------
    # ✅ Excel-only MVP: Clean ALL sheets -> one combined Excel
    # -------------------------
    is_excel = _is_excel_file(selected_file)
    has_many_sheets = len(datasets_meta) > 1

    if is_excel and has_many_sheets:
        st.divider()
        st.subheader("🧽 Clean ALL sheets (Excel only)")

        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            use_llm_all = st.checkbox("Use LLM (experimental)", value=False, key="use_llm_all")
        with c2:
            llm_model_all = st.selectbox(
                "LLM model",
                ["gemini-2.5-flash"],
                index=0,
                disabled=not use_llm_all,
                key="llm_model_all",
            )
        with c3:
            st.caption("One button: (optional) policy → cleaning for every sheet → build one Excel with all cleaned sheets.")

        if st.button("🚀 Clean ALL sheets & build combined Excel", type="primary", use_container_width=True):
            progress = st.progress(0)
            status = st.empty()

            cleaned_frames: list[tuple[str, pd.DataFrame]] = []   # (sheet_name, df)
            run_results: list[dict] = []                          # optional debug

            total = len(datasets_meta)

            try:
                for i, meta in enumerate(datasets_meta, start=1):
                    ds_id = meta["dataset_id"]
                    sh_name = meta.get("sheet_name") or f"Sheet{i}"
                    safe_name = _safe_sheet_name(sh_name, fallback=f"Sheet{i}")

                    status.info(f"Cleaning {i}/{total}: {sh_name}")

                    # (A) если LLM — можно дернуть policy заранее (без кнопки)
                    if use_llm_all:
                        try:
                            suggest_policy(ds_id, mode="llm", llm_model=llm_model_all)
                        except Exception:
                            # policy не обязателен для запуска cleaning, поэтому не валим весь процесс
                            pass

                    # (B) запустить cleaning
                    run_id = run_cleaning(ds_id, use_llm=use_llm_all, llm_model=llm_model_all)
                    _ = get_run_report(run_id)  # чтобы убедиться, что done и есть отчет

                    # (C) скачать cleaned "current" как xlsx и прочитать в pandas
                    xlsx_bytes = download_dataset(ds_id, version="current", fmt="xlsx")
                    df_clean = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=0)
                    cleaned_frames.append((safe_name, df_clean))

                    run_results.append({"dataset_id": ds_id, "sheet_name": sh_name, "run_id": run_id})

                    progress.progress(i / total)

                # (D) собрать один итоговый xlsx (12 sheets)
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                    used = set()
                    for name, df in cleaned_frames:
                        final_name = name
                        # защита от одинаковых имен листов
                        if final_name in used:
                            base = final_name[:28]  # оставим место под _1,_2
                            k = 1
                            while f"{base}_{k}" in used:
                                k += 1
                            final_name = f"{base}_{k}"
                        used.add(final_name)

                        df.to_excel(writer, index=False, sheet_name=final_name)

                out.seek(0)
                st.success("✅ All sheets cleaned and combined Excel is ready")

                st.download_button(
                    "⬇️ Download combined cleaned Excel",
                    data=out.getvalue(),
                    file_name=f"{selected_file.rsplit('.',1)[0]}__CLEANED_ALL.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

                with st.expander("Debug (runs)", expanded=False):
                    st.json(run_results)

            except Exception as e:
                st.error(f"Clean ALL failed: {e}")
            finally:
                status.empty()


    st.divider()
    st.subheader("Sheets summary")
    st.dataframe(sheets_summary_table(datasets_meta), use_container_width=True, hide_index=True)

    st.subheader(f"Raw preview — {sheet_meta['sheet_name']}")
    preview = get_preview(dataset_id, rows=30)
    st.dataframe(pd.DataFrame(preview["rows"]), use_container_width=True)

    st.subheader("Raw dtypes")
    dtypes = sheet_meta.get("dtypes", {}) or {}
    st.dataframe(
        pd.DataFrame([{"column": k, "dtype": v} for k, v in dtypes.items()]),
        use_container_width=True,
        hide_index=True,
    )