from __future__ import annotations

import io
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
import streamlit as st

from ui.data_access import list_my_runs, download_artifact, delete_run


def _safe(s: str) -> str:
    return (s or "").replace("/", "_").replace("\\", "_").replace(" ", "_")


def _safe_sheet_name(name: str) -> str:
    # Excel: <= 31 символ, нельзя []:*?/\
    bad = r'[]:*?/\\'
    n = (name or "").strip() or "Sheet"
    for ch in bad:
        n = n.replace(ch, "_")
    n = n[:31].strip()
    return n or "Sheet"


def _parse_dt(s: Optional[str]) -> datetime:
    if not s:
        return datetime.min
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return datetime.min


def render_tab_saved_datasets():
    token = st.session_state.get("auth_token")
    st.subheader("📂 Saved cleaned datasets (grouped by file)")

    if not token:
        st.info("Please login first.")
        return

    try:
        payload = list_my_runs(token)
        runs: List[Dict[str, Any]] = payload.get("runs", []) or []
    except Exception as e:
        st.error(f"Failed to load runs: {e}")
        return

    if not runs:
        st.info("No cleaned datasets yet.")
        return

    # группируем по file_name
    by_file: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in runs:
        file_name = r.get("file_name") or "(unknown file)"
        by_file[file_name].append(r)

    for file_name, entries in sorted(by_file.items(), key=lambda x: x[0].lower()):
        # ✅ берём ПОСЛЕДНИЙ run для каждого sheet (чтобы Jan дубль не мешал)
        latest_per_sheet: Dict[str, Dict[str, Any]] = {}
        for r in entries:
            sheet = r.get("sheet_name") or "Sheet"
            cur = latest_per_sheet.get(sheet)
            if cur is None or _parse_dt(r.get("created_at")) > _parse_dt(cur.get("created_at")):
                latest_per_sheet[sheet] = r

        sheets_sorted = sorted(latest_per_sheet.items(), key=lambda kv: kv[0].lower())

        with st.container(border=True):
            st.write(f"📄 **{file_name}**")
            st.caption(f"Sheets cleaned: {len(sheets_sorted)} | Total runs stored: {len(entries)}")

            a1, a2, a3 = st.columns([2.4, 2.0, 1.4])

            with a1:
                if st.button("⬇️ Download COMBINED cleaned Excel", key=f"dl_combined_{file_name}", use_container_width=True):
                    try:
                        out = io.BytesIO()
                        used = set()
                        with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                            for sheet_name, r in sheets_sorted:
                                run_id = r["run_id"]
                                cleaned_bytes = download_artifact(run_id, "cleaned.xlsx")

                                df = pd.read_excel(io.BytesIO(cleaned_bytes), sheet_name=0)

                                safe_sheet = _safe_sheet_name(sheet_name)
                                final_sheet = safe_sheet
                                if final_sheet in used:
                                    base = final_sheet[:28]
                                    k = 1
                                    while f"{base}_{k}" in used:
                                        k += 1
                                    final_sheet = f"{base}_{k}"
                                used.add(final_sheet)

                                df.to_excel(writer, index=False, sheet_name=final_sheet)

                        out.seek(0)
                        st.download_button(
                            "✅ Click to download",
                            data=out.getvalue(),
                            file_name=f"{_safe(file_name.rsplit('.',1)[0])}__CLEANED_ALL.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_btn_{file_name}",
                            use_container_width=True
                        )
                    except Exception as e:
                        st.error(f"Build combined Excel failed: {e}")

            with a2:
                if st.button("⬇️ Download reports.zip", key=f"dl_reports_{file_name}", use_container_width=True):
                    try:
                        zbuf = io.BytesIO()
                        with zipfile.ZipFile(zbuf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                            for sheet_name, r in sheets_sorted:
                                run_id = r["run_id"]
                                rep = download_artifact(run_id, "report.json")
                                zf.writestr(f"{_safe_sheet_name(sheet_name)}__{run_id}__report.json", rep)
                        zbuf.seek(0)
                        st.download_button(
                            "✅ Click to download",
                            data=zbuf.getvalue(),
                            file_name=f"{_safe(file_name.rsplit('.',1)[0])}__reports.zip",
                            mime="application/zip",
                            key=f"dl_zip_btn_{file_name}",
                            use_container_width=True
                        )
                    except Exception as e:
                        st.error(f"Build reports.zip failed: {e}")


            with a3:
                if st.button("❌ Delete ALL runs", key=f"del_all_{file_name}", use_container_width=True):
                    try:
                        for r in entries:
                            delete_run(r["run_id"])
                        st.success("Deleted ✅")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")

            st.divider()

            rows = []
            for sheet_name, r in sheets_sorted:
                rows.append({
                    "sheet": sheet_name,
                    "run_id": r["run_id"],
                    "status": r.get("status"),
                    "created_at": r.get("created_at"),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)