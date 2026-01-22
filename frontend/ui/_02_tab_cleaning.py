from __future__ import annotations

import streamlit as st
import pandas as pd

from ui.data_access import suggest_policy, run_cleaning, get_run_report, download_artifact, get_preview


def render_tab_cleaning(file_name: str, sheet_meta: dict, dataset_id: str):
    """
    Returns:
        run_id, report
    """
    st.subheader(f"🧽 Cleaning — {sheet_meta['sheet_name']}")

    # ---- optional: show small preview before cleaning
    with st.expander("Show current preview (from backend)", expanded=False):
        preview = get_preview(dataset_id, rows=10)
        st.dataframe(pd.DataFrame(preview["rows"]), width="stretch")

    # ---- controls
    c1, c2, c3 = st.columns([1, 1, 2])

    with c1:
        use_llm = st.checkbox("Use LLM (experimental)", value=False)
    with c2:
        llm_model = st.selectbox("LLM model", ["gemini-2.5-flash"], index=0, disabled=not use_llm)
    with c3:
        st.caption("Policy is always generated first (rule-based or LLM), then cleaning runs.")

    # ---- Run cleaning (single button)
    if st.button("🧽 Run cleaning", type="primary", use_container_width=True):
        try:
            with st.spinner("Running on backend…"):
                # 1) ALWAYS generate policy (rule_based or llm)
                mode = "llm" if use_llm else "rule_based"
                pol = suggest_policy(dataset_id, mode=mode, llm_model=llm_model)
                st.session_state[f"policy_{dataset_id}"] = pol

                # 2) run cleaning
                run_id = run_cleaning(dataset_id, use_llm=use_llm, llm_model=llm_model)
                report = get_run_report(run_id)

            st.success(f"Cleaning finished ✅ (run_id={run_id})")

            # save into session for other tabs
            st.session_state[f"run_{dataset_id}"] = run_id
            st.session_state[f"report_{dataset_id}"] = report

        except Exception as e:
            st.error(f"Cleaning failed: {e}")

    # ---- Show policy (always when exists)
    pol = st.session_state.get(f"policy_{dataset_id}")
    if pol:
        plan = pol.get("policy", {}) or {}
        st.subheader("🧠 Cleaning plan (Policy)")
        st.metric("Source", str(pol.get("source", plan.get("source", "unknown"))))

        enabled_steps = plan.get("enabled_steps", {}) or {}
        params = plan.get("params", {}) or {}
        notes = pol.get("notes") or plan.get("notes") or []

        if enabled_steps:
            steps_df = (
                pd.DataFrame([{"step": k, "enabled": bool(v)} for k, v in enabled_steps.items()])
                .sort_values("step")
            )
            st.dataframe(steps_df, width="stretch", hide_index=True)

        if params:
            params_df = pd.DataFrame([{"param": k, "value": params[k]} for k in sorted(params.keys())])
            st.dataframe(params_df, width="stretch", hide_index=True)

        if notes:
            st.write("Notes")
            for n in notes:
                st.write(f"- {n}")

        st.divider()

    # ---- show results if already cleaned
    run_id = st.session_state.get(f"run_{dataset_id}")
    report = st.session_state.get(f"report_{dataset_id}")

    if report:
        st.subheader("Execution summary")
        s1, s2, s3, s4 = st.columns(4)
        with s1:
            st.metric("Rows (before)", report.get("rows_before"))
        with s2:
            st.metric("Cols (before)", report.get("cols_before"))
        with s3:
            st.metric("Rows (after)", report.get("rows_after"))
        with s4:
            st.metric("Cols (after)", report.get("cols_after"))

        st.subheader("Cleaned preview (current dataset)")
        preview_after = get_preview(dataset_id, rows=30)
        st.dataframe(pd.DataFrame(preview_after["rows"]), width="stretch")

        st.subheader("Download cleaned dataset")
        if run_id:
            cleaned_bytes = download_artifact(run_id, "cleaned.xlsx")
            st.download_button(
                "Download cleaned Excel",
                cleaned_bytes,
                file_name=f"{file_name.rsplit('.',1)[0]}__{sheet_meta['sheet_name']}__clean.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key=f"dl_clean_tab2_{run_id}",
            )

            report_bytes = download_artifact(run_id, "report.json")
            st.download_button(
                "Download report.json",
                report_bytes,
                file_name=f"{file_name.rsplit('.',1)[0]}__{sheet_meta['sheet_name']}__report.json",
                mime="application/json",
                use_container_width=True,
                key=f"dl_report_tab2_{run_id}",
            )

        return run_id, report

    return None, None