from __future__ import annotations

import pandas as pd
import streamlit as st


def _kv_table(d: dict, key_name: str, value_name: str) -> pd.DataFrame:
    d = d or {}
    return pd.DataFrame([{key_name: k, value_name: v} for k, v in d.items()])


def _render_profile_block(title: str, profile: dict):
    profile = profile or {}

    st.subheader(title)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Dataset type", str(profile.get("dataset_type")))
    with c2:
        st.metric("Has time index", bool(profile.get("has_time_index")))
    with c3:
        st.metric("Time column", str(profile.get("time_column")))

    st.write("**Column groups (counts)**")
    counts = profile.get("counts") or profile.get("column_groups") or {}
    st.dataframe(_kv_table(counts, "group", "count"), width="stretch", hide_index=True)

    st.write("**Top missing columns (%)**")
    miss = (profile.get("missingness") or {}).get("top_missing_columns") or profile.get("top_missing_columns") or {}
    miss_df = _kv_table(miss, "column", "missing_%").sort_values("missing_%", ascending=False)
    st.dataframe(miss_df, width="stretch", hide_index=True)

    st.write("**Top correlations (abs)**")
    corr = (profile.get("correlation") or {}).get("top_abs_pairs") or []
    if corr:
        st.dataframe(pd.DataFrame(corr), width="stretch", hide_index=True)
    else:
        st.info("No correlation pairs (need >= 2 numeric columns).")

    st.write("**Skewness (top abs)**")
    skew = (profile.get("skewness") or {}).get("top_abs_skewed") or profile.get("skewness_top_abs") or {}
    if skew:
        skew_df = _kv_table(skew, "column", "skew").assign(abs_skew=lambda x: x["skew"].abs())
        skew_df = skew_df.sort_values("abs_skew", ascending=False).drop(columns=["abs_skew"])
        st.dataframe(skew_df, width="stretch", hide_index=True)
    else:
        st.info("No skewness computed (need enough numeric data).")

    warnings = profile.get("warnings") or []
    if warnings:
        st.warning("\n".join(warnings))


def render_tab_signals(cleaning_report: dict):
    """
    Expects cleaning_report from run_cleaning_pipeline(), i.e. the same report
    you returned from cached_clean(). It should contain:
      - report["pre_profile"]
      - report["post_profile"]
    """
    pre = (cleaning_report or {}).get("pre_profile")
    post = (cleaning_report or {}).get("post_profile")

    st.header("📡 Signal Generation")

    if not pre and not post:
        st.error("No profiling data found in report. Make sure pipeline saves pre_profile/post_profile.")
        return

    tab_pre, tab_post = st.tabs(["Before cleaning", "After cleaning"])

    with tab_pre:
        _render_profile_block("Signals — BEFORE cleaning", pre)

    with tab_post:
        _render_profile_block("Signals — AFTER cleaning", post)

    # Optional: quick diff (nice to have)
    if pre and post:
        st.divider()
        st.subheader("🔁 Quick comparison")

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Rows", int(pre.get("n_rows", 0)), delta=int(post.get("n_rows", 0)) - int(pre.get("n_rows", 0)))
        with c2:
            st.metric("Cols", int(pre.get("n_cols", 0)), delta=int(post.get("n_cols", 0)) - int(pre.get("n_cols", 0)))
        with c3:
            pre_m = (pre.get("missingness") or {}).get("overall_missing_%")
            post_m = (post.get("missingness") or {}).get("overall_missing_%")
            if pre_m is not None and post_m is not None:
                st.metric("Overall missing %", float(pre_m), delta=float(post_m) - float(pre_m))