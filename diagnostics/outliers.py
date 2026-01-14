from __future__ import annotations
import streamlit as st

def render_outlier_report(outliers_df):
    st.subheader("Statistical Outliers")

    if outliers_df.empty:
        st.success("No statistical outliers detected.")
        return

    st.warning(
        f"Detected {len(outliers_df)} outlier counties — examine carefully."
    )
    st.dataframe(outliers_df, hide_index=True)
