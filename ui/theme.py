"""
ui/theme.py

Global UI theme controller for V_FINDER.
Modes: Basic, Enhanced, Vivid
"""

import streamlit as st


THEMES = {
    "Basic": {
        "--bg": "#0e1117",
        "--panel": "#111827",
        "--text": "#e5e7eb",
        "--muted": "#9ca3af",
        "--accent": "#4b5563",
        "--good": "#10b981",
        "--warn": "#f59e0b",
        "--bad": "#ef4444",
        "--border": "#1f2937",
    },
    "Enhanced": {
        "--bg": "#0b1220",
        "--panel": "#0f172a",
        "--text": "#e5e7eb",
        "--muted": "#94a3b8",
        "--accent": "#38bdf8",
        "--good": "#22c55e",
        "--warn": "#fbbf24",
        "--bad": "#f87171",
        "--border": "#1e293b",
    },
    "Vivid": {
        "--bg": "#05060a",
        "--panel": "#0a0f1f",
        "--text": "#f8fafc",
        "--muted": "#94a3b8",
        "--accent": "#22d3ee",
        "--good": "#00ff9c",
        "--warn": "#ffd166",
        "--bad": "#ff4d6d",
        "--border": "#1f2a44",
    },
}


def apply_theme(mode: str):
    colors = THEMES.get(mode, THEMES["Enhanced"])

    css = f"""
    <style>
    :root {{
        {chr(10).join([f"{k}: {v};" for k, v in colors.items()])}
    }}

    html, body, .stApp {{
        background-color: var(--bg);
        color: var(--text);
    }}

    section[data-testid="stSidebar"] {{
        background-color: var(--panel);
        border-right: 1px solid var(--border);
    }}

    div[data-testid="stMetric"] {{
        background-color: var(--panel);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 10px;
    }}

    .stDataFrame {{
        background-color: var(--panel);
        border: 1px solid var(--border);
    }}

    .stAlert {{
        border-radius: 6px;
        border: 1px solid var(--border);
    }}

    .stAlert[data-baseweb="notification"][kind="warning"] {{
        background-color: rgba(251, 191, 36, 0.12);
    }}

    .stAlert[data-baseweb="notification"][kind="error"] {{
        background-color: rgba(239, 68, 68, 0.12);
    }}

    h1, h2, h3 {{
        color: var(--text);
    }}

    hr {{
        border-color: var(--border);
    }}
    </style>
    """

    st.markdown(css, unsafe_allow_html=True)
