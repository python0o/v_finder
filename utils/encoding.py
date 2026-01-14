# encoding.py (Handles multi-encoding CSV chunking for robust ingestion)
from __future__ import annotations

import pandas as pd

DEFAULT_ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")

def iter_csv_chunks(
    path: str,
    chunk_size: int,
    on_bad_lines: str = "skip",
    encodings=DEFAULT_ENCODINGS,
):
    last_err = None
    for enc in encodings:
        try:
            # Try with encoding_errors (Pandas 1.3+)
            try:
                return pd.read_csv(
                    path,
                    dtype=str,
                    chunksize=chunk_size,
                    encoding=enc,
                    encoding_errors="replace",  # Replace invalid chars
                    on_bad_lines=on_bad_lines,
                    low_memory=False,
                )
            except TypeError:  # Older Pandas without encoding_errors
                return pd.read_csv(
                    path,
                    dtype=str,
                    chunksize=chunk_size,
                    encoding=enc,
                    on_bad_lines=on_bad_lines,
                    low_memory=False,
                )
        except Exception as e:
            last_err = e
            continue
    raise last_err  # Raise the last error if all encodings fail