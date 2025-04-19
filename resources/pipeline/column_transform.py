import dask.dataframe as dd
import pandas as pd

def column_transform(dataframe, columns):
    """Apply per-column extraction, filtering, and transformation."""
    new_columns = {}
    sort_instructions = []
    groupby_cols = None
    aggs = {}
    agg_col_to_source = {}
    unique_count_aggs = {}

    for col in columns:
        col_name = col["name"]
        col_type = col.get("type")
        # Extraction (e.g., split)
        if col_type == "split":
            dataframe[col_name] = dataframe[col["column"]].str.split(col["on"]).str[col["select"]]
        elif col_type == "count":
            groupby_cols = col.get("group")
            aggs[col_name] = (col.get("column"), "count")
            agg_col_to_source[col_name] = col.get("column")
        elif col_type == "unique count":
            groupby_cols = col.get("group")
            unique_count_aggs[col_name] = col.get("column")
        # Add more extraction types as needed

        # Per-column filters
        if "filters" in col:
            for f in col["filters"]:
                kind = f["type"]
                value = f["value"]
                if kind == "below":
                    dataframe[col_name] = dd.to_numeric(dataframe[col_name], errors="coerce")
                    dataframe = dataframe[dataframe[col_name] < value]
                elif kind == "above":
                    dataframe[col_name] = dd.to_numeric(dataframe[col_name], errors="coerce")
                    dataframe = dataframe[dataframe[col_name] > value]
                elif kind == "contains":
                    dataframe = dataframe[dataframe[col_name].astype(str).str.contains(value, na=False)]
        new_columns[col_name] = True

    return dataframe, new_columns, groupby_cols, aggs, agg_col_to_source, unique_count_aggs
