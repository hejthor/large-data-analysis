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
        # Extraction (e.g., split, week number)
        if col_type == "split":
            dataframe[col_name] = dataframe[col["column"]].str.split(col["on"]).str[col["select"]]
        elif col_type == "week number":
            # Convert to datetime then extract ISO week number
            dataframe[col_name] = dd.to_datetime(dataframe[col["column"]], errors="coerce").dt.isocalendar().week
        elif col_type == "count":
            # Always use the 'name' values from the columns array for grouping
            groupby_cols = col.get("group")
            if groupby_cols:
                groupby_cols = [g for g in groupby_cols]
            aggs[col_name] = (col.get("column"), "count", tuple(groupby_cols or []))
            agg_col_to_source[col_name] = col.get("column")
        elif col_type == "unique count":
            groupby_cols = col.get("group")
            if groupby_cols:
                groupby_cols = [g for g in groupby_cols]
            unique_count_aggs[col_name] = (col.get("column"), tuple(groupby_cols or []))
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

    # Ensure all groupby columns exist in the dataframe (by their 'name' as per columns array)
    all_groupby_cols = set()
    for col in columns:
        if "group" in col and col["group"]:
            all_groupby_cols.update(col["group"])
    # Map 'name' to 'column' for all columns
    name_to_column = {col["name"]: col["column"] for col in columns if "column" in col}
    for group_col in all_groupby_cols:
        if group_col not in dataframe.columns and group_col in name_to_column:
            dataframe[group_col] = dataframe[name_to_column[group_col]]
    return dataframe, new_columns, groupby_cols, aggs, agg_col_to_source, unique_count_aggs
