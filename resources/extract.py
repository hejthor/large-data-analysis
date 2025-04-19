import dask.dataframe as dd
import os

from pipeline import (
    save_table
)

def extract(output, tables, memory):
    for table in tables:
        # Use the new 'data' key to load the correct parquet file
        data_path = table.get("data")
        if not data_path:
            continue
        dataframe = dd.read_parquet(data_path, blocksize=memory)
        columns = table.get("columns", [])
        new_columns = {}
        sort_instructions = []
        groupby_cols = None
        aggs = {}
        agg_col_to_source = {}
        unique_count_aggs = {}

        # 1. Extract columns as specified, apply per-column extraction, filters, replacements, sorting
        import pandas as pd
        for col in columns:
            col_name = col["name"]
            col_type = col.get("type")
            # Extraction (e.g., split)
            if col_type == "split":
                dataframe[col_name] = dataframe[col["column"]].str.split(col["on"]).str[col["select"]]
            elif col_type == "count":
                # Add count aggregation
                groupby_cols = col.get("group")
                aggs[col_name] = (col.get("column"), "count")
                agg_col_to_source[col_name] = col.get("column")
            elif col_type == "unique count":
                # Collect unique count aggregations separately
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

            # Per-column sorting (before replacements) ONLY for columns that exist now (not aggregations)
            # (REMOVED: see global sort after all transformations)

            # Do not apply sorting or replacements here; handle them after all transformations/aggregations
            new_columns[col_name] = True

        # 2. Groupby/aggregation logic if present
        import pandas as pd
        result_df = None
        if aggs and groupby_cols:
            # Build a dict for dask's groupby.agg (only string aggregations supported by Dask)
            agg_dict = {k: v for k, v in aggs.items()}
            result_df = dataframe.groupby(groupby_cols).agg(**agg_dict).reset_index()
        elif groupby_cols:
            # If only unique counts are present, start from a groupby object with group columns
            result_df = dataframe[groupby_cols].drop_duplicates().copy()
        # Merge in unique counts
        for out_col, src_col in unique_count_aggs.items():
            nunique_df = dataframe.groupby(groupby_cols)[src_col].nunique().reset_index().rename(columns={src_col: out_col})
            if result_df is None:
                result_df = nunique_df
            else:
                result_df = result_df.merge(nunique_df, on=groupby_cols, how="left")
        if result_df is not None:
            dataframe = result_df
            for gc in groupby_cols:
                new_columns[gc] = True
            for out_col in aggs.keys():
                new_columns[out_col] = True
            for out_col in unique_count_aggs.keys():
                new_columns[out_col] = True

        # 3. Select only the relevant columns
        dataframe = dataframe[[col for col in new_columns]]

        # 4. Sorting after all transformations/aggregations
        sort_cols = []
        sort_dirs = []
        for col in columns:
            col_name = col["name"]
            if "sorting" in col and col_name in dataframe.columns:
                sort_cols.append(col_name)
                sort_dirs.append(col["sorting"] == "ascending")
        if sort_cols:
            dataframe = dataframe.sort_values(by=sort_cols, ascending=sort_dirs)

        # 5. Apply replacements after sorting
        for col in columns:
            col_name = col["name"]
            if "replacements" in col and col_name in dataframe.columns:
                dataframe[col_name] = dataframe[col_name].replace(col["replacements"])

        # 6. Save output
        save_table(dataframe, os.path.join(output, "tables/"), table["name"])
