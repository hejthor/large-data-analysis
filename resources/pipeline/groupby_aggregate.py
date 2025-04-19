import pandas as pd

def groupby_aggregate(dataframe, groupby_cols, aggs, unique_count_aggs):
    """Apply groupby and aggregation logic, including unique counts."""
    result_df = None
    if aggs and groupby_cols:
        agg_dict = {k: v for k, v in aggs.items()}
        result_df = dataframe.groupby(groupby_cols).agg(**agg_dict).reset_index()
    elif groupby_cols:
        result_df = dataframe[groupby_cols].drop_duplicates().copy()
    for out_col, src_col in unique_count_aggs.items():
        nunique_df = dataframe.groupby(groupby_cols)[src_col].nunique().reset_index().rename(columns={src_col: out_col})
        if result_df is None:
            result_df = nunique_df
        else:
            result_df = result_df.merge(nunique_df, on=groupby_cols, how="left")
    return result_df
