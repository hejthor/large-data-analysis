import pandas as pd
from functools import reduce

def groupby_aggregate(dataframe, groupby_cols, aggs, unique_count_aggs):
    """Apply groupby and aggregation logic, supporting multiple groupings and merging results.
    aggs: dict of name -> (column, aggfunc, groupby_cols)
    unique_count_aggs: dict of name -> (column, groupby_cols)
    """
    # Collect all aggregations by their groupby columns

    # Build a mapping from groupby_cols tuple -> dict of {name: (column, aggfunc)}
    groupby_to_aggs = {}
    for name, (column, aggfunc, group_cols) in aggs.items():
        group_tuple = tuple(group_cols)
        if group_tuple not in groupby_to_aggs:
            groupby_to_aggs[group_tuple] = {}
        groupby_to_aggs[group_tuple][name] = (column, aggfunc)
    for name, (column, group_cols) in unique_count_aggs.items():
        group_tuple = tuple(group_cols)
        if group_tuple not in groupby_to_aggs:
            groupby_to_aggs[group_tuple] = {}
        groupby_to_aggs[group_tuple][name] = (column, 'nunique')

    results = []
    for group_tuple, agg_dict in groupby_to_aggs.items():
        groupby_cols_list = list(group_tuple)
        # Ensure columns are string
        for col in groupby_cols_list:
            dataframe[col] = dataframe[col].astype(str)
        # Separate standard aggs and nunique aggs
        standard_aggs = {k: v for k, v in agg_dict.items() if v[1] != 'nunique'}
        nunique_aggs = {k: v for k, v in agg_dict.items() if v[1] == 'nunique'}
        df = None
        if standard_aggs:
            df = dataframe.groupby(groupby_cols_list).agg(**{k: (col, func) for k, (col, func) in standard_aggs.items()}).reset_index()
        if nunique_aggs:
            nunique_cols = {k: v[0] for k, v in nunique_aggs.items()}
            nunique_dfs = []
            for out_col, src_col in nunique_cols.items():
                nunique_series = dataframe.groupby(groupby_cols_list)[src_col].nunique().rename(out_col).reset_index()
                nunique_dfs.append(nunique_series)
            # Merge all nunique results on groupby_cols_list
            if nunique_dfs:
                nunique_df = nunique_dfs[0]
                for other_df in nunique_dfs[1:]:
                    nunique_df = nunique_df.merge(other_df, on=groupby_cols_list, how='outer')
                if df is not None:
                    df = df.merge(nunique_df, on=groupby_cols_list, how='outer')
                else:
                    df = nunique_df
        results.append(df)

    # Merge all results on their shared columns
    if not results:
        return dataframe.copy()
    result_df = reduce(lambda left, right: left.merge(right, how='outer', on=list(set(left.columns) & set(right.columns))), results)
    return result_df
