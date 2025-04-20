import os
import json
from pipeline import save_table, load_data, column_transform, groupby_aggregate, sort_select

def extract(output, table_path, memory, as_markdown=False, split_column=None, split_value=None):
    os.makedirs(output, exist_ok=True)
    table = json.load(open(table_path, 'r'))
    data_path = table.get("data")
    if not data_path:
        return None if as_markdown else None
    columns = table.get("columns", [])
    # Step 1: Load data
    dataframe = load_data(data_path, memory)
    # Optional: filter by split_column and split_value
    if split_column and split_value is not None and split_column in dataframe.columns:
        dataframe = dataframe[dataframe[split_column] == split_value]
    # Step 2: Column transform
    dataframe, new_columns, groupby_cols, aggs, agg_col_to_source, unique_count_aggs = column_transform(dataframe, columns)
    # Step 3: Groupby/aggregate
    if groupby_cols or aggs or unique_count_aggs:
        result_df = groupby_aggregate(dataframe, groupby_cols, aggs, unique_count_aggs)
        if result_df is not None:
            dataframe = result_df
            if groupby_cols:
                for gc in groupby_cols:
                    new_columns[gc] = True
            for out_col in aggs.keys():
                new_columns[out_col] = True
            for out_col in unique_count_aggs.keys():
                new_columns[out_col] = True
    # Step 4: Sort and select
    dataframe = sort_select(dataframe, columns, new_columns)

    # 5. Apply replacements after sorting
    for col in columns:
        col_name = col["name"]
        if "replacements" in col and col_name in dataframe.columns:
            dataframe[col_name] = dataframe[col_name].replace(col["replacements"])

    # 6. Save output or return markdown
    if as_markdown:
        return save_table(dataframe, output, table["name"], return_md=True)
    else:
        save_table(dataframe, output, table["name"])
