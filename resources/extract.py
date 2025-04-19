import os
from pipeline import save_table, load_data, column_transform, groupby_aggregate, sort_select

def extract(output, tables, memory):
    for table in tables:
        data_path = table.get("data")
        if not data_path:
            continue
        columns = table.get("columns", [])
        # Step 1: Load data
        dataframe = load_data(data_path, memory)
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

        # 6. Save output
        save_table(dataframe, os.path.join(output, "tables/"), table["name"])
