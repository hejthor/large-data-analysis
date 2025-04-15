import dask.dataframe as dd
import os

from pipeline import (
    apply_filters,
    extract_columns,
    group_columns,
    apply_additions,
    rename_columns,
    drop_columns,
    sort_columns,
    save_table,
    replace_values
)

def extract(output, tables, memory):
    for folder in os.listdir(output + "/parquets/"):
        dataframe = dd.read_parquet(os.path.join(output, "parquets", folder), blocksize=memory)
        for table in tables:
            if "pre filters" in table:
                print(f"[PYTHON][extract.py] Applying filters for table: {table["name"]}")
                dataframe = apply_filters(dataframe, table["pre filters"])
            if "extract" in table:
                print(f"[PYTHON][extract.py] Extract columns for table: {table["name"]}")
                dataframe = extract_columns(dataframe, table["extract"])
            if "columns" in table:
                print(f"[PYTHON][extract.py] Grouping columns for table: {table["name"]}")
                dataframe = group_columns(dataframe, table["columns"])
            if "additions" in table:
                print(f"[PYTHON][extract.py] Adding columns for table: {table["name"]}")
                dataframe = apply_additions(dataframe, table["additions"])
            if "rename columns" in table:
                print(f"[PYTHON][extract.py] Renaming columns for table: {table["name"]}")
                dataframe = rename_columns(dataframe, table["rename columns"])
            if "drop columns" in table:
                print(f"[PYTHON][extract.py] Dropping columns for table: {table["name"]}")
                dataframe = drop_columns(dataframe, table["drop columns"])
            if "sorting" in table:
                print(f"[PYTHON][extract.py] Sorting columns for table: {table["name"]}")
                dataframe = sort_columns(dataframe, table["sorting"])
            if "post filters" in table:
                print(f"[PYTHON][extract.py] Applying filters for table: {table["name"]}")
                dataframe = apply_filters(dataframe, table["post filters"])
            if "replacements" in table:
                print(f"[PYTHON][extract.py] Replacing values for table: {table["name"]}")
                dataframe = replace_values(dataframe, table["replacements"])
            print(f"[PYTHON][extract.py] Saving table: {table["name"]}")
            save_table(dataframe, os.path.join(output, "tables/"), table["name"])
