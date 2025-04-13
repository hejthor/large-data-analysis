import dask.dataframe as dd
import os

from pipeline import (
    apply_filters,
    group_columns,
    apply_additions,
    rename_columns,
    drop_columns,
    sort_columns,
    save_table
)

def extract(output, tables, blocksize):
    for folder in os.listdir(output + "/parquets/"):
        dataframe = dd.read_parquet(os.path.join(output, "parquets", folder), blocksize=blocksize)
        for table in tables:
            if "filters" in table:
                print(f"[PYTHON][extract.py] Applying filters for table: {table["name"]}")
                dataframe = apply_filters(dataframe, table["filters"])
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
            if "sorting" in table and table["sorting"]:
                print(f"[PYTHON][extract.py] Sorting columns for table: {table["name"]}")
                dataframe = sort_columns(dataframe, table["sorting"])
            print(f"[PYTHON][extract.py] Saving table: {table["name"]}")
            save_table(dataframe, os.path.join(output, "tables/"), table["name"])
