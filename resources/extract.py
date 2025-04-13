import dask.dataframe as dd
import pandas as pd
import os

def apply_filters(dataframe, filters):
    for filter in filters:
        if filter["type"] == "above":
            dataframe = dataframe[dataframe[filter["column"]] > filter["value"]]
    return dataframe

def group_columns(dataframe, columns):
    return dataframe.groupby(columns).size().reset_index()

def apply_additions(dataframe, additions):
    for addition in additions:
        if addition["type"] == "sum":
            dataframe = dataframe.assign(
                **{addition["name"]: lambda df: df.groupby(addition["group"])[addition["column"]].transform('sum')}
            )
    return dataframe

def rename_columns(dataframe, renames):
    for rename in renames:
        dataframe = dataframe.rename(columns={rename["column"]: rename["new name"]})
    return dataframe

def drop_columns(dataframe, drop_cols):
    return dataframe.drop(columns=drop_cols)

def save_table(df, output_path):
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save as CSV
    csv_path = output_path + ".csv"
    df.to_csv(csv_path, index=False)

    # Save as Excel
    excel_path = output_path + ".xlsx"
    df.to_excel(excel_path, index=False)

    # Save as Markdown
    md_path = output_path + ".md"
    with open(md_path, 'w') as md_file:
        # Write header
        header = '| ' + ' | '.join(df.columns) + ' |\n'
        separator = '| ' + ' | '.join(['---'] * len(df.columns)) + ' |\n'
        md_file.write(header)
        md_file.write(separator)

        # Write each row
        for _, row in df.iterrows():
            row_str = '| ' + ' | '.join(str(cell) for cell in row) + ' |\n'
            md_file.write(row_str)

def extract(output, tables, blocksize):
    for folder in os.listdir(output + "/parquets/"):                                            # loop over folder in output + "/parquets")
        dataframe = dd.read_parquet(output + "/parquets/" + folder, blocksize=blocksize)        # read each parquet folder
        for table in tables:                                                                    # loop over tables
            df = dataframe
            df = apply_filters(df, table["filters"])
            df.compute()
            df = group_columns(df, table["columns"])
            df = apply_additions(df, table["additions"])
            df = rename_columns(df, table["rename columns"])
            df = drop_columns(df, table["drop columns"])
            save_table(df, output + "/tables/")