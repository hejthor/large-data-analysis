import dask.dataframe as dd
import os

def apply_filters(dataframe, filters):
    for filter in filters:
        if filter["type"] == "below":
            dataframe = dataframe[dataframe[filter["column"]] < filter["value"]]
    return dataframe

def group_columns(dataframe, columns):
    return dataframe[columns]

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

def save_table(df, output_path, name):
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save as CSV (Dask handles this without compute)
    csv_path = output_path + name + ".csv"
    df.to_csv(csv_path, single_file=True, index=False)

    # Convert to Pandas
    pdf = df.compute()

    # Save as Markdown
    md_path = output_path + name + ".md"
    with open(md_path, 'w') as md_file:
        header = '| ' + ' | '.join(str(col) for col in pdf.columns) + ' |\n'
        separator = '| ' + ' | '.join(['---'] * len(pdf.columns)) + ' |\n'
        md_file.write(header)
        md_file.write(separator)

        for _, row in pdf.iterrows():
            row_str = '| ' + ' | '.join(str(cell) for cell in row) + ' |\n'
            md_file.write(row_str)

def extract(output, tables, blocksize):
    for folder in os.listdir(output + "/parquets/"):                                            # loop over folder in output + "/parquets")
        dataframe = dd.read_parquet(output + "/parquets/" + folder, blocksize=blocksize)        # read each parquet folder
        for table in tables:                                                                    # loop over tables
            df = dataframe
            print(f"[PYTHON][extract.py] Applying filters for table: {table['name']}")
            df = apply_filters(df, table["filters"])
            df.compute()
            print(f"[PYTHON][extract.py] Grouping columns for table: {table['name']}")
            df = group_columns(df, table["columns"])
            print(f"[PYTHON][extract.py] Adding columns for table: {table['name']}")
            # df = apply_additions(df, table["additions"])
            print(f"[PYTHON][extract.py] Renaming columns for table: {table['name']}")
            df = rename_columns(df, table["rename columns"])
            print(f"[PYTHON][extract.py] Dropping columns for table: {table['name']}")
            df = drop_columns(df, table["drop columns"])
            print(f"[PYTHON][extract.py] Saving table: {table['name']}")
            save_table(df, output + "/tables/", table["name"])