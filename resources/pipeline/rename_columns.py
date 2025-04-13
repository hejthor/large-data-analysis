def rename_columns(dataframe, renames):
    for rename in renames:
        dataframe = dataframe.rename(columns={rename["column"]: rename["new name"]})
    return dataframe