def sort_select(dataframe, columns, new_columns):
    """Sort the dataframe and select relevant columns."""
    # Select only the relevant columns that exist in the dataframe
    dataframe = dataframe[[col for col in new_columns if col in dataframe.columns]]

    # Sorting after all transformations/aggregations
    sort_cols = []
    sort_dirs = []
    for col in columns:
        col_name = col["name"]
        if "sorting" in col and col_name in dataframe.columns:
            sort_cols.append(col_name)
            sort_dirs.append(col["sorting"] == "ascending")
    if sort_cols:
        dataframe = dataframe.sort_values(by=sort_cols, ascending=sort_dirs)
    return dataframe
