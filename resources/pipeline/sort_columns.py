def sort_columns(dataframe, sorting_instructions):
    # Extract columns and directions from the sorting instructions
    sort_cols = [item["column"] for item in sorting_instructions]
    sort_dirs = [item["direction"] == "ascending" for item in sorting_instructions]

    # Apply sorting
    dataframe = dataframe.sort_values(by=sort_cols, ascending=sort_dirs)
    return dataframe