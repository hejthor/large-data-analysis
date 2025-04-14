def extract_columns(dataframe, extracts):
    for extract in extracts:
        if extract["type"] == "split":
            dataframe[extract["name"]] = dataframe[extract["column"]].str.split(extract["on"]).str[extract["select"]]
    return dataframe