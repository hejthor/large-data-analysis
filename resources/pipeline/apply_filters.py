import dask.dataframe as dd

def apply_filters(dataframe, filters):
    for filter in filters:
        kind = filter["type"]
        column = filter["column"]
        value = filter["value"]

        if kind == "below":
            dataframe[column] = dd.to_numeric(dataframe[column], errors="coerce")
            dataframe = dataframe[dataframe[column] < value]
        if kind == "above":
            dataframe[column] = dd.to_numeric(dataframe[column], errors="coerce")
            dataframe = dataframe[dataframe[column] > value]
        if kind == "contains":
            dataframe = dataframe[dataframe[column].astype(str).str.contains(value, na=False)]
    return dataframe