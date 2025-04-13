def apply_filters(dataframe, filters):
    for filter in filters:
        if filter["type"] == "below":
            dataframe = dataframe[dataframe[filter["column"]] < filter["value"]]
    return dataframe