def apply_additions(dataframe, additions):
    for addition in additions:
        if addition["type"] == "count":
            dataframe = dataframe.groupby(addition["group"]).size().reset_index()
            dataframe = dataframe.rename(columns={0: addition["name"]})
        if addition["type"] == "sum":
            dataframe = dataframe.assign(
                **{addition["name"]: lambda df: df.groupby(addition["group"])[addition["column"]].transform('sum')}
            )
    return dataframe