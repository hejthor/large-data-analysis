import dask.dataframe as dd

def load_data(data_path, memory):
    """Load a Parquet file into a Dask DataFrame with specified memory blocksize."""
    return dd.read_parquet(data_path, blocksize=memory)
