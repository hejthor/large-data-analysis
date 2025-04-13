import dask.dataframe as dd
import os

def compress(output, data, blocksize):
    for source in data:
        print(f"[PYTHON][compress.py] Reading {source['path']} into dataframe")
        dataframe = dd.read_csv(
            source["path"],
            encoding=source["encoding"],
            delimiter=source["delimiter"],
            on_bad_lines='skip',
            low_memory=False,
            blocksize=blocksize
        )

        for col in dataframe.columns:
            if dataframe[col].nunique().compute() < 1000:
                dataframe[col] = dataframe[col].astype("category")

        print("[PYTHON][compress.py] Saving dataframe to Parquet")
        dataframe.to_parquet(
            output + "/parquets/" + os.path.splitext(os.path.basename("output/data.csv"))[0],
            engine='pyarrow',
            compression='brotli',
            use_dictionary=True,
            write_metadata_file=False
        )