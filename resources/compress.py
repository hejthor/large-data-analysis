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
            dtype=str,
            blocksize=blocksize
        )

        print("[PYTHON][compress.py] Saving dataframe to Parquet")
        dataframe.to_parquet(
            output + "/parquets/" + os.path.splitext(os.path.basename("output/data.csv"))[0],
            compression='snappy'
        )