import dask.dataframe as dd
import dask
import os
import gc

def compress(output, data, memory):
    os.makedirs(output, exist_ok=True)
    with dask.config.set(temporary_directory=output):
        for source in data:
            print(f"[PYTHON][compress.py] Reading {source['path']} into dataframe")
            dataframe = dd.read_csv(
                source["path"],
                encoding=source["encoding"],
                delimiter=source["delimiter"],
                on_bad_lines='skip',
                low_memory=False,
                blocksize=memory
            )
            gc.collect()

            for col in dataframe.columns:
                if dataframe[col].nunique().compute() < 1000:
                    dataframe[col] = dataframe[col].astype("category")
            gc.collect()

            print("[PYTHON][compress.py] Saving dataframe to Parquet")
            dataframe.to_parquet(
                os.path.join(output, os.path.splitext(os.path.basename("output/data.csv"))[0]),
                engine='pyarrow',
                compression='brotli',
                use_dictionary=True,
                write_metadata_file=False,
                row_group_size=100000
            )
            gc.collect()
