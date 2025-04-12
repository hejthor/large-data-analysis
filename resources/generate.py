import argparse
import dask.dataframe as dd
from dask import delayed
import pandas as pd
import numpy as np
import os
import random
import string

def generate(file_path, target_size_gb=15):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Estimate number of rows — assume ~100 bytes per row
    est_bytes_per_row = 100
    target_size_bytes = target_size_gb * 2 * 1024**3
    n_rows = target_size_bytes // est_bytes_per_row

    # Number of partitions — adjust depending on memory
    npartitions = 100

    def random_strings(n, length=10):
        return [''.join(random.choices(string.ascii_letters, k=length)) for _ in range(n)]

    def create_partition(n):
        return pd.DataFrame({
            'id': np.arange(n),
            'name': random_strings(n),
            'age': np.random.randint(18, 99, size=n),
            'email': [f'user{i}@example.com' for i in range(n)],
            'balance': np.random.uniform(1000, 100000, size=n).round(2)
        })

    # Rows per partition
    rows_per_partition = int(n_rows // npartitions)

    # Create Dask DataFrame from delayed partitions
    delayed_dfs = [delayed(create_partition)(rows_per_partition) for _ in range(npartitions)]
    ddf = dd.from_delayed(delayed_dfs)

    # Compute and write to a single CSV file
    ddf = ddf.repartition(npartitions=1)  # ensure we can write a single file
    df = ddf.compute()
    df.to_csv(file_path, index=False, sep=';')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--target", type=str, required=True, help="Path to target")
    args = parser.parse_args()
    generate(args.target)