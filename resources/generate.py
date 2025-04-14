import argparse
import dask.dataframe as dd
from dask import delayed
import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

def generate(file_path, target_size_gb=40):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Estimate number of rows — assume ~100 bytes per row
    est_bytes_per_row = 100
    target_size_bytes = target_size_gb * 6 * 1024**3
    n_rows = target_size_bytes // est_bytes_per_row

    # Number of partitions — adjust depending on memory
    npartitions = 100
    rows_per_partition = int(n_rows // npartitions)

    # Define username list
    usernames = ['alice', 'bob', 'charlie', 'diana', 'eve', 'frank', 'grace', 'heidi']

    # Define date range
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2025, 1, 1)
    delta_days = (end_date - start_date).days

    def random_date():
        return (start_date + timedelta(days=random.randint(0, delta_days))).strftime('%Y-%m-%d')

    def random_username():
        return random.choice(usernames)

    def create_partition(n):
        return pd.DataFrame({
            'date': [random_date() for _ in range(n)],
            'username': [random_username() for _ in range(n)],
        })

    # Create Dask DataFrame from delayed partitions
    delayed_dfs = [delayed(create_partition)(rows_per_partition) for _ in range(npartitions)]
    ddf = dd.from_delayed(delayed_dfs)

    # Compute and write to a single CSV file
    ddf = ddf.repartition(npartitions=1)  # ensure we can write a single file
    df = ddf.compute()
    df.to_csv(file_path, index=False, sep=';')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random user data")
    parser.add_argument("--target", type=str, required=True, help="Path to target CSV file")
    args = parser.parse_args()
    generate(args.target)
