import argparse
import dask.dataframe as dd
from dask import delayed
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def generate(file_path, rows, seed=None):
    print(f"Starting data generation: {rows} rows -> {file_path}")
    rows = int(rows)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    npartitions = max(1, rows // 1_000_000)
    rows_per_partition = int(rows // npartitions)
    print(f"Using {npartitions} partitions, {rows_per_partition} rows per partition.")

    usernames = np.array(['alice', 'bob', 'charlie', 'diana', 'eve', 'frank', 'grace', 'heidi'])
    start_date = np.datetime64('2010-01-01')
    end_date = np.datetime64('2025-01-01')
    delta_days = (end_date - start_date).astype(int)

    def create_partition(n, partition_seed=None):
        rng = np.random.default_rng(partition_seed)
        random_days = rng.integers(0, delta_days, size=n)
        dates = (start_date + random_days).astype('datetime64[D]').astype(str)
        users = rng.choice(usernames, size=n)
        return pd.DataFrame({
            'date': dates,
            'username': users,
        })

    partition_seeds = None
    if seed is not None:
        rng = np.random.default_rng(seed)
        partition_seeds = rng.integers(0, 1 << 32, size=npartitions)

    print("Creating delayed partitions...")
    delayed_dfs = [
        delayed(create_partition)(rows_per_partition, None if partition_seeds is None else partition_seeds[i])
        for i in range(npartitions)
    ]
    print("Building Dask DataFrame...")
    ddf = dd.from_delayed(delayed_dfs)

    print("Repartitioning to a single partition for CSV output...")
    ddf = ddf.repartition(npartitions=1)

    print("Computing final DataFrame (this may take a while)...")
    df = ddf.compute()
    print("Writing CSV file...")
    df.to_csv(file_path, index=False, sep=';')
    print(f"Data generation complete: {file_path}")

def main():
    parser = argparse.ArgumentParser(description="Generate a synthetic CSV dataset.")
    parser.add_argument('--file_path', type=str, required=True, help='Output CSV file path')
    parser.add_argument('--rows', type=int, required=True, help='Number of rows to generate')
    parser.add_argument('--seed', type=int, default=None, help='Optional random seed')
    args = parser.parse_args()
    generate(args.file_path, args.rows, args.seed)

if __name__ == "__main__":
    main()