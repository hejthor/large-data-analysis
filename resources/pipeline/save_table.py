import os
import dask

def save_table(dataframe, output_path, name, return_md=False):
    import pandas as pd
    with dask.config.set(temporary_directory=output_path):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save as CSV (only if not returning md)
        if not return_md:
            csv_path = os.path.join(output_path, name + ".csv")
            dataframe.to_csv(csv_path, single_file=True, index=False, sep=';')
        # Convert to Pandas for markdown generation if needed
        if return_md:
            df = dataframe.compute()
            header = '| ' + ' | '.join(str(col) for col in df.columns) + ' |\n'
            separator = '| ' + ' | '.join(['---'] * len(df.columns)) + ' |\n'
            rows = []
            for _, row in df.iterrows():
                row_str = '| ' + ' | '.join(str(cell) for cell in row) + ' |\n'
                rows.append(row_str)
            md = header + separator + ''.join(rows)
            return md
        # Only save CSV, never write .md file
