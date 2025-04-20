import os
import dask

def save_table(dataframe, output_path, name, split_col=None):
    import pandas as pd
    with dask.config.set(temporary_directory=output_path):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        if split_col is not None and split_col in dataframe.columns:
            # Compute to pandas once for splitting
            pdf = dataframe.compute() if not isinstance(dataframe, pd.DataFrame) else dataframe
            split_folder = os.path.join(output_path, name)
            os.makedirs(split_folder, exist_ok=True)
            for value in pdf[split_col].dropna().unique():
                sub_df = pdf[pdf[split_col] == value]
                safe_value = str(value).replace('/', '_').replace('\\', '_')
                csv_path = os.path.join(split_folder, f"{safe_value}.csv")
                md_path = os.path.join(split_folder, f"{safe_value}.md")
                sub_df.to_csv(csv_path, index=False, sep=';')
                # Markdown export
                with open(md_path, 'w') as md_file:
                    header = '| ' + ' | '.join(str(col) for col in sub_df.columns) + ' |\n'
                    separator = '| ' + ' | '.join(['---'] * len(sub_df.columns)) + ' |\n'
                    md_file.write(header)
                    md_file.write(separator)
                    for _, row in sub_df.iterrows():
                        row_str = '| ' + ' | '.join(str(cell) for cell in row) + ' |\n'
                        md_file.write(row_str)
        else:
            # Save as CSV
            csv_path = output_path + name + ".csv"
            dataframe.to_csv(csv_path, single_file=True, index=False, sep=';')
            # Convert to Pandas
            dataframe = dataframe.compute()
            # Save as Markdown
            md_path = output_path + name + ".md"
            with open(md_path, 'w') as md_file:
                header = '| ' + ' | '.join(str(col) for col in dataframe.columns) + ' |\n'
                separator = '| ' + ' | '.join(['---'] * len(dataframe.columns)) + ' |\n'
                md_file.write(header)
                md_file.write(separator)
                for _, row in dataframe.iterrows():
                    row_str = '| ' + ' | '.join(str(cell) for cell in row) + ' |\n'
                    md_file.write(row_str)
