import os
import dask

def save_table(dataframe, output_path, name):
    with dask.config.set(temporary_directory=output_path):
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

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
