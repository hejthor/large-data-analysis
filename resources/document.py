import os
import json
import pypandoc
from extract import extract
import re
import pandas as pd
from pipeline import load_data

def document(output, document_path, memory):
    os.makedirs(output, exist_ok=True)
    with open(document_path, 'r') as f:
        document = json.load(f)
    template_file = document["template"]
    output_file_extension = os.path.splitext(template_file)[1][1:].lower()
    base_filename = os.path.splitext(os.path.basename(document_path))[0]
    doc_name = document.get("title", base_filename)
    
    # Set pagebreak marker based on template extension
    if template_file.lower().endswith('.odt'):
        pagebreak_marker = '<text:p text:style-name="Pagebreak"/>'
    elif template_file.lower().endswith('.docx'):
        pagebreak_marker = '<w:p><w:r><w:br w:type="page"/></w:r></w:p>'
    else:
        pagebreak_marker = ''
    
    metadata = {
        'author': document["author"],
        'title': document["title"],
        'toc-title': document["toc-title"],
        'lang': document["language"]
    }
    extra_args = [
        *[f'--metadata={key}={value}' for key, value in metadata.items()],
        f"--reference-doc={template_file}",
        "--standalone",
        "--table-of-contents"
    ]

    split_col = document.get("split")
    contents = document.get("contents", [])

    # If split is specified, determine unique values across all tables
    if split_col:
        unique_values = set()
        for item in contents:
            if item.get("type") == "table":
                # Load the table and get unique values in split_col
                table_path = item.get("content")
                try:
                    table_json = json.load(open(table_path, 'r'))
                    data_path = table_json.get("data")
                    if data_path:
                        df = load_data(data_path, memory).compute()
                        if split_col in df.columns:
                            unique_values.update(df[split_col].dropna().unique())
                except Exception as e:
                    print(f"[PYTHON][document.py] Error loading table for split: {e}")
        # For each unique value, generate a document
        for split_val in unique_values:
            safe_val = re.sub(r'[^a-zA-Z0-9_-]', '_', str(split_val))
            split_dir = os.path.join(output, safe_val)
            os.makedirs(split_dir, exist_ok=True)
            md_chunks = []
            for item in contents:
                t = item.get("type")
                c = item.get("content", "")
                if t == "section":
                    md_chunks.append(f"# {c}\n")
                elif t == "subsection":
                    md_chunks.append(f"## {c}\n")
                elif t == "paragraph":
                    md_chunks.append(f"{c}\n")
                elif t == "pagebreak":
                    md_chunks.append(f"{pagebreak_marker}\n")
                elif t == "table":
                    # Save CSV to split_dir
                    extract(split_dir, c, memory, as_markdown=False, split_column=split_col, split_value=split_val)
                    table_md = extract(output, c, memory, as_markdown=True, split_column=split_col, split_value=split_val)
                    if table_md and table_md.strip():
                        md_chunks.append(table_md + "\n")
                else:
                    md_chunks.append(f"{c}\n")
            markdown = '\n'.join(md_chunks)
            filename = f"{doc_name}.{output_file_extension}"
            output_file = os.path.join(split_dir, filename)
            try:
                pypandoc.convert_text(
                    markdown,
                    to=output_file_extension,
                    format='md',
                    outputfile=output_file,
                    extra_args=extra_args
                )
                print(f"[PYTHON][document.py] Converted split '{split_val}' to {output_file}")
            except Exception as e:
                print(f"[PYTHON][document.py] Error converting split '{split_val}': {e}")
    else:
        # No split, behave as before
        md_chunks = []
        for item in contents:
            t = item.get("type")
            c = item.get("content", "")
            if t == "section":
                md_chunks.append(f"# {c}\n")
            elif t == "subsection":
                md_chunks.append(f"## {c}\n")
            elif t == "paragraph":
                md_chunks.append(f"{c}\n")
            elif t == "pagebreak":
                md_chunks.append(f"{pagebreak_marker}\n")
            elif t == "table":
                extract(output, c, memory, as_markdown=False)
                table_md = extract(output, c, memory, as_markdown=True)
                if table_md:
                    md_chunks.append(table_md + "\n")
            else:
                md_chunks.append(f"{c}\n")
        markdown = '\n'.join(md_chunks)
        filename = f"{doc_name}.{output_file_extension}"
        output_file = os.path.join(output, filename)
        try:
            pypandoc.convert_text(
                markdown,
                to=output_file_extension,
                format='md',
                outputfile=output_file,
                extra_args=extra_args
            )
            print(f"[PYTHON][document.py] Converted JSON contents to {output_file}")
        except Exception as e:
            print(f"[PYTHON][document.py] Error converting JSON contents: {e}")