import os
import json
from extract import extract
import re
from pipeline import load_data

def generate_markdown(contents, pagebreak_marker, memory, output, split_dir=None, split_val=None):
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
            if split_dir and split_val is not None:
                extract(split_dir, c, memory, as_markdown=False, split_column=None, split_value=split_val)
                table_md = extract(split_dir, c, memory, as_markdown=True, split_column=None, split_value=split_val)
            else:
                extract(output, c, memory, as_markdown=False)
                table_md = extract(output, c, memory, as_markdown=True)
            if table_md:
                md_chunks.append(table_md + "\n")
        else:
            md_chunks.append(f"{c}\n")
    return '\n'.join(md_chunks)

def convert_with_pandoc(markdown, output_file_extension, output_file, extra_args):
    import pypandoc
    try:
        pypandoc.convert_text(
            markdown,
            to=output_file_extension,
            format='md',
            outputfile=output_file,
            extra_args=extra_args
        )
        print(f"[PYTHON][document.py] Converted to {output_file}")
    except Exception as e:
        print(f"[PYTHON][document.py] Error converting: {e}")

def get_unique_split_values(contents, split_col, memory):
    unique_values = set()
    for item in contents:
        if item.get("type") == "table":
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
    return unique_values

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

    # Helper function usage below
    
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

    if split_col:
        unique_values = get_unique_split_values(contents, split_col, memory)
        for split_val in unique_values:
            safe_val = re.sub(r'[^a-zA-Z0-9_-]', '_', str(split_val))
            split_dir = os.path.join(output, safe_val)
            os.makedirs(split_dir, exist_ok=True)
            markdown = generate_markdown(contents, pagebreak_marker, memory, output, split_dir=split_dir, split_val=split_val)
            filename = f"{doc_name}.{output_file_extension}"
            output_file = os.path.join(split_dir, filename)
            convert_with_pandoc(markdown, output_file_extension, output_file, extra_args)
            print(f"[PYTHON][document.py] Converted split '{split_val}' to {output_file}")
    else:
        markdown = generate_markdown(contents, pagebreak_marker, memory, output)
        filename = f"{doc_name}.{output_file_extension}"
        output_file = os.path.join(output, filename)
        convert_with_pandoc(markdown, output_file_extension, output_file, extra_args)
        print(f"[PYTHON][document.py] Converted to {output_file}")