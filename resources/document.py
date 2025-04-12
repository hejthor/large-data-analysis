import os
import pypandoc

def document(output, documents):
    for document in documents:
        input_file = document["source"]
        template_file = document["template"]
        input_file_extension = os.path.splitext(input_file)[1][1:].lower()
        output_file_extension = os.path.splitext(template_file)[1][1:].lower()
        
        # Extract the filename from the input file and join it with the output path
        filename = os.path.basename(input_file).replace(input_file_extension, output_file_extension)
        output_file = os.path.join(output, filename)

        # Specify metadata for the conversion
        pagebreak = "`<w:p><w:r><w:br w:type='page'/></r></w:p>`{=openxml}"
        metadata = {
            'author': document["author"] + pagebreak,
            'title': document["title"],
            'toc-title': document["toc-title"],
            'lang': document["language"]
        }

        # Define extra arguments, including metadata with an inline loop
        extra_args = [
            "--standalone",
            "--table-of-contents",
            "--lua-filter=resources/filters/include-files.lua",
            "--lua-filter=resources/filters/include-code-files.lua",
            "--lua-filter=resources/filters/pagebreak.lua",
            f"--template={template_file}",
            *[f'--metadata={key}={value}' for key, value in metadata.items()]
        ]
        
        try:
            pypandoc.convert_file(
                source_file=input_file,
                to=output_file_extension,
                format=input_file_extension,
                outputfile=output_file,
                extra_args=extra_args
            )
            print(f"[PYTHON][document.py] Converted {input_file} to {output_file}")
        except Exception as e:
            print(f"[PYTHON][document.py] Error converting {input_file}: {e}")