import streamlit as st
import json
import os
from pathlib import Path
import subprocess

st.set_page_config(page_title="Large Data Analysis", layout="wide")
st.title("Large Data Analysis Streamlit UI (app.py mirror)")

st.markdown("""
This UI mirrors app.py: all .json files in input/documents and input/tables are editable below. Any change triggers an immediate pipeline re-run.
""")

def edit_json(label, file_path):
    with open(file_path, "r") as f:
        file_data = f.read()
    edited = st.text_area(label, value=file_data, height=300, key=file_path)
    valid = True
    try:
        json.loads(edited)
    except Exception as e:
        st.error(f"Invalid JSON in {file_path}: {e}")
        valid = False
    return edited, valid

col1, col2 = st.columns(2)

with col1:
    st.header("Table Form")
    # Initialize with empty/default table (no prefill from any JSON)
    if "table" not in st.session_state:
        st.session_state["table"] = {
            "data": "",
            "name": "",
            "columns": []
        }
    table_data = st.session_state["table"]
    data_val = st.text_input("Data directory", value=table_data.get("data", ""), key="data")
    name_val = st.text_input("Table name", value=table_data.get("name", ""), key="name")
    st.markdown("**Columns**:")
    columns = table_data.get("columns", [])
    new_columns = []
    for i, col in enumerate(columns):
        c1, c2, c3 = st.columns([3, 3, 1])
        with c1:
            col_name = st.text_input(f"Column name #{i+1}", value=col.get("name", ""), key=f"col_name_{i}")
        with c2:
            type_options = ["string", "int", "float", "split", "count", "unique count"]
            col_type_value = col.get("type", "string")
            col_type_index = type_options.index(col_type_value) if col_type_value in type_options else 0
            col_type = st.selectbox(f"Type #{i+1}", type_options, index=col_type_index, key=f"col_type_{i}")
        with c3:
            if st.button("Delete", key=f"delete_col_{i}"):
                columns.pop(i)
                st.session_state["table"]["columns"] = columns
                st.experimental_rerun()
        new_col = {"name": col_name, "type": col_type}
        # Conditional fields for 'split' type
        if col_type == "split":
            c4, c5 = st.columns(2)
            with c4:
                new_col["on"] = st.text_input(f"On #{i+1}", value=col.get("on", ""), key=f"col_on_{i}")
            with c5:
                new_col["select"] = st.text_input(f"Select #{i+1}", value=col.get("select", ""), key=f"col_select_{i}")
        # Conditional fields for 'count' or 'unique count' type
        if col_type in ["count", "unique count"]:
            new_col["group"] = st.text_input(f"Group #{i+1}", value=col.get("group", ""), key=f"col_group_{i}")
        # Optional fields for all types
        new_col["replacements"] = st.text_area(f"Replacements #{i+1}", value=col.get("replacements", ""), key=f"col_replacements_{i}")
        new_col["filters"] = st.text_area(f"Filters #{i+1}", value=col.get("filters", ""), key=f"col_filters_{i}")
        new_columns.append(new_col)
    if st.button("Add Column", key="add_column_table"):
        columns.append({"name": "", "type": "string"})
    st.session_state["table"]["columns"] = new_columns

with col2:
    st.header("Document Form")
    # Ensure all Document Form keys are initialized in session_state
    if "template" not in st.session_state:
        st.session_state["template"] = ""
    if "language" not in st.session_state:
        st.session_state["language"] = ""
    if "title" not in st.session_state:
        st.session_state["title"] = ""
    if "author" not in st.session_state:
        st.session_state["author"] = ""
    if "cover" not in st.session_state:
        st.session_state["cover"] = ""
    if "toc_title" not in st.session_state:
        st.session_state["toc_title"] = "Contents"
    if "split" not in st.session_state:
        st.session_state["split"] = ""
    if "contents" not in st.session_state:
        st.session_state["contents"] = []
    c1, c2, c3 = st.columns(3)
    with c1:
        template = st.text_input("Template file", value=st.session_state["template"], key="template")
    with c2:
        language_options = [
            "en", "da", "de", "fr", "es", "zh", "ru", "ja", "pt", "it", "nl", "sv", "no", "fi", "pl", "cs"
        ]
        if st.session_state["language"] not in language_options:
            st.session_state["language"] = "en"
        language_index = language_options.index(st.session_state["language"])
        language = st.selectbox(
            "Language",
            language_options,
            index=language_index,
            key="language"
        )
    with c3:
        title = st.text_input("Title", value=st.session_state["title"], key="title")
    c4, c5, c6 = st.columns(3)
    with c4:
        author = st.text_input("Author", value=st.session_state["author"], key="author")
    with c5:
        cover = st.text_input("Cover file", value=st.session_state["cover"], key="cover")
    with c6:
        toc_title = st.text_input("TOC Title", value=st.session_state["toc_title"], key="toc")
    split = st.text_input("Split", value=st.session_state["split"], key="split")
    st.markdown("**Contents**:")
    contents = st.session_state["contents"]
    new_contents = []
    for i, item in enumerate(contents):
        c1, c2, c3 = st.columns([2,2,2])
        with c1:
            type_val = st.selectbox(f"Type #{i+1}", ["section", "subsection", "paragraph", "pagebreak", "table"], index=["section", "subsection", "paragraph", "pagebreak", "table"].index(item.get("type", "section")), key=f"doc_type_{i}")
        with c2:
            content_val = ""
            if type_val != "pagebreak":
                content_val = st.text_input(f"Content #{i+1}", value=item.get("content", ""), key=f"doc_content_{i}")
        with c3:
            description_val = ""
            if type_val == "table":
                description_val = st.text_input(f"Description #{i+1}", value=item.get("description", ""), key=f"doc_desc_{i}")
        new_item = {"type": type_val}
        if type_val != "pagebreak":
            new_item["content"] = content_val
        if type_val == "table":
            new_item["description"] = description_val
        new_contents.append(new_item)
    if st.button("Add Content Item", key="add_content_item"):
        contents.append({"type": "section", "content": ""})
    st.session_state["contents"] = new_contents

    new_columns = []
    new_columns = []
    for i, col in enumerate(columns):
        # First row: Column name, Source column, Sorting, Delete button
        c1, c2, c3, c4 = st.columns([3,3,2,1])
        with c1:
            col_name = st.text_input(f"Column name #{i+1}", value=col.get("name", ""), key=f"table_colname_{i}")
        with c2:
            column = st.text_input(f"Source column #{i+1}", value=col.get("column", ""), key=f"table_column_{i}")
        with c3:
            sorting = st.selectbox(f"Sorting #{i+1}", ["", "ascending", "descending"], index=["", "ascending", "descending"].index(col.get("sorting", "")), key=f"table_sorting_{i}")
        with c4:
            delete_col = st.button("🗑️", key=f"delete_col_{i}")
        # Second row: Type, On, Select (On and Select only if type is split)
        c5, c6, c7 = st.columns([3,3,2])
        with c5:
            col_type = st.selectbox(
                f"Type #{i+1}",
                ["split", "week number", "count", "unique count"],
                index=["split", "week number", "count", "unique count"].index(col.get("type", "split")),
                key=f"table_type_{i}"
            )
        on_val = ""
        select_val = ""
        with c6:
            if col_type == "split":
                on_val = st.text_input(f"On #{i+1}", value=col.get("on", ""), key=f"table_on_{i}")
        with c7:
            if col_type == "split":
                select_val = st.text_input(f"Select #{i+1}", value=str(col.get("select", "")), key=f"table_select_{i}")
        # Third row: Replacements, Group, Filters (as appropriate)
        c8, c9, c10 = st.columns([3,3,2])
        with c8:
            replacements_val = st.text_area(f"Replacements (JSON) #{i+1}", value=json.dumps(col.get("replacements", {})), key=f"table_replacements_{i}")
        with c9:
            group_val = ""
            if col_type in ["count", "unique count"]:
                group_val = st.text_area(f"Group (JSON) #{i+1}", value=json.dumps(col.get("group", [])), key=f"table_group_{i}")
        with c10:
            filters_val = st.text_area(f"Filters (JSON) #{i+1}", value=json.dumps(col.get("filters", [])), key=f"table_filters_{i}")
        # If delete button was pressed, skip adding this column
        if delete_col:
            continue
        # Compose the new column
        new_col = {"name": col_name, "column": column}
        if sorting:
            new_col["sorting"] = sorting
        if col_type:
            new_col["type"] = col_type
        if col_type == "split":
            if on_val:
                new_col["on"] = on_val
            if select_val and select_val != "":
                try:
                    new_col["select"] = int(select_val)
                except Exception:
                    new_col["select"] = select_val
        try:
            new_col["replacements"] = json.loads(replacements_val)
        except Exception:
            new_col["replacements"] = {}
        if col_type in ["count", "unique count"] and group_val:
            try:
                new_col["group"] = json.loads(group_val)
            except Exception:
                new_col["group"] = []
        try:
            new_col["filters"] = json.loads(filters_val)
        except Exception:
            new_col["filters"] = []
        new_columns.append(new_col)

        # Compose the new column
        new_col = {"name": col_name, "column": column}
        if sorting:
            new_col["sorting"] = sorting
        if col_type:
            new_col["type"] = col_type
        if col_type == "split":
            if on_val:
                new_col["on"] = on_val
            if select_val and select_val != "":
                try:
                    new_col["select"] = int(select_val)
                except Exception:
                    new_col["select"] = select_val
        # Always process these fields
        try:
            new_col["replacements"] = json.loads(replacements_val)
        except Exception:
            new_col["replacements"] = {}
        if col_type in ["count", "unique count"] and group_val:
            try:
                new_col["group"] = json.loads(group_val)
            except Exception:
                new_col["group"] = []
        try:
            new_col["filters"] = json.loads(filters_val)
        except Exception:
            new_col["filters"] = []
        new_columns.append(new_col)

    if st.button("Add Column", key="add_column"):
        st.session_state["add_column"] = True
    if st.session_state.get("add_column", False):
        new_columns.append({"name": "", "column": ""})
        st.session_state["add_column"] = False
    st.session_state["table"] = {"data": data_val, "name": name_val, "columns": new_columns}

if st.button("Run Pipeline"):
    # Save all valid edits
    all_valid = all(valid for _, valid in list(table_edits.values()))
    if not all_valid:
        st.error("Cannot run pipeline: Fix invalid JSON first.")
    
        for path, (content, _) in doc_edits.items():
            with open(path, "w") as f:
                f.write(content)
        for path, (content, _) in table_edits.items():
            with open(path, "w") as f:
                f.write(content)
        st.info("Saved all JSON changes. Running pipeline...")
        result = subprocess.run([
            "python", "resources/app.py", "--parameters", "input/parameters.json"
        ], capture_output=True, text=True)
        if result.returncode == 0:
            st.success("Pipeline completed successfully.")
        
            st.error(f"Pipeline failed: {result.stderr}")
        # Show output files to download
        output_dir = Path(json.load(open("input/parameters.json"))["output"])
        if output_dir.exists():
            st.header("Output Files")
















# --- Parameters ---
st.header("3. Set Parameters and Run Pipeline")
with open("input/parameters.json") as f:
    parameters = json.load(f)

output_dir = st.text_input("Output directory", value=parameters.get("output", "output"))
memory = st.text_input("Memory limit (e.g. 2GB)", value=parameters.get("memory", "2GB"))

parameters["output"] = output_dir
parameters["memory"] = memory

if st.button("Run Pipeline", key="run_pipeline_btn"):
    # Save parameters
    with open("input/parameters.json", "w") as f:
        json.dump(parameters, f, indent=2)
    # Run the pipeline (call app.py)
    st.info("Running pipeline... this may take a while.")
    result = subprocess.run([
        "python", "resources/app.py", "--parameters", "input/parameters.json"
    ], capture_output=True, text=True)
    if result.returncode == 0:
        st.success("Pipeline completed successfully.")
    
        st.error(f"Pipeline failed: {result.stderr}")

# --- Output ---
st.header("4. Download Output Files")
if Path(output_dir).exists():
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, "rb") as f:
                st.download_button(f"Download {file}", f, file_name=file)

    st.info("No output directory found yet. Run the pipeline to generate outputs.")
