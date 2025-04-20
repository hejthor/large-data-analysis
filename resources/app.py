import argparse
import json
import os

from compress import compress
from extract import extract
from document import document

def parse_memory_string(memory_str):
    units = {'GB': 1024**3}
    number = ''.join(filter(str.isdigit, memory_str))
    unit = ''.join(filter(str.isalpha, memory_str.upper()))
    return int(number) * units.get(unit, 1)

def app(parameters_path):
    parameters = json.load(open(parameters_path, 'r'))

    output = parameters.get("output")
    data = parameters.get("data")
    tables = parameters.get("tables")
    # If tables is a string (path), load the array from that file
    if isinstance(tables, str):
        with open(tables, 'r') as f:
            tables = json.load(f)
    documents = parameters.get("documents")
    memory = parse_memory_string(parameters.get("memory"))

    if not output:
        print("[ERROR] 'output' must be defined in the parameters.")
        return

    os.makedirs(output, exist_ok=True)

    parquets_dir = os.path.join(output, "parquets")
    tables_dir = os.path.join(output, "tables")
    documents_dir = os.path.join(output, "documents")

    if not os.path.exists(parquets_dir) and data and memory:
        print("[PYTHON][app.py] Running compress()")
        compress(output, data, memory)

    if os.path.exists(parquets_dir) and not os.path.exists(tables_dir) and tables and memory:
        print("[PYTHON][app.py] Running extract()")
        extract(output, tables, memory)

    if os.path.exists(parquets_dir) and os.path.exists(tables_dir) and not os.path.exists(documents_dir) and documents:
        print("[PYTHON][app.py] Running document()")
        document(output, documents)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run processing pipeline")
    parser.add_argument("--parameters", type=str, required=True, help="Path to parameters JSON file")
    args = parser.parse_args()
    app(args.parameters)