import argparse
import json
import os

from compress import compress
from document import document

def app(parameters_path):
    parameters = json.load(open(parameters_path, 'r'))
    output = parameters.get("output")
    data = parameters.get("data")
    documents = parameters.get("documents")
    memory = parameters.get("memory")

    os.makedirs(output, exist_ok=True)
    parquets_dir = os.path.join(output, "parquets")
    documents_dir = os.path.join(output, "documents")

    if not os.path.exists(parquets_dir) and data and memory:
        print("[PYTHON][app.py] Running compress()")
        compress(parquets_dir, data, memory)

    if os.path.exists(parquets_dir) and documents and memory:
        print("[PYTHON][app.py] Running document()")
        for path in documents:
            document(documents_dir, path, memory)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run processing pipeline")
    parser.add_argument("--parameters", type=str, required=True, help="Path to parameters JSON file")
    args = parser.parse_args()
    app(args.parameters)