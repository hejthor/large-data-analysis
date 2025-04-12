import argparse
import json
import os

from compress import compress
from document import document

def app(parameters):
    parameters = json.load(
        open(parameters, 'r')
    )
    
    os.makedirs(parameters["output"], exist_ok=True)
    
    if not os.path.exists(parameters["output"] + "/parquets"):
        print("[PYTHON][app.py] Running compress()")
        compress(parameters["output"], parameters["data"], parameters["blocksize"])
    
    if os.path.exists(parameters["output"] + "/parquets"):
        print("[PYTHON][app.py] Running extract()")
        # extract(tables)
        
        print("[PYTHON][app.py] Running document()")
        document(parameters["output"], parameters["documents"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--parameters", type=str, required=True, help="Path to parameters JSON file")
    args = parser.parse_args()
    app(args.parameters)