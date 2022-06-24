#!/bin/bash

echo "SCRIPT: Invoked function"
echo "$INPUT_FILE_PATH"

# Needed for untrusted certificates.
echo 'Davix.GSI.CACheck: n' > .rootrc

mapper_dir=$(grep -m 1 path: /oscar/config/function_config.yaml | awk '{print $2}')
endpoint=$(grep endpoint /oscar/config/function_config.yaml | awk '{print $2}')
access_key=$(grep access_key /oscar/config/function_config.yaml | awk '{print $2}')
secret_key=$(grep secret_key /oscar/config/function_config.yaml | awk '{print $2}')

echo "La salida se guardara en ${TMP_OUTPUT_DIR}"
echo "Creating partial results folder"
mkdir "$TMP_OUTPUT_DIR/partial-results"
mkdir "$TMP_OUTPUT_DIR/logs"
mkdir "$TMP_OUTPUT_DIR/benchmarks"
python3 /opt/bench.py "$INPUT_FILE_PATH" "$TMP_OUTPUT_DIR" "$mapper_dir" "$endpoint" "$access_key" "$secret_key"
echo "Python function ended"

echo "Exiting root.sh"
