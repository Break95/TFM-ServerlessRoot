#!/bin/bash

echo "SCRIPT: Invoked reduce function"
echo "$INPUT_FILE_PATH"

# Get minio credentials from ConfigMap
mapper_dir=$(grep -m 1 path: /oscar/config/function_config.yaml | awk '{print $2}') # Used to get bucket name
endpoint=$(grep endpoint /oscar/config/function_config.yaml | awk '{print $2}')
access_key=$(grep access_key /oscar/config/function_config.yaml | awk '{print $2}')
secret_key=$(grep secret_key /oscar/config/function_config.yaml | awk '{print $2}')

echo "La salida se guardara en ${TMP_OUT_DIR}/${FILE_NAME}"
mkdir "$TMP_OUTPUT_DIR/partial-results"
mkdir "$TMP_OUTPUT_DIR/logs"
python3 /opt/serverless-reducer.py "$INPUT_FILE_PATH" "$TMP_OUTPUT_DIR" "$endpoint" "$access_key" "$secret_key" "$mapper_dir"
echo "Python function ended"

echo "Exiting map.sh"

