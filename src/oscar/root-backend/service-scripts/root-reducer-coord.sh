#!/bin/bash

echo "SCRIPT: Invoked coordinated reducer Service."
echo "$INPUT_FILE_PATH"

echo 'Davix.GSI.CACheck: n' > .rootrc

mapper_dir=$(grep -m 1 path: /oscar/config/function_config.yaml | awk '{print $2}')
endpoint=$(grep endpoint /oscar/config/function_config.yaml | awk '{print $2}')
access_key=$(grep access_key /oscar/config/function_config.yaml | awk '{print $2}')
secret_key=$(grep secret_key /oscar/config/function_config.yaml | awk '{print $2}')

mkdir "$TMP_OUTPUT_DIR/partial-results"

echo "Launching coordinated reducer."
python3 /opt/python-runner.py "$INPUT_FILE_PATH" "$TMP_OUTPUT_DIR" "$endpoint" "$access_key" "$secret_key" "$mapper_dir"
echo "Coordinated reducer finished."

echo "Exiting reducer scipt."
