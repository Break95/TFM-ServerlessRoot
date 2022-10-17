#!/bin/bash

echo "SCRIPT: Invoked Coordinator Serivce."
echo "$INPUT_FILE_PATH"


# In the coordinator we need the credentials to listen to the bucket.
mapper_dir=$(grep -m 1 path: /oscar/config/function_config.yaml | awk '{print $2}')
endpoint=$(grep endpoint /oscar/config/function_config.yaml | awk '{print $2}')
access_key=$(grep access_key /oscar/config/function_config.yaml | awk '{print $2}')
secret_key=$(grep secret_key /oscar/config/function_config.yaml | awk '{print $2}')

echo "Launching Coordinator"
python3 /opt/python-runner.py "$INPUT_FILE_PATH" "$mapper_dir" "$endpoint" "$access_key" "$secret_key"
echo "Coordinator Finished"

echo "Exiting coordinator script."
