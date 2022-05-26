#!/bin/bash

echo "SCRIPT: Invoked reduce function"
echo "$INPUT_TYPE" # Esta variable ya no existe.
echo "$INPUT_FILE_PATH"


# Get credentials and reduce service token from ConfigMap 
endpoint=$(grep endpoint /oscar/config/function_config.yaml | awk '{print $2}')
token=$(grep token /oscar/config/function_config.yaml | awk '{print $2}')
access_key=$(grep access_key /oscar/config/function_config.yaml | awk '{print $2}')
secret_key=$(grep secret_key /oscar/config/function_config.yaml | awk '{print $2}')
red_service_name=$(grep oscar_service /oscar/config/function_config.yaml | awk '{print $2}')

echo "La salida se guardara en ${TMP_OUT_DIR}/${FILE_NAME}"
python3 /opt/serverless-reducer.py "$INPUT_FILE_PATH" "$TMP_OUTPUT_DIR" "$endpoint" "$access_key" "$secret_key" "$token" "$red_service_name"
echo "Python function ended"

echo "Exiting map.sh"

