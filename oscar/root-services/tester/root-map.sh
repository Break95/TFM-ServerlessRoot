#!/bin/bash

echo "SCRIPT: Invoked function"
echo "$INPUT_FILE_PATH"

#. /root_install/bin/thisroot.sh
echo "$(whoami)"

echo "$(cat /oscar/config/function_config.yaml)"

token=$(grep token /oscar/config/function_config.yaml | awk '{print $2}')
access_key=$(grep access_key /oscar/config/function_config.yaml | awk '{print $2}')
secret_key=$(grep secret_key /oscar/config/function_config.yaml | awk '{print $2}')

mkdir "$TMP_OUTPUT_DIR/logs"
echo "Test 1" > "$TMP_OUTPUT_DIR/logs/test1.txt"
mkdir "$TMP_OUTPUT_DIR/out"
echo "Test 2" > "$TMP_OUTPUT_DIR/out/test2.txt"

echo "$token"
echo "$access_key"
echo "$secret_key"

env
