#!/bin/bash

echo "SCRIPT: Invoked function"
echo "$INPUT_TYPE"
echo "$INPUT_FILE_PATH"

#if [ "$INPUT_TYPE" = "json" ]
#then
    echo "Inside then"
    # Pass the function string as parameter. A more verastile alternative
    #   is to open the file inside the python script and parse it there.
    python3 /opt/python-runner.py "$INPUT_FILE_PATH"
    echo "Python function ended"
#fi
echo "Exiting root-map.sh"
#FILE_NAME=`basename "$INPUT_FILE_PATH"
#OUTPUT_FILE="$TMP_OUTPUT_DIR/$FILE_NAME"
#python3 /opt/python-runner.py "$INPUT_FILE_PATH" -o "$OUTPUT_FILE"

