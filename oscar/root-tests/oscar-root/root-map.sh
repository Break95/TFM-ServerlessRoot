#!/bin/bash

echo "SCRIPT: Invoked function"
echo "$INPUT_TYPE" # Esta variable ya no existe.
echo "$INPUT_FILE_PATH"

#. /root_install/bin/thisroot.sh
echo $(whoami)
echo $(python3 --version)
echo $(root-config --python-version)
echo $(root-config --python3-version)
echo $(root-config --python2-version)

#if [ "$INPUT_TYPE" = "json" ]
#then
    echo "Inside then"
    # Pass the function string as parameter. A more verastile alternative
    #   is to open the file inside the python script and parse it there.

    echo "La salida se guardara en ${TMP_OUTPUT_DIR}"
    python3 /opt/python-runner.py "$INPUT_FILE_PATH" "$TMP_OUTPUT_DIR"
    echo "Python function ended"
#fi
echo "Exiting map.sh"
#FILE_NAME=`basename "$INPUT_FILE_PATH"
#OUTPUT_FILE="$TMP_OUTPUT_DIR/$FILE_NAME"
#python3 /opt/python-runner.py "$INPUT_FILE_PATH" -o "$OUTPUT_FILE"

