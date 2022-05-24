#!/bin/bash

echo "SCRIPT: Invoked reduce function"
echo "$INPUT_TYPE" # Esta variable ya no existe.
echo "$INPUT_FILE_PATH"

echo $(env)

#. /root_install/bin/thisroot.sh
#echo $(whoami)
#echo $(python3 --version)
#echo $(root-config --python-version)
#echo $(root-config --python3-version)
#echo $(root-config --python2-version)

#if [ "$INPUT_TYPE" = "json" ]
#then
    echo "Inside then"
    # Pass the function string as parameter. A more verastile alternative
    #   is to open the file inside the python script and parse it there.

    echo "La salida se guardara en ${TMP_OUT_DIR}/${FILE_NAME}"
    python3 /opt/serverless-reducer.py "$INPUT_FILE_PATH" "$TMP_OUT_DIR/$FILE_NAME"
    echo "Python function ended"
#fi
echo "Exiting map.sh"
#FILE_NAME=`basename "$INPUT_FILE_PATH"
#OUTPUT_FILE="$TMP_OUTPUT_DIR/$FILE_NAME"
#python3 /opt/python-runner.py "$INPUT_FILE_PATH" -o "$OUTPUT_FILE"
