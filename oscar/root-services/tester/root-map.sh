#!/bin/bash

echo "SCRIPT: Invoked function"
echo "$INPUT_TYPE" # Esta variable ya no existe.
echo "$INPUT_FILE_PATH"

#. /root_install/bin/thisroot.sh
echo $(whoami)

echo $(cat /oscar/config/function_config.yaml)

token=echo $(grep token /oscar/config/function_config.yaml | awk '{print $2}')
access_key=echo $(grep access_key /oscar/config/function_config.yaml | awk '{print $2}')
secret_key=echo $(grep secret_key /oscar/config/function_config.yaml | awk '{print $2}')


echo $token
echo $access_key
echo $secret_key
