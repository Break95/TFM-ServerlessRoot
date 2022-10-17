#!/usr/bin/env python3
import requests
import os
import sys
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)



# Get the list of services.
response = requests.get(url=f"{os.environ['oscar_endpoint']}/system/services",
                        auth=requests.auth.HTTPBasicAuth(os.environ['oscar_access'],
                                                         os.environ['oscar_secret']),
                        verify=False)

for elem in response.json():
    print(f"Deleting {elem['name']}")
    delete_response = requests.delete(url=f"{os.environ['oscar_endpoint']}/system/services/{elem['name']}",
                                      auth=requests.auth.HTTPBasicAuth(os.environ['oscar_access'],
                                                                       os.environ['oscar_secret']),
                                      verify=False)
    print(delete_response)
