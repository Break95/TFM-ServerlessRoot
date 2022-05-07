import base64
import json
import ast
import sys
#text = 'asddas'
#print('Raw: ' + text )

#payload = base64.b64encode(text.encode('utf-8')).decode('utf-8')

#print('Encoded: ' + payload)


#text = "{'func': b'ZGVmIGZ1bmNfcGF5bG9hZCgpOgogICAgaW1wb3J0IFJPT1QKICAgIGltcG9ydCBvcyAgICAgICAKICAgIGZyb20gZGF0ZXRpbWUgaW1wb3J0IGRhdGV0aW1lCgogICAgZGYgPSBST09ULlJEYXRhRnJhbWUoMTAwMCkKIAogICAgIyBTZXQgdGhlIHJhbmRvbSBzZWVkIGFuZCBkZWZpbmUgdHdvIGNvbHVtbnMgb2YgdGhlIGRhdGFzZXQgd2l0aCByYW5kb20gbnVtYmVycy4KICAgIFJPT1QuZ1JhbmRvbS5TZXRTZWVkKDEpCiAgICBkZl8xID0gZGYuRGVmaW5lKCJnYXVzIiwgImdSYW5kb20tPkdhdXMoMTAsIDEpIikuRGVmaW5lKCJleHBvbmVudGlhbCIsICJnUmFuZG9tLT5FeHAoMTApIikKIAogICAgIyBCb29rIGFuIGhpc3RvZ3JhbSBmb3IgZWFjaCBjb2x1bW4KICAgIGhfZ2F1cyA9IGRmXzEuSGlzdG8xRCgoImdhdXMiLCAiTm9ybWFsIGRpc3RyaWJ1dGlvbiIsIDUwLCAwLCAzMCksICJnYXVzIikKICAgIGhfZXhwID0gZGZfMS5IaXN0bzFEKCgiZXhwb25lbnRpYWwiLCAiRXhwb25lbnRpYWwgZGlzdHJpYnV0aW9uIiwgNTAsIDAsIDMwKSwgImV4cG9uZW50aWFsIikKIAogICAgIyBQbG90IHRoZSBoaXN0b2dyYW1zIHNpZGUgYnkgc2lkZSBvbiBhIGNhbnZhcwogICAgYyA9IFJPT1QuVENhbnZhcygiVGVzdENhbnZhc09zY2FzciIsICJUZXN0Q2FudmFzT3NjYXNyIiwgODAwLCA0MDApCiAgICBjLkRpdmlkZSgyLCAxKQogICAgYy5jZCgxKQogICAgaF9nYXVzLkRyYXdDb3B5KCkKICAgIGMuY2QoMikKICAgIGhfZXhwLkRyYXdDb3B5KCkKIAogICAgIyBTYXZlIHRoZSBjYW52YXMKICAgIGMuU2F2ZUFzKHN0cihvcy5nZXRlbnYoJ1RNUF9PVVRQVVRfRElSJykpICsgInBheWxvYWR0ZXN0X18iICsgc3RyKGRhdGV0aW1lLm5vdygpLnRpbWUoKSkgICsgIl9fLnBuZyIpCg=='}"
with open(sys.argv[1], 'r+b') as file1:
    file1.encode('utf-8')
    text = json.load(file1)
    print(text)
    #  raw_code = ast.literal_eval(text)
    #print(raw_code)
    #raw_code = base64.b64decode(raw_code.decode('utf-8')).decode('utf-8')
    #print('Decoded: ' + str(raw_code))
