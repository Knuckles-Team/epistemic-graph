import msgpack
import json

req = {
    "id": 1,
    "graph": "test",
    "auth_token": "dummy",
    "method": "CreateGraph",
    "params": {"graph_name": "test", "graph_type": "Agent"},
}
# Pack using msgpack
p = msgpack.packb(req)
print(p)
