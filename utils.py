import json

def json_compact(obj):
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)
