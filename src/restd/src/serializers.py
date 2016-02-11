from datetime import datetime

import json


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return str(obj)
        return json.JSONEncoder.default(self, obj)
