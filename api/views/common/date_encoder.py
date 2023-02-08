# -*- coding: utf-8 -*-
# Author: Lx
# Date: 2021/3/16 16:16

import datetime
import json


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        # if isinstance(obj, bytes):
        #     return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)
