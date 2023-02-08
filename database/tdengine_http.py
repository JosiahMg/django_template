# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/6/27 11:48
# Function:  http://<fqdn>:<port>/rest/sql/[db_name]

import pandas as pd
import requests
import numpy as np
from common.log_utils import get_logger
from conf.config import TDengineHttp

logger = get_logger(__name__)


class TDEngineHTTPOp:
    def __init__(self):
        self.url = f"http://{TDengineHttp['url']}/rest/sql/{TDengineHttp['db_name']}"
        self.user = TDengineHttp['user']
        self.pswd = TDengineHttp['password']
        """
        Column Type
            TDENGINE                 PANDAS                 描述
        ----------------        ----------------      ----------------
        - 1：BOOL                    np.bool          布尔型，{true, false}          
        - 2：TINYINT                 np.int8       1字节 - 单字节整型，范围 [-127, 127], -128 用作 NULL
        - 3：SMALLINT                np.int16
        - 4：INT                     np.int32     
        - 5：BIGINT                  np.int64  
        - 6：FLOAT                   np.float32
        - 7：DOUBLE                  np.float64
        - 8：BINARY                  str
        - 9：TIMESTAMP               datetime64[ns]
        - 10：NCHAR                  str
        """
        self.types = {1: np.bool, 2: np.int8, 3: np.int16, 4: np.int32, 5: np.int64,
                      6: np.float32, 7: np.float64, 8: "str", 9: "datetime64[ns]", 10: "str"}
        logger.info(f"connect {self.url}, user: {TDengineHttp['user']}, database: {TDengineHttp['db_name']}")

    def execute(self, sql) -> dict:
        r = requests.get(self.url, auth=(self.user, self.pswd), data=sql.encode("utf-8"))
        if r.status_code != 200:
            logger.error(f"request return status_code: {r.status_code}")
            raise Exception(f"request return status_code: {r.status_code}")
        jdata = r.json()
        if jdata["status"] != "succ":
            logger.error("TDengine http failed")
            raise Exception("Execute failed...")

        return r.json()

    def query(self, sql) -> pd.DataFrame:
        jsondata = self.execute(sql)

        columns = []
        dtype = {}
        for c in jsondata["column_meta"]:
            columns.append(c[0])
            dtype[c[0]] = self.types[c[1]]

        df = pd.DataFrame(jsondata["data"], columns=columns).astype(dtype)
        data = df.to_dict(orient='records')
        return data

    def close(self):
        # http接口不需要close操作
        pass
