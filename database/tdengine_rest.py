"""
使用tdengine restful接口读取TDengine数据库
读取数据库时只支持utf-8的编码方式
不支持异步查询和写入操作
"""
from conf.config import TDengineRest
import taosrest
from common.log_utils import get_logger

logger = get_logger(__name__)


class TDEngineRestOp:
    def __init__(self):
        try:
            self.conn = taosrest.connect(url=TDengineRest['url'],
                                         user=TDengineRest['user'],
                                         password=TDengineRest['password'],
                                         timeout=TDengineRest['timeout'],
                                         token=TDengineRest['token'])
            logger.info('server version info: {}'.format(self.conn.server_info))
            self.cursor = self.conn.cursor()
            # restfule接口需要通过use指定数据库名
            # self.execute(f"use {TDengineRest['db_name']}")
            logger.info(f"connect {TDengineRest['url']}, user: {TDengineRest['user']}, database: {TDengineRest['db_name']}")
        except Exception as e:
            logger.error("连接数据库出错: {}".format(e))
            raise ValueError("连接数据库出错")

    # 查询数据库 返回 List[Dict]类型
    def query(self, sql):
        try:
            self.cursor.fetch_start_time_data(sql)
            return self._row2dict()
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错[query]')

    # 查询数据库 返回 List[Tuple]类型
    def query_tuple(self, sql):
        try:
            self.cursor.fetch_start_time_data(sql)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错[query_tuple]')

    # 执行sql语句 不需要返回值
    def execute(self, sql):
        try:
            self.cursor.fetch_start_time_data(sql)
        except Exception as e:
            logger.error("执行sql语句出错:{}".format(e))
            raise ValueError('执行sql语句出错')

    def _row2dict(self):
        columns = [col[0] for col in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def close(self):
        self.cursor.close()
        self.conn.close()