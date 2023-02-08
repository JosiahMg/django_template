"""
使用tdengine 原生接口读取TDengine数据库
需要本地安装tdengine驱动
配置 taos.cfg文件：
firstEp                   tdengine-server:6030
fqdn                      tdengine-server
serverPort                6030
timezone
charset: UTF-8

配置本地hosts:
<xxx.xxx.xxx.xxx> <FQDN>
"""
from conf.config import TDConfig
import taos
from common.log_utils import get_logger

logger = get_logger(__name__)


class TDEngineOp:
    def __init__(self):
        try:
            self.conn = taos.connect(host=TDConfig['host'],
                                     port=TDConfig['port'],
                                     user=TDConfig['user'],
                                     password=TDConfig['password'],
                                     database=TDConfig['db_name'],
                                     config=TDConfig['config'],
                                     timezone=TDConfig['timezone'])
            logger.info(f"connect {TDConfig['host']}:{TDConfig['port']}, user: {TDConfig['user']}, database: {TDConfig['db_name']}")
            logger.info('server version info: {}'.format(self.conn.server_info))
            logger.info('client version info: {}'.format(self.conn.client_info))
            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error("连接数据库出错: {}".format(e))
            raise ValueError("连接数据库出错")

    # 查询数据库 返回 List[Dict]类型
    def query(self, sql):
        try:
            result = self.conn.query(sql)
            return result.fetch_all_into_dict()
        except Exception as e:
            logger.error(f"查询{sql}数据库出错:{e}")
            raise ValueError('查询数据库出错')

    # 查询数据库 返回 List[Tuple]类型
    def query_tuple(self, sql):
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"查询{sql}数据库出错:{e}")
            raise ValueError('查询数据库出错')

    # 执行sql语句 不需要返回值
    def execute(self, sql):
        try:
            self.conn.execute(sql)
        except Exception as e:
            logger.error(f"执行{sql}语句出错:{e}")
            raise ValueError('执行sql语句出错')

    def close(self):
        self.cursor.close()
        self.conn.close()
