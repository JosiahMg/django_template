"""
MongoDB操作命令
"""
import pymongo

from conf.log import logger
# from conf.path_config import rule_data_dir


class MongodbOperator(object):  # 关闭连接的设置
    def __init__(self, host, port, user, password):
        """创建连接，eg:mongodb://[username:password@]
        host1[:port1][/[database][?options]] """
        connect_info = 'mongodb://{}:{}@{}:{}'.format(user, password, host, port)
        logger.info('mongodb connect info:{}'.format(connect_info))
        self.client = pymongo.MongoClient(connect_info)
        self.database = None
        self.collection = None

    def db_connect(self, db_name):
        self.database = self.client.get_database(db_name)

    def describe(self):  # TODO 需要展示哪些信息
        coll_names = self.database.list_collection_names()
        logger.info('数据库内已有集合：{}'.format(coll_names))

    def clean_collection(self, coll_name):
        """清空一个集合的内容"""
        logger.info('清空集合: {}'.format(coll_name))
        self.delete(coll_name, {})

    def insert(self, coll_name, document):
        """ 插入数据到指定集合, document为list(dict)"""
        collection = self.database.get_collection(coll_name)
        if not isinstance(document, list):
            raise TypeError('插入数据应为list，错误类型：{}'.format(type(document)))
        else:
            res = collection.insert_many(document)
            flag = res.acknowledged
            if flag:
                logger.info('插入数据成功')
            else:
                logger.error('插入数据失败')

        # if isinstance(document, dict):  # 插入单条 todo isinstance
        #     res = collection.insert_one(document)
        #     flag = res.acknowledged
        # elif type(document) == list:  # 插入多条
        #     res = collection.insert_many(document)
        #     flag = res.acknowledged
        # else:
        #     logger.debug('插入数据类型:{}错误'.format(type(document)))  # todo exception 先检查异常
        #     flag = False
        # if flag:
        #     logger.debug('插入数据成功')  # todo 抛异常 外层业务逻辑处理
        # else:
        #     logger.debug('插入数据失败')

    def delete(self, coll_name, condition):
        """删除指定集合内符合条件的数据"""
        collection = self.database.get_collection(coll_name)
        res = collection.delete_many(condition)
        flag = res.acknowledged
        if flag:
            logger.info('删除数据成功')
        else:
            logger.error('删除数据失败')
        # logger.info('删除结果:{}'.format(res.acknowledged))  # todo info级别 内容一致

    def search(self, coll_name, condition, columns, sort_key=None):  # todo 通用化 顺序
        """查询指定集合内满足条件的数据,升序排列返回"""
        collection = self.database.get_collection(coll_name)
        res = collection.find(condition, columns)
        # todo res 空的时候 为何也是有值的呢
        if sort_key:
            res = res.sort(sort_key, 1)  #
        return list(res)

    def update(self, coll_name, condition, update_value):
        """修改指定集合内符合条件的文档的相关字段"""
        collection = self.database.get_collection(coll_name)
        res = collection.update_many(condition, {'$set': update_value})
        logger.debug('修改成功:{}'.format(res.acknowledged))

    def sort(self, ):
        pass

    def close(self):
        self.client.close()

