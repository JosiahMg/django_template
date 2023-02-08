# -*- coding: utf-8 -*-
# Author: WHd
# Date: 2022/11/2 9:12
import os.path
import time
import pandas as pd
from conf.config import TDengineConfig
from taosrest import RestClient
from concurrent.futures import ThreadPoolExecutor
from conf.path_config import resource_dir
from common.log_utils import get_logger

logger = get_logger(__name__)


class TDOperatedByClient:
    """相比较Rest和sqlalchemy执行sql要快2秒左右"""

    def __init__(self):
        try:
            self.client = RestClient(url=f"http://{TDengineConfig['url']}",
                                     user=TDengineConfig['user'],
                                     password=TDengineConfig['password'],
                                     database=TDengineConfig['db_name'])
            logger.info(f"connect {TDengineConfig['url']}, user: {TDengineConfig['user']}, database: {TDengineConfig['db_name']}")
        except Exception as e:
            logger.error("连接数据库出错: {}".format(e))
            raise ValueError("连接数据库出错")

    def exec_sql(self, _sql):
        """执行所有操作"""
        try:
            return self.client.sql(_sql)
        except Exception as e:
            logger.error("执行sql语句出错:{}".format(e))
            raise ValueError('执行sql语句出错')

    #   """--------------------------关于表的查询操作---------------------------------"""

    def query(self, _sql):
        """执行查询list[dict]操作"""
        try:
            return self._row2dict(self.client.sql(_sql))
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错[query]')

    def query_df(self, _sql):
        """执行查询dataframe操作"""
        try:
            return self._row2df(self.client.sql(_sql))
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错[query]')

    def _row2dict(self, _df):
        """返回字典列表"""
        return self._row2df(_res=_df).to_dict(orient="records")

    def query_table_cols(self, table_name):
        """获取表字段名称"""
        return [item[0] for item in self.client.sql(f"""Describe {TDengineConfig['db_name']}.{table_name};""")["data"]]

    #   """--------------------------关于表的查询操作---------------------------------"""

    #   """--------------------------关于表的创建操作---------------------------------"""
    @staticmethod
    def judge_in(list_, item_):
        if ('BINARY' in item_) | ('NCHAR' in item_):
            list_.append(item_[0] + ' ' + item_[1] + '(' + str(item_[2]) + ')')
        else:
            list_.append(item_[0] + ' ' + item_[1])
        return list_

    @staticmethod
    def splice_fields(field_list):
        """字段拼接"""
        field_property = ''
        for index, field_ in enumerate(field_list):
            if index < len(field_list) - 1:
                field_property += field_ + ','
            else:
                field_property += field_
        return field_property

    def duplicate_table_struct(self, origin_table, table_name):
        """复制表结构, 返回sql"""
        tags_list = []
        fields_list = []
        struct = self.client.sql(f"""Describe {TDengineConfig['db_name']}.{origin_table};""")["data"]
        for item in struct:
            if 'TAG' in item:
                self.judge_in(tags_list, item)
            else:
                self.judge_in(fields_list, item)
        tags = self.splice_fields(tags_list)
        fields = self.splice_fields(fields_list)
        return f"""CREATE TABLE IF NOT EXISTS {table_name} ({fields}) TAGS({tags});"""

    def create_child_table(self, sub_table, sup_table, tup_fields):
        """根据超级表创建子表"""
        sql = f"""CREATE TABLE IF NOT EXISTS {sub_table} using {TDengineConfig['db_name']}.{sup_table} tags {tuple(tup_fields)};"""
        self.exec_sql(sql)

    #   """--------------------------关于表的创建操作---------------------------------"""

    #   """--------------------------从dataframe批量插入到TD库---------------------------------"""
    def split_from_middle(self, df_mid, sub_table, sup_table=None, tag_fields=None, tag=None):
        """将dataframe从中间进行拆分"""
        mid = int(df_mid.shape[0] / 2)
        dfs_mid = df_mid.loc[df_mid.index.values[0]:df_mid.index.values[0] + mid, :], df_mid.loc[
                                                                                      df_mid.index.values[0] + mid:, :]
        self.batch2db_by_multithreading(dfs_mid, sub_table, sup_table, tag_fields, tag)

    def df2td_by_sql(self, _df, sub_table, sup_table=None, tag_fields=None, tag=None):
        """dataframe插入到数据库表格中, sup_table和tag_fields必须同时存在或同时消失"""
        if tag_fields is not None:
            if tag is None:
                tag = _df[tag_fields].values.tolist()[0]
            _df = _df[list(set(_df.columns).difference(set(tag_fields)))].copy()

        try:
            df_str = ""
            _df = _df.fillna(value='None')
            for item in _df.apply(lambda x: tuple(x), axis=1):
                df_str += " " + str(item)
        except Exception as e:
            logger.error("dataframe 数据为空:{}".format(e))
            raise ValueError('生成sql出错')

        if (sup_table is None) | (tag is None):
            sql_ = f"""INSERT INTO {sub_table} {tuple(_df.columns)} VALUES"""
        else:
            sql_ = f"""INSERT INTO {sub_table} USING {sup_table} TAGS {tuple(tag)} {tuple(_df.columns)} VALUES"""
        sql_ += df_str

        try:
            self.exec_sql(sql_)
            # logger.debug('sql 批量插入成功！')
        except Exception as e:
            try:
                self.split_from_middle(_df, sub_table, sup_table, tag_fields, tag)
                # logger.debug("二分法进行 sql 批量插入成功！")
            except Exception as e:
                logger.error("sql 二分法进行批量插入出错:{}".format(e))
                raise ValueError('批量插入出错')

    def batch2db_by_multithreading(self, df_iteration, sub_table, sup_table=None, tag_fields=None, tag=None):
        """划分区域采用多线程批量插入"""
        try:
            if isinstance(df_iteration, pd.DataFrame):
                df_iteration = [df_iteration]
            with ThreadPoolExecutor(max_workers=14) as pool:
                [pool.submit(self.df2td_by_sql, _df=df_item, sub_table=sub_table, sup_table=sup_table,
                             tag_fields=tag_fields, tag=tag) for df_item in df_iteration]
        except Exception as e:
            raise SyntaxError(e)

    #   """--------------------------从dataframe批量插入到TD库---------------------------------"""

    @staticmethod
    def _row2df(_res):
        """返回dataframe"""
        try:
            cols = [item[0] for item in _res['column_meta']]
            return pd.DataFrame(data=_res['data'], columns=cols)
        except Exception as e:
            logger.error("数据转dataframe错误:{}".format(e))
            raise ValueError('列名不存在或表数据为空')


if __name__ == '__main__':
    """-------------------------------199服务器查询上限测试--------------------------------------开始--"""
    start = time.time()
    # df3 = TDOperatedByClient().query_df('select * from weather')
    # print(df3.shape[0])
    """-------------------------------199服务器查询上限测试---------------------------------------结束--"""

    """--------------使用170服务器TD库数据进行测试-----------创建超级表（生成sql，执行sql）-------------开始----"""
    # 第一步 在199服务器生成超级表结构，开始

    # from app.tdengine_rest import TDOperatedByRest
    # td = TDOperatedByClient()
    # td2 = TDOperatedByRest()
    #
    # struct_sql = td.duplicate_table_struct('device_message', 'device_message')
    # print(struct_sql)
    # td2.execute(struct_sql)
    # 结束

    # 在199服务器生成超级表的子表
    # a = ["imei", '0']
    # td.create_child_table("sub_device_message1", "device_message", tuple(a))

    # 批量插入数据

    # 三种情况下的多线程批量插入
    # td.batch2db_by_multithreading(df2, sub_table='sub_device_message2')  # 子表已存在
    # td.batch2db_by_multithreading(df2, sub_table='sub_device_message4', sup_table='device_message',
    #                 tag_fields=['imei', 'message_function_code'])  # 子表不存在，dataframe中有tag
    # td.batch2db_by_multithreading(df2, sub_table='sub_device_message4', sup_table='device_message',
    #                               tag_fields=['imei', 'message_function_code'], tag=['imei_copy2', '15jd'])
    # 子表不存在，根据自己需求设定tag

    # 批量插入测试
    # df = pd.read_csv("./d.csv")
    # print(list(df))
    # df2 = df.loc[0:12000, :].copy()
    # td.df2td_by_sql(df2, sub_table='sub_device_message4', sup_table='device_message',
    #                 tag_fields=['imei', 'message_function_code'])  # 子表不存在，dataframe中有tag
    # 多线程批量插入测试
    # 快速插入，第一种情况，在读取csv时进行分块或使用时分块
    df = pd.read_csv(os.path.join(resource_dir, "./d.csv"), chunksize=500)
    # 第二种情况，直接批量插入，出错时采用二分法进行处理
    # df2 = pd.read_csv(os.path.join(resource_dir, "./d.csv"))
    # print(df2.shape[0])

    # td.df2td_by_sql(df2, sub_table='sub_device_message4', tag_fields=['imei', 'message_function_code'])
    TDOperatedByClient().batch2db_by_multithreading(df, sub_table='sub_device_message4',
                                                    tag_fields=['imei', 'message_function_code'])
    # td.batch2db_by_multithreading(df, sub_table='sub_device_message4', sup_table='device_message',
    #                               tag_fields=['imei', 'message_function_code'])  # 子表不存在，dataframe中有tag
    """--------------使用170服务器TD库数据进行测试-----------------创建超级表（生成sql，执行sql）-----------------结束-----"""
    end = time.time()
    print(end - start)
