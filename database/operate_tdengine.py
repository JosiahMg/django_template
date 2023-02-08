# -*- coding: utf-8 -*-
# Author: WHd
# Date: 2023/1/10 16:15
"""
该TD库操作脚本适合TD库
"""
import os
import pandas as pd
from conf.config import TDengineConfig
from taosrest import RestClient
from concurrent.futures import ThreadPoolExecutor
from conf.path_config import resource_dir, data_dir
from common.common_tool import save_object_general
from common.log_utils import get_logger

logger = get_logger(__name__)


class OperateTD:
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
        try:
            return self.client.sql(_sql)
        except Exception as e:
            logger.error("执行sql语句出错:{}".format(e))
            raise ValueError('执行sql语句出错')

    """----------------------------关于表的查询操作------------------------------开始------"""

    def __row2dict(self, df):
        """
        将查询到的数据按行转为列表字典
        :param df: dataframe
        :return: [dict]
        """
        return self.__row2df(res=df).to_dict(orient="records")

    @staticmethod
    def __row2df(res):
        """
        将查询到的数据转为dataframe
        :param res: 查询结果
        :return: dataframe
        """
        try:
            cols = [item[0] for item in res['column_meta']]
            return pd.DataFrame(data=res['data'], columns=cols)
        except Exception as e:
            logger.error("数据转dataframe错误:{}".format(e))
            raise ValueError('列名不存在或表数据为空')

    def query(self, _sql):
        """
        执行查询sql并返回所有查询信息
        :param _sql: sql查询语句
        :return: 完整查询结果
        """
        try:
            return self.__row2dict(self.client.sql(_sql))
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错[query]')

    def query_df(self, _sql):
        """
        将查询结果转为dataframe
        :param _sql: 查询sql语句
        :return: dataframe
        """
        try:
            return self.__row2df(self.client.sql(_sql))
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错[query]')

    def query_table_cols(self, table_name):
        """
        获取表字段名称
        :param table_name: 表名
        :return: 表字段列表
        """
        return [item[0] for item in self.client.sql(f"""Describe {TDengineConfig['db_name']}.{table_name};""")["data"]]

    """----------------------------关于表的查询操作------------------------------结束------"""

    """----------------------------关于表的创建操作------------------------------开始------"""

    def create_child_table(self, sub_table_name, sup_table_name, tag_fields):
        """
        基于原有超级表数据结构创建子表
        :param sub_table_name: 超级表名称
        :param sup_table_name: 子表名称
        :param tag_fields: 标签字段名称列表
        :return:
        """
        try:
            self.exec_sql(
                f"""CREATE TABLE IF NOT EXISTS {sub_table_name} using {TDengineConfig['db_name']}.{sup_table_name} tags {
                tuple(tag_fields)};"""
            )
            logger.info("创建子表成功！")
        except Exception as e:
            logger.error("创建子表失败:{}".format(e))
            raise ValueError('创建子表出错 ！')

    def copy_stable_data_structure(self, original_stable_name, new_stable_name):
        """
        复制已有超级表的数据结构，创建一个新名称的超级表
        :param original_stable_name:
        :param new_stable_name:
        :return: 返回创建新超级表的sql语句
        """
        try:
            tags = []
            fields = []
            struct = self.client.sql(f"""Describe {TDengineConfig['db_name']}.{original_stable_name};""")["data"]
            for item in struct:
                if 'TAG' in item:
                    self.__judge_whether_exists_nchar_or_binary_type(tags, item)
                else:
                    self.__judge_whether_exists_nchar_or_binary_type(fields, item)
            tags = self.__concatenate_fields_type(tags)
            fields = self.__concatenate_fields_type(fields)
        except Exception as e:
            logger.error("复制超级表数据结构失败:{}".format(e))
            raise ValueError('复制超级表数据结构失败 ！')
        return f"""CREATE TABLE IF NOT EXISTS {new_stable_name} ({fields}) TAGS({tags});"""

    @staticmethod
    def __judge_whether_exists_nchar_or_binary_type(field, field_desc):
        """
        判断超级表字段类型是否为不定长类型并进行字段类型拼接
        :param field: 超级表中的字段名称
        :param field_desc: 超级表中字段名称的描述
        :return: 字段列表
        """
        try:
            if ('BINARY' in field_desc) | ('NCHAR' in field_desc):
                field.append(field_desc[0] + ' ' + field_desc[1] + '(' + str(field_desc[2]) + ')')
            else:
                field.append(field_desc[0] + ' ' + field_desc[1])
        except Exception as e:
            logger.error("拼接字段+数据类型失败:{}".format(e))
            raise ValueError('拼接字段+数据类型失败 ！')
        return field

    @staticmethod
    def __concatenate_fields_type(fields):
        """
        将字段类型拼接为sql需要的字段形式
        :param fields: 字段名称+字段类型的列表
        :return: 字段+类型拼接后的结果
        """
        try:
            field_prop = ''
            for index, field in enumerate(fields):
                if index < len(fields) - 1:
                    field_prop += field + ','
                else:
                    field_prop += field
        except Exception as e:
            logger.error("拼接所有字段及标签失败:{}".format(e))
            raise ValueError('拼接所有字段及标签失败 ！')
        return field_prop

    def save_copy_stable_sql(self, original_stable_name, new_stable_name):
        """
        保存拷贝超级表数据结构的sql语句
        :param original_stable_name:
        :param new_stable_name:
        :return:
        """
        try:
            save_object_general({
                new_stable_name: self.copy_stable_data_structure(original_stable_name, new_stable_name)
            }, data_dir, 'sql')
        except Exception as e:
            logger.error("保存创建超级表sql语句失败:{}".format(e))
            raise ValueError('保存创建超级表sql语句失败 ！')

    """----------------------------关于表的创建操作------------------------------结束------"""

    """----------------------------从dataframe格式数据批量插入到TD库---------------开始------"""

    def batch_insert_by_tags(self, df, sup_table_name, tag_fields, child_tags, sub_prefix=None, sep='_', max_workers=8,
                             max_rows=100):
        """
        根据标签列表唯一性对dataframe进行切分
        :param df: 要插入的dataframe
        :param sup_table_name: 子表名称
        :param tag_fields: 标签字段
        :param child_tags: 子表命名需要的tag
        :param sub_prefix: 子表名称前缀
        :param sep: 子表名称命名分割符
        :param max_workers: 最大线程数
        :param max_rows: 子表最大行数
        :return:
        """
        if sub_prefix is None:
            sub_prefix = sup_table_name
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                [pool.submit(self.batch_insert, df=sub_df, sub_table_name=sub_name, sup_table_name=sup_table_name,
                             tag_fields=tag_fields, max_workers=max_workers, max_rows=max_rows) for sub_name, sub_df in
                 self.__obtain_unique_df(df, tag_fields, child_tags, sub_prefix, sep).items()]
        except Exception as e:
            logger.error("通过标签名进行多线程插入失败：{}".format(e))
            raise ValueError('通过标签名进行多线程插入失败 ！')

    def batch_insert(self, df, sub_table_name, sup_table_name, tag_fields, max_workers=8, max_rows=100):
        """
        采用多线程进行批量插入
        :param max_workers: 最大执行的线程数
        :param df: 要插入到数据库的dataframe
        :param sub_table_name: 子表名称
        :param sup_table_name: 超级表名称
        :param tag_fields: 标签字段列表
        :param max_rows: 每次插入到数据库的最大行数
        :return:
        """

        try:
            dfs = self.__split_df(df, max_rows)
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                [pool.submit(self.__df2td, df=df, sub_table_name=sub_table_name, sup_table_name=sup_table_name,
                             tag_fields=tag_fields) for df in dfs]
            # logger.info('执行sql语句进行批量插入成功！')
        except Exception as e:
            logger.error("多线程插入失败：{}".format(e))
            raise ValueError('多线程插入失败 ！')

    def __df2td(self, df, sub_table_name, sup_table_name, tag_fields):
        """
        将dataframe格式数据框插入到TD库
        :param df: 要插入的dataframe
        :param sub_table_name: 子表名称
        :param sup_table_name: 超级表名称
        :param tag_fields: 标签字段列表
        :return:
        """
        if df.shape[0] > 0:
            tag_values = df[tag_fields].values.tolist()[0]
            fields_df = df[list(set(df.columns).difference(set(tag_fields)))].copy()

            try:
                field_values_str = ""
                fields_df = fields_df.fillna(value='None')
                for item in fields_df.apply(lambda x: tuple(x), axis=1):
                    field_values_str += " " + str(item)
                insert_sql = f"""INSERT INTO {sub_table_name} USING {sup_table_name} TAGS {tuple(tag_values)} {tuple(
                    fields_df.columns)} VALUES"""
                insert_sql += field_values_str
            except Exception as e:
                logger.error("拼接批量插入sql语句失败:{}".format(e))
                raise ValueError('拼接批量插入sql语句失败! ')

            try:
                self.exec_sql(insert_sql)
            except Exception as e:
                logger.error('批量插入失败：{}'.format(e))
                raise ValueError('批量插入出错')

    def __split_df(self, df, max_rows):
        """
        将dataframe进行分割
        :param df: 要分割的dataframe
        :param max_rows: 分割最大行数
        :return:
        """
        df_list = []
        if df.shape[0] > max_rows:
            df_list = self.__split_by_max_rows(df, max_rows)
        else:
            df_list.append(df)
        return df_list

    @staticmethod
    def __split_by_max_rows(df, max_rows):
        """
        将dataframe按最大行数进行分割
        :param df: 要分割的dataframe
        :param max_rows: 分割最大行数
        :return: 标签字段列表
        """
        df_list = []
        try:
            group_index = int(df.shape[0] / max_rows)
            for i in range(group_index + 1):
                if i < group_index:
                    df_list.append(df.loc[(max_rows * i):(max_rows * (i + 1) - 1), :])
                else:
                    df_list.append(df.loc[(max_rows * i):, :])
        except Exception as e:
            logger.error('将dataframe按最大行数进行分割失败：{}'.format(e))
            raise ValueError('将dataframe按最大行数进行分割失败！')
        return df_list

    @staticmethod
    def __obtain_unique_tags(df, tag_fields):
        """
        获取dataframe中唯一标签组值
        :param df: dataframe
        :param tag_fields:
        :return:
        """
        return set(df[list(tag_fields)].apply(lambda x: tuple(x), axis=1))

    def __obtain_unique_df(self, df, tag_labels, child_tags, sub_prefix, sep='_'):
        """
        根据tags将dataframe进行分割为若干个不同的子表
        :param df: dataframe
        :param tag_labels: 标签字段
        :param sub_prefix: 子表名称前缀
        :param sep: 连接分割符
        :return: 子表名称+子表的字典
        """
        try:
            unique_sub_df = {}
            for sub_name, _df in df.groupby(tag_labels):
                sub_name = self.__inter2str(sub_name)
                tags_key_list = {key: val for key, val in zip(tag_labels, self.__inter2str(sub_name))}
                sub_name = sub_prefix
                for item in child_tags:
                    sub_name += sep + tags_key_list[item]
                unique_sub_df[sub_name] = _df
        except Exception as e:
            logger.error('获取tag标签唯一的子表名称失败：{}'.format(e))
            raise ValueError('获取tag标签唯一的子表名称失败！')
        return unique_sub_df

    @staticmethod
    def __inter2str(tup):
        """
        将元组中为整形元素转为字符串
        :param tup: 元组
        :return: 字符串列表
        """
        return [str(tup_item) for tup_item in tup]


"""----------------------------从dataframe格式数据批量插入到TD库---------------结束------"""

if __name__ == '__main__':
    td = OperateTD()
    # td.save_copy_sql('device_message', 'device_message')
    # sql = open_object_general(data_dir, 'sql')['device_message']
    # td.exec_sql(sql)
    df2 = pd.read_csv(os.path.join(data_dir, "./data.csv"))
    import datetime

    now_time = datetime.datetime.now()
    times = [(now_time + datetime.timedelta(seconds=-i)).strftime('%Y-%m-%d %H:%M:%S') for i in range(10000)]
    df3 = df2.iloc[0:10000].copy()
    df3['ts'] = times
    # td.batch_insert(8, df3, sub_table_name='sub_device_message1', sup_table_name='device_message',
    #                 tag_fields=['imei', 'message_function_code'])
    td.batch_insert_by_tags(max_workers=8, df=df3, sup_table_name='device_message',
                            tag_fields=['imei', 'message_function_code'], child_tags=['imei'],
                            sub_prefix='device_message', sep='_')
    # data = td.query_df("select * from device_message limit 10000")
    # data.to_csv(os.path.join(data_dir, 'data.csv'), index=False)
    # print(data)
