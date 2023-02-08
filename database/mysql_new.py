# -*- coding: utf-8 -*-
# Author: Lx
# Date: 2020/12/21 15:23

import pymysql

from conf.log import logger
# from
from conf.config import MySQLConfig
# from django.db import connection


class MysqlOp(object):
    def __init__(self):
        # print(MySQLConfig)
        self.conn = pymysql.connect(host=MySQLConfig['host'], port=MySQLConfig['port'],
                                    user=MySQLConfig['user'], password=str(MySQLConfig['password']),
                                    db=MySQLConfig['db_name'])
        # self.conn = connection
        try:
            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error("连接数据库出错: {}".format(e))
            raise ValueError("连接数据库出错")

    def query_by_id(self, sql):
        try:
            self.cursor.execute(sql)
            # self.conn.commit()
            return self.cursor.fetchone()
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错')

    def query(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            # self.cursor.execute('UPDATE signal_origin SET used=1 WHERE sig_index = 4858 ;')
            return self._row2dict()
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错')
            # finally:
            #     self.close()

    def update(self, sql):
        try:
            self.cursor.execute(sql)
            # self.conn.commit()
        except Exception as e:
            logger.error("更新数据库出错:{}".format(e))
            raise ValueError('更新数据库出错')
            # finally:
            #     self.close()

    def update_easy(self, table_name, fields, values, where):
        """
        更新指定表的某个字段, e.g. "UPDATE table_name SET age=22, class='class2' WHERE name='Sam'"
        :param table_name:
        :param fields: 准备修改的字段
        :param values: 修改后的内容
        :param where: 条件，注意引号
        :return:
        """
        values = [f"\'{value}\'" if isinstance(value, str) else value for value in values]
        set_str = ', '.join([f'{field}={value}' for field, value in zip(fields, values)])
        update_sql = """
                UPDATE {} SET {} {};
                """.format(table_name, set_str, where)
        # logger.info("update_sql: {}".format(update_sql))
        cursor = self.conn.cursor()
        try:
            cursor.execute(update_sql)
            self.conn.commit()
        except Exception as e:
            logger.error("更新数据出错:{}".format(e))
            raise ValueError('更新数据出错')

    def update_many(self, table_name, value_id_list):
        """
        :param table_name:
        :param value_id_list:
        :return:
        """
        update_many_sql = """
                        UPDATE {} 
                        SET used = (%s) 
                        WHERE sig_index = (%s)
                     """.format(table_name)
        # logger.info("update_sql: {}".format(update_sql))
        cursor = self.conn.cursor()
        try:
            cursor.executemany(update_many_sql, value_id_list)
            self.conn.commit()
        except Exception as e:
            logger.error("批量更新数据出错:{}".format(e))
            raise ValueError('批量更新数据出错')
        cursor.close()

    def insert(self, table_name, fields, values, multiple=False):
        fields_str = '({})'.format(', '.join(fields))
        if multiple:
            values_str = ', '.join([str(tuple(value)) for value in values])
        else:
            values_str = str(tuple(values))
        insert_sql = "INSERT INTO {}{} " \
                     "VALUES {}".format(table_name, fields_str, values_str)
        # logger.info('sql: {}'.format(insert_sql))
        try:
            self.cursor.execute(insert_sql)
            self.conn.commit()
        except Exception as e:
            logger.error("插入数据出错:{}".format(e))
            raise ValueError('插入数据出错')
            # finally:
            #     self.close()

    def insert_many(self, table_name, fields, values_list):
        fields_str = '({})'.format(', '.join(fields))
        fields_num_str = '({})'.format(', '.join(('%s', )*len(fields)))

        insert_many_template = "INSERT INTO {}{} " \
                               "VALUES {}".format(table_name, fields_str, fields_num_str)
        # logger.info('sql: {}'.format(insert_sql))
        try:
            self.cursor.executemany(insert_many_template, values_list)
            self.conn.commit()
        except Exception as e:
            logger.error("批量插入数据出错:{}".format(e))
            raise ValueError('批量插入数据出错')
            # finally:
            #     self.close()

    def delete(self, table_name, where):
        delete_sql = "DELETE FROM {} {} ".format(table_name, where)
        logger.info('sql: {}'.format(delete_sql))
        try:
            self.cursor.execute(delete_sql)
            self.conn.commit()
        except Exception as e:
            logger.error("删除数据出错".format(e))
            raise ValueError("删除数据出错")

    def clean_table(self, tb_name):
        """清除一个表的内容"""
        logger.debug('Cleaning table: {}.'.format(tb_name))
        clean_sql = """
        truncate {};
        """.format(tb_name)
        try:
            cursor = self.conn.cursor()
            cursor.execute('SET FOREIGN_KEY_CHECKS=0')
            cursor.execute(clean_sql)
            cursor.execute('SET FOREIGN_KEY_CHECKS=1')
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning('Failed to execute sql: {}, msg: {}'.format(clean_sql, e))
            self.conn.rollback()

    def _row2dict(self):
        columns = [col[0] for col in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    mysql_op = MysqlOp()
    # mysql_op.query('UPDATE signal_origin SET `used`=0 WHERE sig_index = 4883')
    # a = mysql_op.query('select * from signal_origin limit 5')
    # print(a)
    # mysql_op.update_easy('signal_origin', ['used'], [0], 'where sig_index = "4883"')
    mysql_op.insert('signal_event_tag', ['sig_index', 'event_id', 'event_name'], [123, 'id1', 'name1'])
    mysql_op.close()
    # mysql_op.query('SELECT * FROM signal_origin;')
