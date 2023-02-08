import psycopg2
from psycopg2 import extras as ex
from common.log_utils import get_logger

from conf.config import PostgresqlConfig

logger = get_logger(__name__)


class PostgreOp(object):
    def __init__(self):
        logger.debug(f"postgresql host: {PostgresqlConfig['host']}")
        self.conn = psycopg2.connect(host=PostgresqlConfig['host'], port=PostgresqlConfig['port'],
                                     user=PostgresqlConfig['user'], password=str(PostgresqlConfig['password']),
                                     database=PostgresqlConfig['db_name'])
        try:
            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error("连接PG数据库出错: {}".format(e))
            raise ValueError("连接PG数据库出错")

    def query(self, sql, get_type='dict'):
        """
        get_type: dict: 返回词典类型数据   list: 返回list类型数据
        """
        try:
            self.cursor.execute(sql)
            if get_type == 'list':
                return [row[0] for row in self.cursor.fetchall() if row[0]]
            else:
                return self._row2dict()
        except Exception as e:
            logger.error("查询PG数据库出错:{}".format(e))
            raise ValueError('查询PG数据库出错')

    def binary_query(self, sql):
        try:
            self.cursor.execute(sql)
            return self._tup2dict()
        except Exception as e:
            logger.error("查询PG数据库出错:{}".format(e))
            raise ValueError('查询PG数据库出错')

    def delete(self, table_name, where):
        delete_sql = "DELETE FROM {} {} ".format(table_name, where)
        logger.info('sql: {}'.format(delete_sql))
        try:
            self.cursor.execute(delete_sql)
            self.conn.commit()
        except Exception as e:
            logger.error("删除数据出错".format(e))
            raise ValueError("删除数据出错")

    def update(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            logger.error("更新数据库出错:{}".format(e))
            raise ValueError('更新数据库出错')

    def update_many(self, table_name, columns, values_list):
        s = ''
        for i in columns:
            s += f'{i} = new_data.{i},'
        sql = f"""update {table_name} set {s[:-1]}  from (values %s) as new_data ({','.join(columns)}) where {table_name}.gid=new_data.gid;"""
        try:
            ex.execute_values(self.cursor, sql, values_list, page_size=10000)
            self.conn.commit()
        except Exception as e:
            logger.error("批量更新数据出错:{}".format(e))
            raise ValueError('批量更新数据出错')

    def execute_many(self, sql, values_list):
        try:
            ex.execute_values(self.cursor, sql, values_list, page_size=10000)
            self.conn.commit()
        except Exception as e:
            logger.error("批量执行数据出错:{}".format(e))
            raise ValueError('批量执行数据出错')

    def delete_(self, delete_sql):
        logger.info('sql: {}'.format(delete_sql))
        try:
            self.cursor.execute(delete_sql)
            self.conn.commit()
        except Exception as e:
            logger.error("删除数据出错".format(e))
            raise ValueError("删除数据出错")

    def insert(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            logger.error("插入或更新数据库出错:{}".format(e))
            raise ValueError('插入或更新数据库出错')

    def update_many(self, project_id, table_name, columns, values_list):
        s = ''
        for i in columns:
            s += f'{i} = new_data.{i},'
        sql = f"""update {table_name} set {s[:-1]}  from (values %s) as new_data ({','.join(columns)}) where projectid={project_id} and {table_name}.gid=new_data.gid;"""
        try:
            ex.execute_values(self.cursor, sql, values_list, page_size=10000)
            self.conn.commit()
        except Exception as e:
            logger.error("批量更新数据出错:{}".format(e))
            raise ValueError('批量更新数据出错')

    def execute_many(self, sql, values_list):
        try:
            ex.execute_values(self.cursor, sql, values_list, page_size=10000)
            self.conn.commit()
        except Exception as e:
            logger.error("批量执行数据出错:{}".format(e))
            raise ValueError('批量执行数据出错')

    def delete_(self, delete_sql):
        logger.info('sql: {}'.format(delete_sql))
        try:
            self.cursor.execute(delete_sql)
            self.conn.commit()
        except Exception as e:
            logger.error("删除数据出错".format(e))
            raise ValueError("删除数据出错")

    def insert(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            logger.error("更新数据库出错:{}".format(e))
            raise ValueError('更新数据库出错')

    def insert_many(self, table_name, fields, values_list):
        fields_str = '({})'.format(', '.join(fields))

        insert_many_template = "INSERT INTO {}{} " \
                               "VALUES %s".format(table_name, fields_str)
        try:
            ex.execute_values(self.cursor, insert_many_template, values_list, page_size=10000)
            self.conn.commit()
        except Exception as e:
            logger.error("批量插入数据出错:{}".format(e))
            raise ValueError('批量插入数据出错')

    def _row2dict(self):
        columns = [col[0] for col in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def _tup2dict(self):
        return dict(self.cursor.fetchall())

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    ps = PostgreOp()
    projectid = 'qd_high_merge'
    node_list = ps.query("""select gid, dno, name, type from dt_node_merge  where projectid = '{}' """.format(projectid))
    pipe_list = ps.query("""select gid, dno, pipematerial, pipelength, pipediam, pressurerating, wallthickness, source, target from dt_pipeline_merge  where projectid = '{}' """.format(projectid))

