import pymysql.cursors
import redis


class BaseMigration(object):
    # 连接redis
    redis_config = {"host": "localhost", "port": 6379, "db": 10}
    redis_conn = redis.Redis(**redis_config)

    # 链接mysql
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='123456',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    def __init__(self, table_name):
        self.table_name = table_name

    # 从info_base中查询数据，
    def _get_data(self):
        # start = time.process_time()
        # 获取cursor对象
        cursor = self.connection.cursor()
        # 初始值
        id = self.redis_conn.get(self.table_name)
        id = id if id else 0
        # 查询新库中的的数据
        sql = "SELECT * FROM " + self.table_name + "  where id>%d limit 2" % int(id)
        cursor.execute(sql)
        return cursor.fetchall()

    # 将处理后的数据存进新的数据库
    def _insert_data(self, sql, lastId):
        try:
            # 获取cursor对象
            cursor = self.connection.cursor()
            cursor.execute(sql)
            # 记录索引
            self.redis_conn.set(self.table_name, lastId)
            self.connection.commit()
        except Exception as e:
            print(e)
            self.connection.rollback()
        finally:
            self.connection.close()

    def export_data(self):
        # 1.获取旧数据
        datas = self._get_data()
        if datas:
            # 2.处理数据
            sql, lastId = self.handle_data(datas)
            # 3.添加到新表中
            self._insert_data(sql, lastId)
        else:
            print('no data to deal')

    def handle_data(self, datas=[]):
        """
        处理数据，该方法必须由子类重写

        :param datas: 旧表中查询出来的所有数据

        :return: 多值 sql,id
        """
        return '', 0
