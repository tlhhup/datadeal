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

    # 从info_base中查询数据，
    def get_data(self, table_name):
        # start = time.process_time()
        # 获取cursor对象
        cursor = self.connection.cursor()
        # 初始值
        id = self.redis_conn.get(table_name)
        id = id if id else 0
        # 查询新库中的的数据
        sql = "SELECT * FROM " + table_name + "  where id>%d limit 2" % int(id)
        cursor.execute(sql)
        return cursor.fetchall()

    # 将处理后的数据存进新的数据库
    def insert_data(self, sql, old_table_name,lastId):
        try:
            # 获取cursor对象
            cursor = self.connection.cursor()
            cursor.execute(sql)
            # 记录索引
            self.redis_conn.set(old_table_name, lastId)
            self.connection.commit()
        except Exception as e:
            print(e)
            self.connection.rollback()
        finally:
            self.connection.close()

