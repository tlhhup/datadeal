from BaseMigration import BaseMigration


class UserMigration(BaseMigration):
    def __init__(self, table_name):
        self.table_name = table_name

    def _handle_data(self):
        datas = self.get_data(self.table_name)
        result = []
        item = {
            'id': None,
            'userName': None,
            'password': None,
            'age': None,
            'address': None
        }
        for data in datas:
            item['id'] = data['id']
            item['userName'] = data['username']
            item['password'] = data['password']
            item['age'] = data['age'] if data.get('age') else 0
            item['address'] = data['address'] if data.get('address') else ''
            result.append(tuple(item.values()))
        return result

    def execute_sql(self):
        datas = self._handle_data()
        if datas:
            values = ','.join(str(i) for i in datas)
            sql = 'insert into dropwizard.users(id,userName,password,age,address) values'+values
            self.insert_data(sql, self.table_name, datas[-1][0])


if __name__ == '__main__':
    user = UserMigration('klicen_app.base_users')
    user.execute_sql()
