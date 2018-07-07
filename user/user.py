from BaseMigration import BaseMigration


class UserMigration(BaseMigration):

    def handle_data(self, datas):
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
        if result:
            values = ','.join(str(i) for i in result)
            sql = 'insert into dropwizard.users(id,userName,password,age,address) values' + values
            return sql, result[-1][0]
        return None


if __name__ == '__main__':
    user = UserMigration('klicen_app.base_users')
    user.export_data()
